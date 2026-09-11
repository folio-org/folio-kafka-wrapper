package org.folio.kafka.filtering;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

/**
 * End-to-end verification of entitlement-based filtering wired through
 * {@link KafkaConsumerWrapper}: a real Kafka broker, a fake sidecar-like HTTP endpoint answering
 * {@code /entitlements/modules/{id}}, and the real {@code entitlement} topic consumer.
 */
@ExtendWith(VertxExtension.class)
class TenantEntitlementFilteringIntegrationTest {

  private final Vertx vertx = Vertx.vertx();
  private final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:4.2.0").withStartupAttempts(3);

  private String moduleId;
  private String env;
  private KafkaConfig kafkaConfig;
  private KafkaProducer<String, String> producer;
  private HttpServer entitlementsServer;
  private volatile Set<String> entitledTenants;
  private final AtomicInteger entitlementsFailuresRemaining = new AtomicInteger(0);
  private volatile long entitlementsResponseDelayMs = 0;

  @BeforeEach
  void setUp() throws Exception {
    kafka.start();

    moduleId = "mod-filter-test-" + UUID.randomUUID();
    env = "it" + UUID.randomUUID().toString().substring(0, 8);
    entitledTenants = Set.of("diku");

    entitlementsServer = startEntitlementsServer();
    setFilterSystemProperties();

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafka.getHost())
      .kafkaPort(kafka.getFirstMappedPort() + "")
      .okapiUrl("http://localhost:" + entitlementsServer.actualPort())
      .build();
    producer = KafkaProducer.create(vertx, producerConfig());
  }

  private HttpServer startEntitlementsServer() throws Exception {
    return vertx.createHttpServer()
      .requestHandler(request -> {
        if (entitlementsFailuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
          // Mirrors folio-module-sidecar's 503 while its own boot-time entitlement load is in flight.
          request.response().setStatusCode(503).end();
          return;
        }
        vertx.setTimer(Math.max(entitlementsResponseDelayMs, 1),
          id -> request.response().end(new JsonArray(List.copyOf(entitledTenants)).encode()));
      })
      .listen(0)
      .toCompletionStage()
      .toCompletableFuture()
      .get(10, SECONDS);
  }

  private void setFilterSystemProperties() {
    System.setProperty("ENV", env);
    System.setProperty(TenantEntitlementFilterProperties.ENABLED, "true");
    System.setProperty(TenantEntitlementFilterProperties.ENTITLEMENT_REFRESH_INTERVAL_MS, "3600000");
    System.setProperty(TenantEntitlementFilterProperties.TENANT_DISABLED_STRATEGY, "SKIP");
    System.setProperty(TenantEntitlementFilterProperties.ALL_TENANTS_DISABLED_STRATEGY, "FAIL");
  }

  private Map<String, String> producerConfig() {
    return Map.of(
      "bootstrap.servers", kafka.getHost() + ":" + kafka.getFirstMappedPort(),
      "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
      "acks", "1");
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    System.clearProperty("ENV");
    System.clearProperty(TenantEntitlementFilterProperties.ENABLED);
    System.clearProperty(TenantEntitlementFilterProperties.ENTITLEMENT_REFRESH_INTERVAL_MS);
    System.clearProperty(TenantEntitlementFilterProperties.TENANT_DISABLED_STRATEGY);
    System.clearProperty(TenantEntitlementFilterProperties.ALL_TENANTS_DISABLED_STRATEGY);

    producer.close()
      .compose(v -> entitlementsServer.close())
      .onComplete(x -> {
        kafka.stop();
        testContext.completeNow();
      });
  }

  @Test
  void shouldSkipNonEntitledTenantAndHonorLiveEntitlementUpdates(VertxTestContext testContext) throws Exception {
    String eventType = "shouldSkipNonEntitledTenant";
    String topicName = KafkaTopicNameHelper.formatTopicName(env, getDefaultNameSpace(), "diku", eventType);
    List<String> handledTenants = startConsumerCollectingHandledKeys(eventType);

    assertEntitledTenantIsHandled(topicName, handledTenants);
    assertNonEntitledTenantIsSkipped(topicName, handledTenants);
    assertLiveEntitlementEventUnblocksTenant(topicName, handledTenants);

    testContext.completeNow();
  }

  @Test
  void shouldWaitForRealAnswer_whenFirstRecordRacesTheEntitlementLoad(VertxTestContext testContext) throws Exception {
    entitledTenants = Set.of("diku");
    entitlementsResponseDelayMs = 1000;
    String eventType = "shouldWaitForRealAnswer";
    String topicName = KafkaTopicNameHelper.formatTopicName(env, getDefaultNameSpace(), "college", eventType);
    List<String> handledTenants = startConsumerCollectingHandledKeys(eventType);

    // Sent immediately, so this races the deliberately slow initial /entitlements/modules/{id}
    // lookup. The record should wait for the real (non-entitled) answer instead of being accepted
    // unfiltered just because the cache wasn't populated yet.
    sendRecord(topicName, "college", "college").toCompletionStage().toCompletableFuture().get(10, SECONDS);
    Thread.sleep(2000);
    assertFalse(handledTenants.contains("college"),
      "record racing the entitlement load should wait for and honor the real, non-entitled answer");

    testContext.completeNow();
  }

  private List<String> startConsumerCollectingHandledKeys(String eventType) throws Exception {
    List<String> handledKeys = new CopyOnWriteArrayList<>();
    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
      .subscriptionDefinition(KafkaTopicNameHelper.createSubscriptionDefinition(env, getDefaultNameSpace(), eventType))
      .build();

    consumerWrapper.start(record -> {
      handledKeys.add(record.key());
      return Future.succeededFuture(record.key());
    }, moduleId).toCompletionStage().toCompletableFuture().get(10, SECONDS);

    return handledKeys;
  }

  private void assertEntitledTenantIsHandled(String topicName, List<String> handledTenants) throws Exception {
    // "diku" is entitled from the start, so this also confirms the pipeline (consumer + filter) is up.
    sendRecord(topicName, "diku", "diku").toCompletionStage().toCompletableFuture().get(10, SECONDS);
    awaitCondition(() -> handledTenants.contains("diku"), 10000);
    assertTrue(handledTenants.contains("diku"), "message for entitled tenant 'diku' should be handled");
  }

  private void assertNonEntitledTenantIsSkipped(String topicName, List<String> handledTenants) throws Exception {
    // By now the initial /entitlements/modules/{id} lookup has resolved, so a non-entitled tenant is filtered.
    sendRecord(topicName, "college", "college").toCompletionStage().toCompletableFuture().get(10, SECONDS);
    Thread.sleep(2000);
    assertFalse(handledTenants.contains("college"), "message for non-entitled tenant 'college' should be skipped");
  }

  private void assertLiveEntitlementEventUnblocksTenant(String topicName, List<String> handledTenants)
    throws Exception {
    publishEntitlementEvent("college", EntitlementEvent.Type.ENTITLE)
      .toCompletionStage().toCompletableFuture().get(10, SECONDS);
    awaitCondition(() -> {
      sendRecord(topicName, "college", "college");
      return handledTenants.contains("college");
    }, 15000);
    assertTrue(handledTenants.contains("college"),
      "message for tenant entitled via a live entitlement event should now be handled");
  }

  @Test
  void shouldRouteToErrorHandler_whenAllTenantsDisabledStrategyIsFail(VertxTestContext testContext) throws Exception {
    entitledTenants = Set.of();
    String eventType = "shouldFailWhenNoTenantsEntitled";
    String topicName = KafkaTopicNameHelper.formatTopicName(env, getDefaultNameSpace(), "diku", eventType);

    @SuppressWarnings("unchecked")
    ProcessRecordErrorHandler<String, String> errorHandler = mock(ProcessRecordErrorHandler.class);

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
      .subscriptionDefinition(KafkaTopicNameHelper.createSubscriptionDefinition(env, getDefaultNameSpace(), eventType))
      .processRecordErrorHandler(errorHandler)
      .build();

    consumerWrapper.start(record -> Future.succeededFuture(record.key()), moduleId)
      .toCompletionStage().toCompletableFuture().get(10, SECONDS);

    // Resend until the initial /entitlements/modules/{id} lookup (entitledTenants = {}) has landed;
    // before that, the filter fails open and the record is handled normally instead of erroring.
    for (int i = 0; i < 20; i++) {
      sendRecord(topicName, "diku-" + i, "diku").toCompletionStage().toCompletableFuture().get(10, SECONDS);
      Thread.sleep(300);
    }

    verify(errorHandler, org.mockito.Mockito.timeout(5000).atLeastOnce())
      .handle(any(TenantsAreDisabledException.class), any(KafkaConsumerRecord.class));

    testContext.completeNow();
  }

  @Test
  void shouldRetryInitialLoad_whenSidecarRespondsNotYetLoaded(VertxTestContext testContext) throws Exception {
    entitledTenants = Set.of();
    entitlementsFailuresRemaining.set(1);

    String eventType = "shouldRetryInitialLoad";
    String topicName = KafkaTopicNameHelper.formatTopicName(env, getDefaultNameSpace(), "diku", eventType);

    @SuppressWarnings("unchecked")
    ProcessRecordErrorHandler<String, String> errorHandler = mock(ProcessRecordErrorHandler.class);

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
      .subscriptionDefinition(KafkaTopicNameHelper.createSubscriptionDefinition(env, getDefaultNameSpace(), eventType))
      .processRecordErrorHandler(errorHandler)
      .build();

    consumerWrapper.start(record -> Future.succeededFuture(record.key()), moduleId)
      .toCompletionStage().toCompletableFuture().get(10, SECONDS);

    resendUntilEntitlementsLoad(topicName);

    verify(errorHandler, org.mockito.Mockito.timeout(6000).atLeastOnce())
      .handle(any(TenantsAreDisabledException.class), any(KafkaConsumerRecord.class));

    testContext.completeNow();
  }

  /**
   * The periodic reconciliation is configured for 1 hour (see {@link #setFilterSystemProperties()}), so
   * any success within this short resend window must come from the initial-load retry, not that timer.
   */
  private void resendUntilEntitlementsLoad(String topicName) throws Exception {
    for (int i = 0; i < 15; i++) {
      sendRecord(topicName, "diku-" + i, "diku").toCompletionStage().toCompletableFuture().get(10, SECONDS);
      Thread.sleep(300);
    }
  }

  private Future<Void> sendRecord(String topicName, String key, String tenant) {
    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, "payload");
    record.addHeader(TENANT, tenant);
    return producer.send(record).mapEmpty();
  }

  private Future<Void> publishEntitlementEvent(String tenant, EntitlementEvent.Type type) {
    String json = "{\"moduleId\":\"%s\",\"tenantName\":\"%s\",\"type\":\"%s\"}".formatted(moduleId, tenant, type);
    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(env + ".entitlement", tenant, json);
    return producer.send(record).mapEmpty();
  }

  private void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(200);
    }
  }
}
