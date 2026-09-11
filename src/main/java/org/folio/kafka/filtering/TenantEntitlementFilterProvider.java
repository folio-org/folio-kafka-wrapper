package org.folio.kafka.filtering;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.services.KafkaEnvironmentProperties;

/**
 * Lazily creates and shares one {@link TenantEntitlementFilter} per module id within this JVM, so
 * every {@code KafkaConsumerWrapper} for a module reuses the same entitlement cache and
 * {@code entitlement}-topic consumer.
 *
 * <p>Returns {@code null} when filtering is disabled (the default) via
 * {@link TenantEntitlementFilterProperties}.
 */
public final class TenantEntitlementFilterProvider {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final Map<String, TenantEntitlementFilter> FILTERS = new ConcurrentHashMap<>();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Backoff for {@link #loadInitialEntitlements}, which retries indefinitely on failure - unlike
   * (and much longer than) each record's own short, bounded wait ({@code CACHE_WAIT_TIMEOUT_MS}).
   */
  private static final long BACKGROUND_RETRY_BASE_DELAY_MS = 1000;
  private static final long BACKGROUND_RETRY_MAX_DELAY_MS = 15000;

  private TenantEntitlementFilterProvider() {
  }

  /**
   * Returns the shared entitlement filter for the given module, creating it on first call.
   *
   * @param vertx the Vertx instance
   * @param kafkaConfig Kafka client configuration, also used for {@code okapiUrl}
   * @param moduleId current module id, for example {@code mod-foo-1.0.0}
   * @return the shared filter, or {@code null} if filtering is disabled or {@code moduleId} is blank
   */
  public static TenantEntitlementFilter getOrCreate(Vertx vertx, KafkaConfig kafkaConfig, String moduleId) {
    if (!TenantEntitlementFilterProperties.enabled()) {
      return null;
    }

    if (StringUtils.isBlank(moduleId)) {
      LOGGER.warn("getOrCreate:: Tenant entitlement filtering is enabled but moduleId is blank; "
        + "skipping filter setup");
      return null;
    }

    return FILTERS.computeIfAbsent(moduleId, id -> createFilter(vertx, kafkaConfig, id));
  }

  private static TenantEntitlementFilter createFilter(Vertx vertx, KafkaConfig kafkaConfig, String moduleId) {
    LOGGER.info("createFilter:: Setting up tenant entitlement filter: moduleId = {}", moduleId);

    var client = new WebClientTenantEntitlementClient(vertx, kafkaConfig.getOkapiUrl());
    var service = new TenantEntitlementService(moduleId, client);

    startEntitlementEventConsumer(vertx, kafkaConfig, moduleId, service);
    vertx.setPeriodic(TenantEntitlementFilterProperties.entitlementRefreshIntervalMs(),
      timerId -> service.refresh());

    return new TenantEntitlementFilter(moduleId, service,
      TenantEntitlementFilterProperties.tenantDisabledStrategy(),
      TenantEntitlementFilterProperties.allTenantsDisabledStrategy(),
      vertx, () -> loadInitialEntitlements(vertx, service, 1));
  }

  private static void loadInitialEntitlements(Vertx vertx, TenantEntitlementService service, int attempt) {
    service.refresh().onFailure(cause -> {
      long delayMs = nextRetryDelayMs(attempt);
      LOGGER.info("loadInitialEntitlements:: Initial entitlement load failed, retrying in {} ms: "
        + "moduleId = {}, attempt = {}", delayMs, service.getModuleId(), attempt);
      vertx.setTimer(delayMs, timerId -> loadInitialEntitlements(vertx, service, attempt + 1));
    });
  }

  /**
   * Doubles the delay on each attempt (1s, 2s, 4s, 8s, ...), capped at {@code BACKGROUND_RETRY_MAX_DELAY_MS}.
   */
  private static long nextRetryDelayMs(int attempt) {
    long delayMs = BACKGROUND_RETRY_BASE_DELAY_MS;
    for (int i = 1; i < attempt && delayMs < BACKGROUND_RETRY_MAX_DELAY_MS; i++) {
      delayMs *= 2;
    }
    return Math.min(delayMs, BACKGROUND_RETRY_MAX_DELAY_MS);
  }

  private static void startEntitlementEventConsumer(Vertx vertx, KafkaConfig kafkaConfig, String moduleId,
    TenantEntitlementService service) {
    var consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-tenant-filter-entitlement-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProps);
    consumer.handler(record -> handleEntitlementRecord(record.value(), moduleId, service));
    consumer.exceptionHandler(throwable ->
      LOGGER.error("startEntitlementEventConsumer:: Error consuming entitlement topic: moduleId = {}",
        moduleId, throwable));

    var topic = KafkaEnvironmentProperties.environment() + ".entitlement";
    consumer.subscribe(topic)
      .onSuccess(ar -> LOGGER.info("startEntitlementEventConsumer:: Subscribed to {}: moduleId = {}",
        topic, moduleId))
      .onFailure(throwable -> LOGGER.error(
        "startEntitlementEventConsumer:: Failed to subscribe to {}: moduleId = {}", topic, moduleId, throwable));
  }

  private static void handleEntitlementRecord(String value, String moduleId, TenantEntitlementService service) {
    try {
      var event = OBJECT_MAPPER.readValue(value, EntitlementEvent.class);
      service.applyEntitlementEvent(event);
    } catch (Exception e) {
      LOGGER.warn("handleEntitlementRecord:: Failed to parse entitlement event: moduleId = {}, value = {}",
        moduleId, value, e);
    }
  }
}
