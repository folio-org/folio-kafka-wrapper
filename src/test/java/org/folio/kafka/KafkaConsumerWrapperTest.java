package org.folio.kafka;

import static java.lang.String.format;
import static org.folio.kafka.KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.okapi.common.XOkapiHeaders.REQUEST_ID;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

@ExtendWith(VertxExtension.class)
class KafkaConsumerWrapperTest {

  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
  private static final String MODULE_NAME = "test_module";
  KafkaContainer kafka = new KafkaContainer("apache/kafka-native:4.2.0")
    .withStartupAttempts(3);
  private final Vertx vertx = Vertx.vertx();
  private String testMethodName;
  private KafkaConfig kafkaConfig;
  private KafkaAdminClient kafkaAdminClient;
  private KafkaProducer<String, String> producer;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("test");

    kafka.start();

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafka.getHost())
      .kafkaPort(kafka.getFirstMappedPort() + "")
      .build();

    kafkaAdminClient =
      KafkaAdminClient.create(vertx, Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaUrl()));

    Map<String, String> config = Map.of(
      "bootstrap.servers", kafka.getHost() + ":" + kafka.getFirstMappedPort(),
      "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
      "acks", "1");
    producer = KafkaProducer.create(vertx, config);
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    kafkaAdminClient.close()
      .onComplete(x -> producer.close())
      .onComplete(testContext.succeeding(v -> {
        kafka.stop();
        testContext.completeNow();
      }));
  }

  @Test
  void consumerInResumedModeAfterFetch() {
    int loadLimit = 5;
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    kafkaConsumerWrapper.start(event -> Future.succeededFuture(event.key()), MODULE_NAME);
    assertFalse(kafkaConsumerWrapper.isConsumerPaused());
    kafkaConsumerWrapper.pause();
    assertTrue(kafkaConsumerWrapper.isConsumerPaused());
    kafkaConsumerWrapper.fetch(2);
    assertFalse(kafkaConsumerWrapper.isConsumerPaused());
  }

  @Test
  void shouldResumeConsumerAndPollRecordAfterConsumerWasPaused(VertxTestContext testContext) {
    resumeConsumerAndPollRecordAfterConsumerWasPaused(testContext, new GlobalLoadSensor(), null)
      .onComplete(testContext.succeeding(pauseCount -> testContext.verify(() -> {
        assertEquals(1, pauseCount);
        testContext.completeNow();
      })));
  }

  /**
   * When the backpressure gauge takes the global load into account, the consumer should resume
   * after the global load is minimized.
   */
  @Test
  void shouldResumeConsumerAndPollRecordAfterConsumerWasPausedGlobalSensor(VertxTestContext testContext) {
    int globalLoadLimit = 7;
    GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor(globalLoadLimit);
    BackPressureGauge<Integer, Integer, Integer> backPressureGauge =
      (g, l, t) -> l > 0 && l > t || g > globalLoadLimit;
    resumeConsumerAndPollRecordAfterConsumerWasPaused(testContext, globalLoadSensor, backPressureGauge)
      .onComplete(testContext.succeeding(pauseCount -> {
        vertx.setTimer(6000, l -> {
          for (int i = 0; i < globalLoadLimit; i++) {
            globalLoadSensor.decrement();
          }
        });
        testContext.completeNow();
      }));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedBusinessHandlerIsNull(VertxTestContext testContext) {
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(null, MODULE_NAME);

    future.onComplete(testContext.failingThenComplete());
  }

  @Test
  void shouldReturnFailedFutureWhenSubscriptionDefinitionIsNull(VertxTestContext testContext) {
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .subscriptionDefinition(null)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(event -> Future.succeededFuture(event.key()), MODULE_NAME);

    future.onComplete(testContext.failingThenComplete());
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedLoadLimitLessThenOne(VertxTestContext testContext) {
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(0)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(event -> Future.succeededFuture(event.key()), MODULE_NAME);

    future.onComplete(testContext.failingThenComplete());
  }

  @Test
  void shouldReturnSucceededFutureAndUnsubscribeWhenStopIsCalled(VertxTestContext testContext) {
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    String groupId = KafkaTopicNameHelper.formatGroupName(eventType(), MODULE_NAME);

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    awaitMembersSize(groupId, 0)
      .compose(v -> kafkaConsumerWrapper.start(event -> Future.succeededFuture(event.key()), MODULE_NAME))
      .compose(v -> awaitMembersSize(groupId, 1))
      .compose(v -> kafkaConsumerWrapper.stop())
      .compose(v -> awaitMembersSize(groupId, 0))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldInvokeSpecifiedProcessRecordErrorHandlerWhenAsyncRecordHandlerFails() throws InterruptedException {
    // Use a standalone VertxTestContext (not extension-injected) since we block on it
    // mid-test: an injected context also tracks an internal "invocation checkpoint"
    // that is only flagged once this method returns, which would deadlock with awaitCompletion().
    VertxTestContext testContext = new VertxTestContext();
    Checkpoint errorHandlerCheckpoint = testContext.checkpoint();
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, eventType());
    ProcessRecordErrorHandler<String, String> recordErrorHandler = mock(ProcessRecordErrorHandler.class);
    doAnswer(invocation -> {
      errorHandlerCheckpoint.flag();
      return null;
    }).when(recordErrorHandler).handle(any(Throwable.class), any(KafkaConsumerRecord.class));

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .processRecordErrorHandler(recordErrorHandler)
      .build();

    kafkaConsumerWrapper.start(r -> Future.failedFuture("test error msg"), MODULE_NAME)
      .eventually(() -> sendRecord("1", "test_payload", topicName));

    assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
    verify(recordErrorHandler).handle(any(Throwable.class), any(KafkaConsumerRecord.class));
  }

  @Test
  void shouldThrowExceptionOnStartCallIfGroupInstanceIdIsBlankString(VertxTestContext testContext) {
    Checkpoint checkpoint = testContext.checkpoint(2);
    int loadLimit = 5;
    String emptyStringGroupInstanceId = "";
    String blankStringGroupInstanceId = " ";
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper.KafkaConsumerWrapperBuilder<String, String> consumerWrapperBuilder =
      KafkaConsumerWrapper.<String, String>builder()
        .context(vertx.getOrCreateContext())
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
        .subscriptionDefinition(subscriptionDefinition);

    consumerWrapperBuilder.groupInstanceId(emptyStringGroupInstanceId)
      .build()
      .start(kafkaRecord -> Future.succeededFuture(kafkaRecord.key()), MODULE_NAME)
      .onComplete(testContext.failing(t -> checkpoint.flag()));

    consumerWrapperBuilder.groupInstanceId(blankStringGroupInstanceId)
      .build()
      .start(kafkaRecord -> Future.succeededFuture(kafkaRecord.key()), MODULE_NAME)
      .onComplete(testContext.failing(t -> checkpoint.flag()));
  }

  private Future<Integer> resumeConsumerAndPollRecordAfterConsumerWasPaused(
    VertxTestContext testContext,
    GlobalLoadSensor globalLoadSensor,
    BackPressureGauge<Integer, Integer, Integer> backPressureGauge) {
    int loadLimit = 5;
    int recordsAmountToSend = 7;
    System.setProperty(KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, "2");

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper =
      buildPausedTestConsumer(globalLoadSensor, backPressureGauge, loadLimit);

    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, eventType());
    // create back pressure by waiting until all records have been sent before starting the consumer
    Future<Void> future = sendTestRecords(topicName, recordsAmountToSend);

    AtomicInteger recordCounter = new AtomicInteger(0);
    Promise<Integer> promise = Promise.promise();
    AtomicInteger pauseCount = new AtomicInteger();
    future.compose(y -> kafkaConsumerWrapper.start(r -> {
      var i = recordCounter.incrementAndGet();
      testContext.verify(() -> assertEquals(format("test_payload-%s", i), r.value()));
      if (kafkaConsumerWrapper.isConsumerPaused()) {
        pauseCount.incrementAndGet();
      }
      if (i == loadLimit + 2) {
        promise.complete(pauseCount.get());
      }
      return Future.future(timer -> vertx.setTimer(20, x -> timer.complete(r.key())));
    }, MODULE_NAME));
    return promise.future();
  }

  private KafkaConsumerWrapper<String, String> buildPausedTestConsumer(
    GlobalLoadSensor globalLoadSensor,
    BackPressureGauge<Integer, Integer, Integer> backPressureGauge,
    int loadLimit) {
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper.KafkaConsumerWrapperBuilder<String, String> kafkaConsumerWrapperBuilder =
      KafkaConsumerWrapper.<String, String>builder()
        .context(vertx.getOrCreateContext())
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(globalLoadSensor)
        .subscriptionDefinition(subscriptionDefinition);
    if (backPressureGauge != null) {
      kafkaConsumerWrapperBuilder.backPressureGauge(backPressureGauge);
    }
    return kafkaConsumerWrapperBuilder.build();
  }

  private Future<Void> sendTestRecords(String topicName, int recordsAmountToSend) {
    Future<Void> future = Future.succeededFuture();
    for (int i = 1; i <= recordsAmountToSend; i++) {
      // use same key to keep order
      var sendRecord = sendRecord("key", format("test_payload-%s", i), topicName);
      future = future.compose(x -> sendRecord);
    }
    return future;
  }

  /**
   * To make tests independent from each other use the test method name as eventType.
   */
  private String eventType() {
    return testMethodName;
  }

  private Future<Void> sendRecord(String key, String recordPayload, String topicName) {
    KafkaProducerRecord<String, String> kafkaRecord =
      KafkaProducerRecord.create(topicName, String.valueOf(key), recordPayload);
    kafkaRecord.addHeader(TENANT, TENANT_ID);
    kafkaRecord.addHeader(REQUEST_ID, "request-id");
    kafkaRecord.addHeader(USER_ID, "user-id");

    return producer.send(kafkaRecord).mapEmpty();
  }

  private Future<Void> awaitMembersSize(String groupId, int expectedSize) {
    return kafkaAdminClient.describeConsumerGroups(List.of(groupId))
      .compose(groups -> {
        if (groups.get(groupId).getMembers().size() == expectedSize) {
          return Future.succeededFuture();
        }
        return awaitMembersSize(groupId, expectedSize);
      })
      .recover(t -> {
        if (isGroupNotFoundError(t)) {
          return expectedSize == 0 ? Future.succeededFuture() : awaitMembersSize(groupId, expectedSize);
        }
        return Future.failedFuture(t);
      });
  }

  private boolean isGroupNotFoundError(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof GroupIdNotFoundException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
