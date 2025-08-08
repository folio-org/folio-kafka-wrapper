package org.folio.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static java.lang.String.format;
import static org.folio.kafka.KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.okapi.common.XOkapiHeaders.REQUEST_ID;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(VertxUnitRunner.class)
public class KafkaConsumerWrapperTest {

  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
  private static final String MODULE_NAME = "test_module";

  @Rule
  public TestName testName = new TestName();

  @Rule
  public KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0")
      .withStartupAttempts(3);

  private Vertx vertx = Vertx.vertx();
  private KafkaConfig kafkaConfig;
  private KafkaAdminClient kafkaAdminClient;
  private KafkaProducer<String,String> producer;

  @Before
  public void setUp() {
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafka.getHost())
      .kafkaPort(kafka.getFirstMappedPort() + "")
      .build();

    kafkaAdminClient = KafkaAdminClient.create(vertx, Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaUrl()));

    Map<String,String> config = Map.of(
        "bootstrap.servers", kafka.getHost() + ":" + kafka.getFirstMappedPort(),
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "acks", "1");
    producer = KafkaProducer.create(vertx, config);
  }

  @After
  public void tearDown(TestContext testContext) {
    kafkaAdminClient.close()
    .onComplete(x -> producer.close())
    .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void consumerInResumedModeAfterFetch(TestContext testContext) {
    int loadLimit = 5;
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor.GlobalLoadSensorNA())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    kafkaConsumerWrapper.start(record -> Future.succeededFuture(record.key()), MODULE_NAME);
    testContext.assertFalse(kafkaConsumerWrapper.isConsumerPaused());
    kafkaConsumerWrapper.pause();
    testContext.assertTrue(kafkaConsumerWrapper.isConsumerPaused());
    kafkaConsumerWrapper.fetch(2);
    testContext.assertFalse(kafkaConsumerWrapper.isConsumerPaused());

  }

  @Test
  public void shouldResumeConsumerAndPollRecordAfterConsumerWasPaused(TestContext testContext) {
    resumeConsumerAndPollRecordAfterConsumerWasPaused(testContext, new GlobalLoadSensor(), null)
    .onComplete(testContext.asyncAssertSuccess(pauseCount -> {
      testContext.assertEquals(1, pauseCount);
    }));
  }

  /**
   * When the backpressure gauge takes the global load into account, the consumer should resume
   * after the global load is minimized.
   */
  @Test
  public void shouldResumeConsumerAndPollRecordAfterConsumerWasPausedGlobalSensor(TestContext testContext) {
    int globalLoadLimit = 7;
    GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor(globalLoadLimit);
    BackPressureGauge<Integer, Integer, Integer> backPressureGauge = (g, l, t) -> (l > 0 && l > t) || (g > globalLoadLimit);
    resumeConsumerAndPollRecordAfterConsumerWasPaused(testContext, globalLoadSensor, backPressureGauge)
      .onComplete(ar -> {
        vertx.setTimer(6000, (l) -> {
          for (int i = 0; i < globalLoadLimit; i++) {
            globalLoadSensor.decrement();
          }
        });
      });
  }

  private Future<Integer> resumeConsumerAndPollRecordAfterConsumerWasPaused(TestContext testContext,
                                                                         GlobalLoadSensor globalLoadSensor,
                                                                         BackPressureGauge<Integer, Integer, Integer> backPressureGauge) {
    Promise<Integer> promise = Promise.promise();
    int loadLimit = 5;
    int recordsAmountToSend = 7;
    AtomicInteger pauseCount = new AtomicInteger();
    System.setProperty(KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, "2");

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper.KafkaConsumerWrapperBuilder<String, String> kafkaConsumerWrapperBuilder = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(globalLoadSensor)
      .subscriptionDefinition(subscriptionDefinition);
    if(backPressureGauge != null) kafkaConsumerWrapperBuilder.backPressureGauge(backPressureGauge);
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = kafkaConsumerWrapperBuilder.build();

    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, eventType());
    AtomicInteger recordCounter = new AtomicInteger(0);

    Future<Void> future = Future.succeededFuture();
    for (int i = 1; i <= recordsAmountToSend; i++) {
      // use same key to keep order
      var sendRecord = sendRecord("key", format("test_payload-%s", i), topicName);
      future = future.compose(x -> sendRecord);
    }
    // create back pressure by waiting until all records have been sent before starting the consumer

    future.compose(y -> kafkaConsumerWrapper.start(r -> {
          var i = recordCounter.incrementAndGet();
          testContext.assertEquals(format("test_payload-%s", i), r.value());
          if (kafkaConsumerWrapper.isConsumerPaused()) {
            pauseCount.incrementAndGet();
          }
          if (i == loadLimit + 2) {
            promise.complete(pauseCount.get());
          }
          return Future.future(timer -> vertx.setTimer(20, x -> timer.complete(r.key())));
        }, MODULE_NAME));
     return promise.future()
         .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedBusinessHandlerIsNull(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(null, MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSubscriptionDefinitionIsNull(TestContext testContext) {
    Async async = testContext.async();
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .subscriptionDefinition(null)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(record -> Future.succeededFuture(), MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedLoadLimitLessThenOne(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(0)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(record -> Future.succeededFuture(), MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnSucceededFutureAndUnsubscribeWhenStopIsCalled(TestContext testContext) throws Exception {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
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
    .compose(v -> kafkaConsumerWrapper.start(record -> Future.succeededFuture(), MODULE_NAME))
    .compose(v -> awaitMembersSize(groupId, 1))
    .compose(v -> kafkaConsumerWrapper.stop())
    .compose(v -> awaitMembersSize(groupId, 0))
    .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void shouldInvokeSpecifiedProcessRecordErrorHandlerWhenAsyncRecordHandlerFails(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, eventType());
    ProcessRecordErrorHandler<String, String> recordErrorHandler = mock(ProcessRecordErrorHandler.class);

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .processRecordErrorHandler(recordErrorHandler)
      .build();

    kafkaConsumerWrapper
      .start(r -> {
        return Future.failedFuture("test error msg");
      }, MODULE_NAME)
      .eventually(() -> sendRecord("1", "test_payload", topicName))
      .onComplete(x -> async.complete());

    async.await();
    verify(recordErrorHandler, after(500)).handle(any(Throwable.class), any(KafkaConsumerRecord.class));
  }

  @Test
  public void shouldThrowExceptionOnStartCallIfGroupInstanceIdIsBlankString(TestContext testContext) {
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
      .onComplete(testContext.asyncAssertFailure());

    consumerWrapperBuilder.groupInstanceId(blankStringGroupInstanceId)
      .build()
      .start(kafkaRecord -> Future.succeededFuture(kafkaRecord.key()), MODULE_NAME)
      .onComplete(testContext.asyncAssertFailure());
  }

  /**
   * To make tests independent from each other use the test method name as eventType
   */
  private String eventType() {
    return testName.getMethodName();
  }

  private Future<Void> sendRecord(String key, String recordPayload, String topicName) {
    KafkaProducerRecord<String,String> kafkaRecord =
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
        });
  }
}
