package org.folio.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.filtering.TenantEntitlementFilter;
import org.folio.kafka.filtering.TenantEntitlementFilterProvider;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.okapi.common.logging.FolioLocal;
import org.folio.okapi.common.logging.FolioLoggingContext;

@SuppressWarnings("checkstyle:FinalClass")
public class KafkaConsumerWrapper<K, V> implements Handler<KafkaConsumerRecord<K, V>> {

  public static final GlobalLoadSensor GLOBAL_SENSOR_NA = new GlobalLoadSensor.GlobalLoadSensorNA();
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INVALID_GROUP_INSTANCE_ID_MSG =
    "groupInstanceId must be non-empty String value. Current value is '%s'";
  private static final AtomicInteger INDEXER = new AtomicInteger();
  private static final long PERIODIC_CHECK_INTERVAL = 3000;

  @Getter
  private final int id = INDEXER.getAndIncrement();

  private final AtomicInteger localLoadSensor = new AtomicInteger();

  private final AtomicBoolean isPaused = new AtomicBoolean(false);

  private final Vertx vertx;

  private final Context context;

  private final KafkaConfig kafkaConfig;

  private final SubscriptionDefinition subscriptionDefinition;

  /**
   * Common "id: X subscriptionPattern: Y" descriptor shared by log messages, computed once since id and
   * subscriptionDefinition never change, so it can be logged without a per-call method invocation.
   */
  private final String consumerDescriptor;

  private final GlobalLoadSensor globalLoadSensor;

  private final boolean shouldAddToGlobalLoad;

  private final ProcessRecordErrorHandler<K, V> processRecordErrorHandler;

  private final BackPressureGauge<Integer, Integer, Integer> backPressureGauge;

  private AsyncRecordHandler<K, V> businessHandler;

  private TenantEntitlementFilter entitlementFilter;

  @Getter
  private int loadLimit;

  private int loadBottomGreenLine;

  private KafkaConsumer<K, V> kafkaConsumer;

  @Setter
  private String groupInstanceId;

  @Builder
  private KafkaConsumerWrapper(Vertx vertx, Context context, KafkaConfig kafkaConfig,
                               SubscriptionDefinition subscriptionDefinition, Boolean addToGlobalLoad,
                               GlobalLoadSensor globalLoadSensor,
                               ProcessRecordErrorHandler<K, V> processRecordErrorHandler,
                               BackPressureGauge<Integer, Integer, Integer> backPressureGauge, int loadLimit,
                               String groupInstanceId) {
    this.vertx = vertx;
    this.context = context;
    this.kafkaConfig = kafkaConfig;
    this.subscriptionDefinition = subscriptionDefinition;
    this.consumerDescriptor = "id: %d subscriptionPattern: %s".formatted(id, subscriptionDefinition);
    this.globalLoadSensor = globalLoadSensor;
    this.shouldAddToGlobalLoad = addToGlobalLoad != null ? addToGlobalLoad : true;
    this.processRecordErrorHandler = processRecordErrorHandler;
    this.groupInstanceId = groupInstanceId;
    this.backPressureGauge = backPressureGauge != null
                             ? backPressureGauge
                             // Just the simplest gauge - if the local load is greater than the threshold and above zero
                             : (g, l, t) -> l > 0 && l > t;
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  public Future<Void> start(AsyncRecordHandler<K, V> businessHandler, String moduleName) {
    LOGGER.debug("start:: KafkaConsumerWrapper is starting for module: {}", moduleName);

    String validationFailureMessage = validateStartParameters(businessHandler);
    if (validationFailureMessage != null) {
      return Future.failedFuture(validationFailureMessage);
    }

    this.businessHandler = businessHandler;
    this.entitlementFilter = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, moduleName);

    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
      KafkaTopicNameHelper.formatGroupName(subscriptionDefinition.getEventType(), moduleName));
    consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);

    kafkaConsumer = KafkaConsumer.create(vertx, consumerProps);

    kafkaConsumer.handler(this);
    kafkaConsumer.exceptionHandler(
      throwable -> LOGGER.error("start:: Error while KafkaConsumerWrapper is working: ", throwable));

    Pattern pattern = Pattern.compile(subscriptionDefinition.getSubscriptionPattern());
    return kafkaConsumer.subscribe(pattern)
      .onSuccess(ar -> LOGGER.info("start:: Consumer created - {}", consumerDescriptor))
      .onFailure(throwable -> LOGGER.error("start:: Consumer creation failed", throwable));
  }

  private String validateStartParameters(AsyncRecordHandler<K, V> businessHandler) {
    if (businessHandler == null) {
      String failureMessage = "start:: businessHandler must be provided and can't be null.";
      LOGGER.error(failureMessage);
      return failureMessage;
    }

    if (subscriptionDefinition == null || StringUtils.isBlank(subscriptionDefinition.getSubscriptionPattern())) {
      String failureMessage = "start:: subscriptionPattern can't be null nor empty. " + subscriptionDefinition;
      LOGGER.error(failureMessage);
      return failureMessage;
    }

    if (loadLimit < 1) {
      String failureMessage = "start:: loadLimit must be greater than 0. Current value is " + loadLimit;
      LOGGER.error(failureMessage);
      return failureMessage;
    }

    if (groupInstanceId != null && groupInstanceId.isBlank()) {
      String failureMessage = INVALID_GROUP_INSTANCE_ID_MSG.formatted(groupInstanceId);
      LOGGER.error("start:: {}", failureMessage);
      return failureMessage;
    }

    return null;
  }

  public void setLoadLimit(int loadLimit) {
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  /**
   * Pauses kafka consumer.
   */
  public void pause() {
    kafkaConsumer.pause();
    isPaused.set(true);
  }

  /**
   * Pauses the kafka consumer and enabling a periodic check to see the load is under the threshold and resume
   * the consumer automatically.
   */
  public void pauseWithPeriodicCheck() {
    pause();
    startPeriodicCheck();
  }

  /**
   * Check if consumer is paused.
   */
  public boolean isConsumerPaused() {
    return isPaused.get();
  }

  /**
   * Resumes kafka consumer.
   */
  public void resume() {
    kafkaConsumer.resume();
    isPaused.set(false);
  }

  public void fetch(long amount) {
    kafkaConsumer.fetch(amount);
    isPaused.set(false);
  }

  /**
   * Gets usage demand to determine if consumer paused.
   *
   * @return 0 if consumer paused, otherwise any value greater than 0 would mean that consumer working.
   */
  public long demand() {
    return kafkaConsumer.demand();
  }

  public Future<Void> stop() {
    LOGGER.debug("stop:: KafkaConsumerWrapper is stopping");
    return kafkaConsumer.unsubscribe()
      .onSuccess(ar -> LOGGER.info("stop:: Consumer unsubscribed - {}", consumerDescriptor))
      .onFailure(throwable -> LOGGER.error("stop:: Consumer was not unsubscribed - {}", consumerDescriptor,
        throwable))
      .compose(x -> kafkaConsumer.close()
        .onSuccess(ar -> LOGGER.info("stop:: Consumer closed - {}", consumerDescriptor))
        .onFailure(throwable -> LOGGER.error("stop:: Consumer was not closed - {}", consumerDescriptor,
          throwable)));
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> consumerRecord) {
    LOGGER.trace("handle:: Handling record: {}", consumerRecord);
    int globalLoad = getGlobalLoadSensorForMutation().increment();

    int currentLoad = localLoadSensor.incrementAndGet();

    if (backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit) && !isConsumerPaused()) {
      pauseWithPeriodicCheck();
      LOGGER.info("handle:: Consumer - {} kafkaConsumer.pause() requested currentLoad: {}, globalLoad: {}, "
                  + "loadLimit: {}", consumerDescriptor, currentLoad, globalLoad, loadLimit);
    }

    LOGGER.debug("handle:: Consumer - {} a Record has been received. key: {} currentLoad: {} globalLoad: {}",
      consumerDescriptor, consumerRecord.key(), currentLoad,
      globalLoadSensor != null ? String.valueOf(globalLoadSensor.current()) : "N/A");

    populateLoggingContext(consumerRecord);

    if (entitlementFilter != null && applyEntitlementFilter(consumerRecord)) {
      return;
    }

    businessHandler.handle(consumerRecord).onComplete(businessHandlerCompletionHandler(consumerRecord));
  }

  /**
   * Applies the tenant entitlement filter to the record, completing it (skip or filter-error) without
   * involving the business handler when needed.
   *
   * @return {@code true} if the record has already been completed and {@link #handle} should return
   */
  private boolean applyEntitlementFilter(KafkaConsumerRecord<K, V> consumerRecord) {
    try {
      if (!entitlementFilter.shouldSkip(consumerRecord)) {
        return false;
      }
      LOGGER.info("applyEntitlementFilter:: Consumer - {} Skipping record for non-entitled tenant: key: {}",
        consumerDescriptor, consumerRecord.key());
      businessHandlerCompletionHandler(consumerRecord).handle(Future.<K>succeededFuture(null));
    } catch (RuntimeException e) {
      LOGGER.error("applyEntitlementFilter:: Consumer - {} Tenant entitlement filter failed for record - key: {}",
        consumerDescriptor, consumerRecord.key(), e);
      businessHandlerCompletionHandler(consumerRecord).handle(Future.<K>failedFuture(e));
    }
    return true;
  }

  private void populateLoggingContext(KafkaConsumerRecord<K, V> consumerRecord) {
    consumerRecord.headers().forEach(header -> {
      String key = header.key();
      if (key == null) {
        return;
      }
      String value = header.value() == null ? "" : header.value().toString();

      if (key.equalsIgnoreCase(XOkapiHeaders.REQUEST_ID)) {
        FolioLoggingContext.put(FolioLocal.REQUEST_ID, value);
      } else if (key.equalsIgnoreCase(XOkapiHeaders.TENANT)) {
        FolioLoggingContext.put(FolioLocal.TENANT_ID, value);
      } else if (key.equalsIgnoreCase(XOkapiHeaders.USER_ID)) {
        FolioLoggingContext.put(FolioLocal.USER_ID, value);
      }
    });
  }

  /**
   * Periodically check if the consumer can be resumed.
   */
  private void startPeriodicCheck() {
    vertx.setPeriodic(PERIODIC_CHECK_INTERVAL, timerId -> {
      int globalLoad = getGlobalLoadSensorForMutation().current();
      int currentLoad = localLoadSensor.get();
      LOGGER.debug("periodicCheck:: Consumer - {} checking if consumer can resume. currentLoad: {} globalLoad: {}",
        consumerDescriptor, currentLoad, globalLoad);
      if (!backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit)) {
        resume();
        vertx.cancelTimer(timerId);
      }
    });
  }

  private Handler<AsyncResult<K>> businessHandlerCompletionHandler(KafkaConsumerRecord<K, V> consumerRecord) {
    LOGGER.debug("businessHandlerCompletionHandler:: Consumer - {} Starting business completion handler, "
                 + "globalLoadSensor: {}", consumerDescriptor, globalLoadSensor.current());
    return har -> {
      long offset = consumerRecord.offset() + 1;
      try {
        commitOffset(consumerRecord, offset);
        handleBusinessResult(har, consumerRecord, offset);
      } finally {
        adjustLoadAfterProcessing();
      }
    };
  }

  private void commitOffset(KafkaConsumerRecord<K, V> consumerRecord, long offset) {
    Map<TopicPartition, OffsetAndMetadata> offsets = HashMap.newHashMap(2);
    TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, null);
    offsets.put(topicPartition, offsetAndMetadata);
    LOGGER.debug("businessHandlerCompletionHandler:: Consumer - {} Committing offset: {}",
      consumerDescriptor, offset);
    kafkaConsumer.commit(offsets)
      .onSuccess(ar -> LOGGER.info("businessHandlerCompletionHandler:: Consumer - {} Committed offset: {}",
        consumerDescriptor, offset))
      .onFailure(throwable -> LOGGER.error(
        "businessHandlerCompletionHandler:: Consumer - {} Error while commit offset: {}",
        consumerDescriptor, offset, throwable));
  }

  private void handleBusinessResult(AsyncResult<K> har, KafkaConsumerRecord<K, V> consumerRecord, long offset) {
    if (har.failed()) {
      if (har.cause() instanceof DuplicateEventException) {
        LOGGER.info("businessHandlerCompletionHandler:: Duplicate event for a record - {} offset: {} has been "
                    + "skipped, logging more info about it in error handler", consumerDescriptor, offset);
      } else {
        LOGGER.error("businessHandlerCompletionHandler:: Error while processing a record - {} offset: {}",
          consumerDescriptor, offset, har.cause());
      }
      if (processRecordErrorHandler != null) {
        LOGGER.info("businessHandlerCompletionHandler:: Starting error handler to process failures for a record - "
                    + "{} offset: {} and send DI_ERROR events", consumerDescriptor, offset);
        processRecordErrorHandler.handle(har.cause(), consumerRecord);
      } else {
        LOGGER.warn("businessHandlerCompletionHandler:: Error handler has not been implemented "
                    + "for subscriptionPattern: {} failures", subscriptionDefinition);
      }
    }
  }

  private void adjustLoadAfterProcessing() {
    int actualCurrentLoad = localLoadSensor.decrementAndGet();

    int globalLoad = getGlobalLoadSensorForMutation().decrement();

    if (!backPressureGauge.isThresholdExceeded(globalLoad, actualCurrentLoad, loadBottomGreenLine)
        && isConsumerPaused()) {
      resume();
      LOGGER.info("businessHandlerCompletionHandler:: Consumer - {} kafkaConsumer.resume() requested "
                  + "currentLoad: {} loadBottomGreenLine: {}", consumerDescriptor, actualCurrentLoad,
        loadBottomGreenLine);
    }
  }

  private GlobalLoadSensor getGlobalLoadSensorForMutation() {
    return globalLoadSensor != null && shouldAddToGlobalLoad ? globalLoadSensor : GLOBAL_SENSOR_NA;
  }
}
