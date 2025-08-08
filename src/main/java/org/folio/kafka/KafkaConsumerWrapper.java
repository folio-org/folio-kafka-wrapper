package org.folio.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.okapi.common.logging.FolioLoggingContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class KafkaConsumerWrapper<K, V> implements Handler<KafkaConsumerRecord<K, V>> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final GlobalLoadSensor GLOBAL_SENSOR_NA = new GlobalLoadSensor.GlobalLoadSensorNA();

  private static final AtomicInteger indexer = new AtomicInteger();

  private final int id = indexer.getAndIncrement();

  private final AtomicInteger localLoadSensor = new AtomicInteger();

  private final AtomicBoolean isPaused = new AtomicBoolean(false);

  private final long periodicCheckInterval = 3000;

  private final Vertx vertx;

  private final Context context;

  private final KafkaConfig kafkaConfig;

  private final SubscriptionDefinition subscriptionDefinition;

  private final GlobalLoadSensor globalLoadSensor;

  private final boolean shouldAddToGlobalLoad;

  private final ProcessRecordErrorHandler<K, V> processRecordErrorHandler;

  private final BackPressureGauge<Integer, Integer, Integer> backPressureGauge;

  private AsyncRecordHandler<K, V> businessHandler;

  private int loadLimit;

  private int loadBottomGreenLine;

  private KafkaConsumer<K, V> kafkaConsumer;

  private String groupInstanceId;

  public int getLoadLimit() {
    return loadLimit;
  }

  public void setLoadLimit(int loadLimit) {
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  public void setGroupInstanceId(String groupInstanceId) {
    this.groupInstanceId = groupInstanceId;
  }

  @Builder
  private KafkaConsumerWrapper(Vertx vertx, Context context, KafkaConfig kafkaConfig, SubscriptionDefinition subscriptionDefinition, Boolean addToGlobalLoad,
                               GlobalLoadSensor globalLoadSensor, ProcessRecordErrorHandler<K, V> processRecordErrorHandler, BackPressureGauge<Integer, Integer, Integer> backPressureGauge, int loadLimit,
                               String groupInstanceId) {
    this.vertx = vertx;
    this.context = context;
    this.kafkaConfig = kafkaConfig;
    this.subscriptionDefinition = subscriptionDefinition;
    this.globalLoadSensor = globalLoadSensor;
    this.shouldAddToGlobalLoad = addToGlobalLoad != null ? addToGlobalLoad : true;
    this.processRecordErrorHandler = processRecordErrorHandler;
    this.groupInstanceId  = groupInstanceId;
    this.backPressureGauge = backPressureGauge != null ?
      backPressureGauge :
      (g, l, t) -> l > 0 && l > t; // Just the simplest gauge - if the local load is greater than the threshold and above zero
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  public Future<Void> start(AsyncRecordHandler<K, V> businessHandler, String moduleName) {
    LOGGER.debug("start:: KafkaConsumerWrapper is starting for module: {}", moduleName);

    if (businessHandler == null) {
      String failureMessage = "start:: businessHandler must be provided and can't be null.";
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (subscriptionDefinition == null || StringUtils.isBlank(subscriptionDefinition.getSubscriptionPattern())) {
      String failureMessage = "start:: subscriptionPattern can't be null nor empty. " + subscriptionDefinition;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (loadLimit < 1) {
      String failureMessage = "start:: loadLimit must be greater than 0. Current value is " + loadLimit;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    this.businessHandler = businessHandler;
    Promise<Void> startPromise = Promise.promise();

    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaTopicNameHelper.formatGroupName(subscriptionDefinition.getEventType(), moduleName));

    if (groupInstanceId != null) {
      consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
    }

    kafkaConsumer = KafkaConsumer.create(vertx, consumerProps);

    kafkaConsumer.handler(this);
    kafkaConsumer.exceptionHandler(throwable -> LOGGER.error("start:: Error while KafkaConsumerWrapper is working: ", throwable));

    Pattern pattern = Pattern.compile(subscriptionDefinition.getSubscriptionPattern());
    kafkaConsumer.subscribe(pattern, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("start:: Consumer created - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
        startPromise.complete();
      } else {
        LOGGER.error("start:: Consumer creation failed", ar.cause());
        startPromise.fail(ar.cause());
      }
    });

    return startPromise.future();
  }

  /**
   * Gets consumer id.
   *
   * @return consumer id
   */
  public int getId() {
    return id;
  }

  /**
   * periodically check if the consumer can be resumed
   */
  private void startPeriodicCheck() {
    vertx.setPeriodic(periodicCheckInterval, timerId -> {
      int globalLoad = getGlobalLoadSensorForMutation().current();
      int currentLoad = localLoadSensor.get();
      LOGGER.debug("periodicCheck:: Consumer - id: {} subscriptionPattern: {} checking if consumer can resume. currentLoad: {} globalLoad: {}",
        id, subscriptionDefinition, currentLoad, globalLoad);
      if (!backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit)) {
        resume();
        vertx.cancelTimer(timerId);
      }
    });
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
    Promise<Void> stopPromise = Promise.promise();
    kafkaConsumer.unsubscribe(uar -> {
        if (uar.succeeded()) {
          LOGGER.info("stop:: Consumer unsubscribed - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
        } else {
          LOGGER.error("stop:: Consumer was not unsubscribed - id: {} subscriptionPattern: {}", id, subscriptionDefinition, uar.cause());
        }
        kafkaConsumer.close(car -> {
          if (uar.succeeded()) {
            LOGGER.info("stop:: Consumer closed - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
          } else {
            LOGGER.error("stop:: Consumer was not closed - id: {} subscriptionPattern: {}", id, subscriptionDefinition, car.cause());
          }
          stopPromise.complete();
        });
      }
    );

    return stopPromise.future();
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> record) {
    LOGGER.trace("handle:: Handling record: {}", record);
    int globalLoad = getGlobalLoadSensorForMutation().increment();

    int currentLoad = localLoadSensor.incrementAndGet();

    if (backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit) && !isConsumerPaused()) {
      pauseWithPeriodicCheck();
      LOGGER.info("handle:: Consumer - id: {} subscriptionPattern: {} kafkaConsumer.pause() requested" + " currentLoad: {}, globalLoad: {}, loadLimit: {}",
        id, subscriptionDefinition, currentLoad, globalLoad, loadLimit);
    }

    LOGGER.debug("handle:: Consumer - id: {} subscriptionPattern: {} a Record has been received. key: {} currentLoad: {} globalLoad: {}",
      id, subscriptionDefinition, record.key(), currentLoad, globalLoadSensor != null ? String.valueOf(globalLoadSensor.current()) : "N/A");

    // populate logging context
    record.headers().forEach(header -> {
      String key = header.key();
      if (key == null) return;
      String value = header.value() == null ? "" : header.value().toString();

      if (key.equalsIgnoreCase(XOkapiHeaders.REQUEST_ID)) {
        FolioLoggingContext.put(FolioLoggingContext.REQUEST_ID_LOGGING_VAR_NAME, value);
      } else if (key.equalsIgnoreCase(XOkapiHeaders.TENANT)) {
        FolioLoggingContext.put(FolioLoggingContext.TENANT_ID_LOGGING_VAR_NAME, value);
      } else if (key.equalsIgnoreCase(XOkapiHeaders.USER_ID)) {
        FolioLoggingContext.put(FolioLoggingContext.USER_ID_LOGGING_VAR_NAME, value);
      }
    });

    businessHandler.handle(record).onComplete(businessHandlerCompletionHandler(record));

  }

  private Handler<AsyncResult<K>> businessHandlerCompletionHandler(KafkaConsumerRecord<K, V> record) {
    LOGGER.debug("businessHandlerCompletionHandler:: Consumer - id: {} subscriptionPattern: {} Starting business completion handler, globalLoadSensor: {}", id, subscriptionDefinition, globalLoadSensor.current());
    return har -> {
      try {
        long offset = record.offset() + 1;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, null);
        offsets.put(topicPartition, offsetAndMetadata);
        LOGGER.debug("businessHandlerCompletionHandler:: Consumer - id: {} subscriptionPattern: {} Committing offset: {}", id, subscriptionDefinition, offset);
        kafkaConsumer.commit(offsets, ar -> {
          if (ar.succeeded()) {
            LOGGER.info("businessHandlerCompletionHandler:: Consumer - id: {} subscriptionPattern: {} Committed offset: {}", id, subscriptionDefinition, offset);
          } else {
            LOGGER.error("businessHandlerCompletionHandler:: Consumer - id: {} subscriptionPattern: {} Error while commit offset: {}", id, subscriptionDefinition, offset, ar.cause());
          }
        });

        if (har.failed()) {
          if (har.cause() instanceof DuplicateEventException) {
            LOGGER.info("businessHandlerCompletionHandler:: Duplicate event for a record - id: {} subscriptionPattern: {} offset: {} has been skipped, logging more info about it in error handler", id, subscriptionDefinition, offset);
          } else {
            LOGGER.error("businessHandlerCompletionHandler:: Error while processing a record - id: {} subscriptionPattern: {} offset: {}", id, subscriptionDefinition, offset, har.cause());
          }
          if (processRecordErrorHandler != null) {
            LOGGER.info("businessHandlerCompletionHandler:: Starting error handler to process failures for a record - id: {} subscriptionPattern: {} offset: {} and send DI_ERROR events",
              id, subscriptionDefinition, offset);
            processRecordErrorHandler.handle(har.cause(), record);
          } else {
            LOGGER.warn("businessHandlerCompletionHandler:: Error handler has not been implemented for subscriptionPattern: {} failures", subscriptionDefinition);
          }
        }
      } finally {
        int actualCurrentLoad = localLoadSensor.decrementAndGet();

        int globalLoad = getGlobalLoadSensorForMutation().decrement();

        if (!backPressureGauge.isThresholdExceeded(globalLoad, actualCurrentLoad, loadBottomGreenLine) && isConsumerPaused()) {
            resume();
            LOGGER.info("businessHandlerCompletionHandler:: Consumer - id: {} subscriptionPattern: {} kafkaConsumer.resume() requested currentLoad: {} loadBottomGreenLine: {}", id, subscriptionDefinition, actualCurrentLoad, loadBottomGreenLine);
        }
      }
    };
  }

  private GlobalLoadSensor getGlobalLoadSensorForMutation() {
    return globalLoadSensor != null && shouldAddToGlobalLoad ? globalLoadSensor : GLOBAL_SENSOR_NA;
  }

}
