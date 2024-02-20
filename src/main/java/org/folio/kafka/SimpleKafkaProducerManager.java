package org.folio.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleKafkaProducerManager implements KafkaProducerManager {
  private static final String PRODUCER_SUFFIX = "_Producer";
  private final Map<String, KafkaProducer<?, ?>> producers = new ConcurrentHashMap<>();
  private static final Logger LOGGER = LogManager.getLogger();

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  public SimpleKafkaProducerManager(Vertx vertx, KafkaConfig kafkaConfig) {
    super();
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public <K, V> KafkaProducer<K, V> createShared(String producerName) {
    String key = producerName + PRODUCER_SUFFIX;
    return (KafkaProducer<K, V>) producers.computeIfAbsent(key, k -> KafkaProducer.createShared(vertx, k, kafkaConfig.getProducerProps()));
  }

  public void closeAllProducers() {
    producers.forEach((name, producer) -> producer.close().onComplete(ar -> {
      if (ar.succeeded()) {
        LOGGER.info("Closed producer: {}", name);
      } else {
        LOGGER.warn("Failed to close producer: {}", name);
      }
    }));
  }
}
