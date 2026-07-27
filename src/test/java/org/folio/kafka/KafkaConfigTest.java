package org.folio.kafka;

import static org.folio.kafka.KafkaConfig.KAFKA_NUMBER_OF_PARTITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.folio.kafka.interceptors.TenantIdCheckInterceptor;
import org.junit.jupiter.api.Test;

class KafkaConfigTest {

  @Test
  void shouldReturnProducerProperties() {
    Map<String, String> producerProps = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .build()
      .getProducerProps();

    assertEquals("127.0.0.1:9092", producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("true", producerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.StringSerializer",
      producerProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.StringSerializer",
      producerProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

  @Test
  void shouldReturnConsumerProperties() {
    String maxPullRecordsValue = "500";
    System.setProperty(KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, maxPullRecordsValue);

    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .build();
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();

    assertEquals("127.0.0.1:9092", consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(KafkaConfig.KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG_DEFAULT,
      consumerProps.get(ConsumerConfig.METADATA_MAX_AGE_CONFIG));
    assertEquals(KafkaConfig.KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG_DEFAULT,
      consumerProps.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
    assertEquals(maxPullRecordsValue, consumerProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.StringDeserializer",
      consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.StringDeserializer",
      consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
  }

  @Test
  void shouldReturnConsumerPropertiesWithCustomDeserializers() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .consumerKeyDeserializerClass("org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .consumerValueDeserializerClass("org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .build();
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();

    assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer",
      consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer",
      consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
  }

  @Test
  void shouldReturnPartitionsNumberFromSystemProperties() {
    System.setProperty(KAFKA_NUMBER_OF_PARTITIONS, "5");
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .build();

    assertEquals(5, kafkaConfig.getNumberOfPartitions());
  }

  @Test
  void shouldHaveTenantIdInterceptorSet() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .build();

    Map<String, String> producerProps = kafkaConfig.getProducerProps();

    assertEquals(TenantIdCheckInterceptor.class.getName(),
      producerProps.getOrDefault(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ""));
  }
}
