package org.folio.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.folio.kafka.interceptors.TenantIdCheckInterceptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.folio.kafka.KafkaConfig.KAFKA_NUMBER_OF_PARTITIONS;

public class KafkaConfigTest {

    @Test
    public void shouldReturnProducerProperties() {
      Map<String, String> producerProps = KafkaConfig.builder()
        .kafkaHost("127.0.0.1")
        .kafkaPort("9092")
        .build()
        .getProducerProps();

      Assert.assertEquals("127.0.0.1:9092", producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
      Assert.assertEquals("true", producerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
      Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", producerProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
      Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", producerProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    public void shouldReturnConsumerProperties() {
      String maxPullRecordsValue = "500";
      System.setProperty(KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, maxPullRecordsValue);

      KafkaConfig kafkaConfig = KafkaConfig.builder()
        .kafkaHost("127.0.0.1")
        .kafkaPort("9092")
        .build();
      Map<String, String> consumerProps = kafkaConfig.getConsumerProps();

      Assert.assertEquals("127.0.0.1:9092", consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      Assert.assertEquals(KafkaConfig.KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG_DEFAULT, consumerProps.get(ConsumerConfig.METADATA_MAX_AGE_CONFIG));
      Assert.assertEquals(KafkaConfig.KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG_DEFAULT, consumerProps.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
      Assert.assertEquals(maxPullRecordsValue, consumerProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
      Assert.assertEquals("org.apache.kafka.common.serialization.StringDeserializer", consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
      Assert.assertEquals("org.apache.kafka.common.serialization.StringDeserializer", consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

      consumerProps = kafkaConfig.toBuilder()
        .consumerKeyDeserializerClass("org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .consumerValueDeserializerClass("org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .build()
        .getConsumerProps();

      Assert.assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
      Assert.assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

    }

    @Test
    public void shouldReturnPartitionsNumberFromSystemProperties() {
      System.setProperty(KAFKA_NUMBER_OF_PARTITIONS, "5");
      KafkaConfig kafkaConfig = KafkaConfig.builder()
        .kafkaHost("127.0.0.1")
        .kafkaPort("9092")
        .build();

      Assert.assertEquals(5, kafkaConfig.getNumberOfPartitions());
    }

  @Test
  public void shouldHaveTenantIdInterceptorSet() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("127.0.0.1")
      .kafkaPort("9092")
      .build();

    Map<String, String> producerProps = kafkaConfig.getProducerProps();

    Assert.assertEquals(TenantIdCheckInterceptor.class.getName(),
      producerProps.getOrDefault(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ""));
  }

}
