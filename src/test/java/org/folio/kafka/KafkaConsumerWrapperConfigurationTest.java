package org.folio.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class KafkaConsumerWrapperConfigurationTest {

  private static final String EVENT_TYPE = "test-event";
  private static final String CONSUMER_GROUP_SUFFIX = "mod-test-1.0.0";

  @Test
  void createConsumerProperties_positive_usesConfiguredDefaultWhenOverrideIsMissing() {
    var consumerWrapper = consumerWrapper(null);

    var consumerProperties = consumerWrapper.createConsumerProperties(CONSUMER_GROUP_SUFFIX);

    assertEquals(KafkaConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG_DEFAULT,
      consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  @Test
  void createConsumerProperties_positive_usesPerConsumerOverride() {
    var consumerWrapper = consumerWrapper("latest");

    var consumerProperties = consumerWrapper.createConsumerProperties(CONSUMER_GROUP_SUFFIX);

    assertEquals("latest", consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  @Test
  void createConsumerProperties_positive_ignoresBlankOverride() {
    var consumerWrapper = consumerWrapper(" ");

    var consumerProperties = consumerWrapper.createConsumerProperties(CONSUMER_GROUP_SUFFIX);

    assertEquals(KafkaConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG_DEFAULT,
      consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  private static KafkaConsumerWrapper<String, String> consumerWrapper(String autoOffsetReset) {
    var kafkaConfig = KafkaConfig.builder()
      .kafkaHost("localhost")
      .kafkaPort("9092")
      .build();
    var subscriptionDefinition = SubscriptionDefinition.builder()
      .eventType(EVENT_TYPE)
      .subscriptionPattern(".*")
      .build();

    return KafkaConsumerWrapper.<String, String>builder()
      .kafkaConfig(kafkaConfig)
      .subscriptionDefinition(subscriptionDefinition)
      .autoOffsetReset(autoOffsetReset)
      .build();
  }
}
