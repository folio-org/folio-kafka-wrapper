package org.folio.kafka.services;

public enum TestKafkaTopic implements KafkaTopic {
  TOPIC_ONE("topic1"),
  TOPIC_TWO("topic2"),
  TOPIC_THREE("topic3");

  private final String topic;

  TestKafkaTopic(String topic) {
   this.topic = topic;
  }

  @Override
  public String moduleName() {
    return "kafka-wrapper";
  }

  @Override
  public String topicName() {
    return topic;
  }
}
