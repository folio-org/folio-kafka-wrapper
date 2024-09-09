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

  @Override
  public Integer messageRetentionTime() {
    if (this == TOPIC_THREE) {
      return 1000;
    }
    return null;
  }

  @Override
  public Integer messageMaxSize() {
    if (this == TOPIC_THREE) {
      return 2000;
    }
    return null;
  }


}
