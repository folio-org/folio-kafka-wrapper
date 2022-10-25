package org.folio.kafka.services;

public enum TestKafkaTopics implements KafkaTopic {
  TOPIC_ONE("topic1", 10),
  TOPIC_TWO("topic2", 20),
  TOPIC_THREE("topic3", 30);

  private final String topic;
  private final int partitions;

  TestKafkaTopics(String topic, int partitions) {
   this.topic = topic;
   this.partitions = partitions;
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
  public int numPartitions() {
    return partitions;
  }
}
