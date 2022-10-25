package org.folio.kafka.services;

public enum TestKafkaTopics implements KafkaTopic {
  TOPIC_ONE("topic1", 50),
  TOPIC_TWO("topic2", 50),
  TOPIC_THREE("topic3", 50);

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

  @Override
  public short replicationFactor() {
    return 0;
  }
}
