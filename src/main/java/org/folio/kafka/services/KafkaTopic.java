package org.folio.kafka.services;

import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;

public interface KafkaTopic {

  /**
   * Returns module name
   */
  String moduleName();

  /**
   * Returns topic name
   */
  String topicName();

  /**
   * Return num partitions
   */
  int numPartitions();

  /**
   * Return replication factor
   */
  short replicationFactor();

  /**
   * Return full topic name
   */
  default String fullTopicName(String environment, String tenant) {
    return formatTopicName(environment, tenant, moduleName(), topicName());
  }
}
