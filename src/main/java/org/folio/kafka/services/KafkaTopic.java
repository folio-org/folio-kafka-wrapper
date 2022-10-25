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
   * Returns num partitions
   * Default - 50
   */
  default int numPartitions() {
    return 50;
  }

  /**
   * Returns replication factor.
   * Default - 0
   */
  default short replicationFactor(){
    return 0;
  }

  /**
   * Returns full topic name.
   * Order: {environment}.{tenantId}.{modulePrefix}.{topicName}
   */
  default String fullTopicName(String environment, String tenant) {
    return formatTopicName(environment, tenant, moduleName(), topicName());
  }
}
