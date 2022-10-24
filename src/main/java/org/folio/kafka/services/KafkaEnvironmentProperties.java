package org.folio.kafka.services;

import static java.lang.Short.parseShort;
import static java.lang.System.getenv;

public final class KafkaEnvironmentProperties {

  private KafkaEnvironmentProperties() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static String getPort() {
    return getenv().getOrDefault("KAFKA_PORT", "9092");
  }

  public static String getHost() {
    return getenv().getOrDefault("KAFKA_HOST", "localhost");
  }

  public static short getReplicationFactor() {
    return parseShort(getenv().getOrDefault("REPLICATION_FACTOR", "1"));
  }
}
