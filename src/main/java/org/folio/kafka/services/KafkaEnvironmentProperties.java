package org.folio.kafka.services;

import static java.lang.Short.parseShort;
import static java.lang.System.getenv;

public final class KafkaEnvironmentProperties {

  private KafkaEnvironmentProperties() { }

  public static String port() {
    return getenv().getOrDefault("KAFKA_PORT", "9092");
  }

  public static String host() {
    return getenv().getOrDefault("KAFKA_HOST", "localhost");
  }

  public static String environment() {
    return getenv().getOrDefault("ENV", "folio");
  }

  public static short replicationFactor() {
    return parseShort(getenv().getOrDefault("REPLICATION_FACTOR", "1"));
  }
}
