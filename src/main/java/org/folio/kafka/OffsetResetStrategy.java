package org.folio.kafka;

/**
 * Strategies used when Kafka has no committed offset for a consumer group.
 */
public enum OffsetResetStrategy {
  EARLIEST("earliest"),
  LATEST("latest"),
  NONE("none");

  private final String value;

  OffsetResetStrategy(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }
}
