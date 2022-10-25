package org.folio.kafka.exception;

public class ProducerCreationException extends RuntimeException {
  private static final String MESSAGE = "Failed to parse producer value. Reason: %s";

  public ProducerCreationException(String reason) {
    super(String.format(MESSAGE, reason));
  }
}
