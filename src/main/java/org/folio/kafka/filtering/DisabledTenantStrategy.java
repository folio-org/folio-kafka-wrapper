package org.folio.kafka.filtering;

/**
 * Strategy applied when a tenant is not entitled to receive messages for the current module.
 */
public enum DisabledTenantStrategy {

  /** Accept the record and let the business handler process it. */
  ACCEPT,

  /** Silently skip the record without invoking the business handler. */
  SKIP,

  /** Throw a typed exception to signal the unexpected state. */
  FAIL
}
