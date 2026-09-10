package org.folio.kafka.filtering;

/**
 * Thrown when a Kafka record's tenant is not entitled to the current module and the
 * configured {@link DisabledTenantStrategy} is {@link DisabledTenantStrategy#FAIL}.
 */
public final class TenantIsDisabledException extends RuntimeException {

  private TenantIsDisabledException(String message) {
    super(message);
  }

  public static TenantIsDisabledException of(String tenant, String moduleId) {
    return new TenantIsDisabledException(
      "Tenant '%s' is not entitled to module '%s'".formatted(tenant, moduleId));
  }
}
