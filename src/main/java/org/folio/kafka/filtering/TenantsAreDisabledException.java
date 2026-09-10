package org.folio.kafka.filtering;

/**
 * Thrown when no tenants are entitled to the current module and the configured
 * {@link DisabledTenantStrategy} for that case is {@link DisabledTenantStrategy#FAIL}.
 */
public final class TenantsAreDisabledException extends RuntimeException {

  private TenantsAreDisabledException(String message) {
    super(message);
  }

  public static TenantsAreDisabledException of(String moduleId) {
    return new TenantsAreDisabledException("No tenants are entitled to module '%s'".formatted(moduleId));
  }
}
