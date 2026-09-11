package org.folio.kafka.filtering;

import org.folio.kafka.SimpleConfigurationReader;

/**
 * Environment/system-property driven configuration for entitlement-based Kafka consumer
 * filtering. Off by default - a module opts in explicitly via {@code FOLIO_KAFKA_TENANT_FILTER_ENABLED}.
 *
 * <p>Names match folio-spring-kafka's {@code folio.kafka.tenant-filter.*} Spring Boot properties, so both libraries
 * share the same env vars for the settings they have in common.
 */
public final class TenantEntitlementFilterProperties {

  public static final String ENABLED = "FOLIO_KAFKA_TENANT_FILTER_ENABLED";
  public static final String TENANT_DISABLED_STRATEGY = "FOLIO_KAFKA_TENANT_FILTER_TENANT_DISABLED_STRATEGY";
  public static final String ALL_TENANTS_DISABLED_STRATEGY =
    "FOLIO_KAFKA_TENANT_FILTER_ALL_TENANTS_DISABLED_STRATEGY";
  public static final String ENTITLEMENT_REFRESH_INTERVAL_SECONDS =
    "FOLIO_KAFKA_TENANT_FILTER_ENTITLEMENT_REFRESH_INTERVAL_SECONDS";

  private static final String ENABLED_DEFAULT = "false";
  private static final String TENANT_DISABLED_STRATEGY_DEFAULT = "SKIP";
  private static final String ALL_TENANTS_DISABLED_STRATEGY_DEFAULT = "FAIL";
  private static final String ENTITLEMENT_REFRESH_INTERVAL_SECONDS_DEFAULT = "900";

  private TenantEntitlementFilterProperties() {
  }

  public static boolean isEnabled() {
    return Boolean.parseBoolean(SimpleConfigurationReader.getValue(ENABLED, ENABLED_DEFAULT));
  }

  public static DisabledTenantStrategy tenantDisabledStrategy() {
    return DisabledTenantStrategy.valueOf(
      SimpleConfigurationReader.getValue(TENANT_DISABLED_STRATEGY, TENANT_DISABLED_STRATEGY_DEFAULT));
  }

  public static DisabledTenantStrategy allTenantsDisabledStrategy() {
    return DisabledTenantStrategy.valueOf(
      SimpleConfigurationReader.getValue(ALL_TENANTS_DISABLED_STRATEGY, ALL_TENANTS_DISABLED_STRATEGY_DEFAULT));
  }

  /**
   * Returns the configured refresh interval in milliseconds.
   */
  public static long entitlementRefreshIntervalMs() {
    var seconds = Long.parseLong(SimpleConfigurationReader.getValue(ENTITLEMENT_REFRESH_INTERVAL_SECONDS,
      ENTITLEMENT_REFRESH_INTERVAL_SECONDS_DEFAULT));
    return seconds * 1000;
  }
}
