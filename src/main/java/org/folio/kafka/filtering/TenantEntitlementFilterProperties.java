package org.folio.kafka.filtering;

import org.folio.kafka.SimpleConfigurationReader;

/**
 * Environment/system-property driven configuration for entitlement-based Kafka consumer
 * filtering. Off by default - a module opts in explicitly via {@code KAFKA_TENANT_FILTER_ENABLED}.
 */
public final class TenantEntitlementFilterProperties {

  public static final String ENABLED = "KAFKA_TENANT_FILTER_ENABLED";
  public static final String TENANT_DISABLED_STRATEGY = "KAFKA_TENANT_FILTER_TENANT_DISABLED_STRATEGY";
  public static final String ALL_TENANTS_DISABLED_STRATEGY = "KAFKA_TENANT_FILTER_ALL_TENANTS_DISABLED_STRATEGY";
  public static final String ENTITLEMENT_REFRESH_INTERVAL_MS = "KAFKA_TENANT_FILTER_REFRESH_INTERVAL_MS";

  private static final String ENABLED_DEFAULT = "false";
  private static final String TENANT_DISABLED_STRATEGY_DEFAULT = "SKIP";
  private static final String ALL_TENANTS_DISABLED_STRATEGY_DEFAULT = "FAIL";
  private static final String ENTITLEMENT_REFRESH_INTERVAL_MS_DEFAULT = "900000";

  private TenantEntitlementFilterProperties() {
  }

  public static boolean enabled() {
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

  public static long entitlementRefreshIntervalMs() {
    return Long.parseLong(
      SimpleConfigurationReader.getValue(ENTITLEMENT_REFRESH_INTERVAL_MS, ENTITLEMENT_REFRESH_INTERVAL_MS_DEFAULT));
  }
}
