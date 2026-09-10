package org.folio.kafka.filtering;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;
import org.folio.okapi.common.XOkapiHeaders;

/**
 * Decides whether a Kafka record should be skipped because its tenant is not currently entitled
 * to the current module.
 */
@Log4j2
public class TenantEntitlementFilter {

  private final String moduleId;
  private final TenantEntitlementService tenantEntitlementService;
  private final DisabledTenantStrategy tenantDisabledStrategy;
  private final DisabledTenantStrategy allTenantsDisabledStrategy;

  public TenantEntitlementFilter(String moduleId, TenantEntitlementService tenantEntitlementService,
    DisabledTenantStrategy tenantDisabledStrategy, DisabledTenantStrategy allTenantsDisabledStrategy) {
    if (isBlank(moduleId)) {
      throw new IllegalArgumentException("Module ID must not be blank");
    }

    this.moduleId = moduleId;
    this.tenantEntitlementService = Objects.requireNonNull(tenantEntitlementService,
      "tenantEntitlementService must not be null");
    this.tenantDisabledStrategy = Objects.requireNonNull(tenantDisabledStrategy,
      "tenantDisabledStrategy must not be null");
    this.allTenantsDisabledStrategy = Objects.requireNonNull(allTenantsDisabledStrategy,
      "allTenantsDisabledStrategy must not be null");
  }

  /**
   * Returns whether the given record should be skipped rather than handed to the business handler.
   *
   * @param consumerRecord the record to test
   * @return {@code true} if the record should be skipped
   */
  public boolean shouldSkip(KafkaConsumerRecord<?, ?> consumerRecord) {
    var tenant = resolveTenant(consumerRecord);
    if (tenant == null) {
      return false;
    }

    var enabledTenants = tenantEntitlementService.getEnabledTenants();
    if (enabledTenants == null) {
      log.debug("Tenant entitlement cache not yet populated: moduleId = {}. Accepting record.", moduleId);
      return false;
    }

    var result = filterByEnabledTenants(enabledTenants, tenant);
    log.debug("Message for tenant is {}: moduleId = {}, tenant = {}", result ? "skipped" : "accepted",
      moduleId, tenant);
    return result;
  }

  private String resolveTenant(KafkaConsumerRecord<?, ?> consumerRecord) {
    for (var header : consumerRecord.headers()) {
      if (XOkapiHeaders.TENANT.equalsIgnoreCase(header.key())) {
        var value = header.value();
        var tenant = value == null ? null : trimToNull(value.toString());
        if (tenant != null) {
          return tenant;
        }
      }
    }

    log.warn("Received message with missing or blank {} header: moduleId = {}. Filter won't be applied.",
      XOkapiHeaders.TENANT, moduleId);
    return null;
  }

  private boolean filterByEnabledTenants(Set<String> enabledTenants, String currentTenant) {
    if (enabledTenants.isEmpty()) {
      log.warn("No tenants are entitled to module '{}'. Applying 'no entitled tenants' strategy: {}",
        moduleId, allTenantsDisabledStrategy);
      return applyStrategy(allTenantsDisabledStrategy, () -> TenantsAreDisabledException.of(moduleId));
    }

    var notEnabled = !enabledTenants.contains(currentTenant);
    return notEnabled && applyStrategy(tenantDisabledStrategy,
      () -> TenantIsDisabledException.of(currentTenant, moduleId));
  }

  private static boolean applyStrategy(DisabledTenantStrategy strategy, Supplier<RuntimeException> exceptionSupplier) {
    return switch (strategy) {
      case ACCEPT -> false;
      case SKIP -> true;
      case FAIL -> throw exceptionSupplier.get();
    };
  }
}
