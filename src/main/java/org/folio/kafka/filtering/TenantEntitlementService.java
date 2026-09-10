package org.folio.kafka.filtering;

import static org.apache.commons.lang3.StringUtils.isBlank;

import io.vertx.core.Future;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * Resolves tenants entitled to the current module.
 *
 * <p>The entitlement info is cached, kept current by {@link #applyEntitlementEvent(EntitlementEvent)}
 * and periodically corrected by {@link #refresh()}.
 */
@Log4j2
public class TenantEntitlementService {

  @Getter
  private final String moduleId;
  private final TenantEntitlementClient tenantEntitlementClient;
  private final AtomicReference<Set<String>> enabledTenants = new AtomicReference<>();

  public TenantEntitlementService(String moduleId, TenantEntitlementClient tenantEntitlementClient) {
    if (isBlank(moduleId)) {
      throw new IllegalArgumentException("Module ID must not be blank");
    }

    this.moduleId = moduleId;
    this.tenantEntitlementClient = Objects.requireNonNull(tenantEntitlementClient,
      "tenantEntitlementClient must not be null");
  }

  /**
   * Returns the cached entitled-tenant set, or {@code null} if the initial {@link #refresh()} has
   * not completed yet.
   *
   * @return entitled tenant ids, or {@code null} if not yet loaded
   */
  public Set<String> getEnabledTenants() {
    return enabledTenants.get();
  }

  /**
   * Re-fetches the full entitled-tenants set from the entitlement client and replaces the cached
   * result, correcting any drift accumulated from missed or duplicate entitlement events.
   *
   * @return a future completed with the freshly fetched entitled tenant ids
   */
  public Future<Set<String>> refresh() {
    var beforeFetch = enabledTenants.get();
    return tenantEntitlementClient.lookupTenantsByModuleId(moduleId)
      .map(result -> result == null ? Set.<String>of() : Set.copyOf(result))
      .onSuccess(refreshed -> {
        if (!enabledTenants.compareAndSet(beforeFetch, refreshed)) {
          log.debug("Skipped applying stale refresh: moduleId = {}, an entitlement event was applied "
            + "while the refresh was in flight", moduleId);
        } else {
          log.debug("Refreshed tenant entitlement cache: moduleId = {}, enabledTenants = {}", moduleId, refreshed);
        }
      })
      .onFailure(cause -> log.warn("Failed to refresh tenant entitlement cache: moduleId = {}", moduleId, cause));
  }

  /**
   * Applies an entitlement change event directly to the cached set, skipping a round trip to the
   * entitlement client. Events for other modules, or received before the cache is populated, are ignored.
   *
   * @param event entitlement change event received from the {@code entitlement} Kafka topic
   */
  public void applyEntitlementEvent(EntitlementEvent event) {
    if (!moduleId.equals(event.getModuleId())) {
      return;
    }

    var updated = enabledTenants.updateAndGet(current -> {
      if (current == null) {
        return null;
      }

      var next = new HashSet<>(current);
      if (event.getType() == EntitlementEvent.Type.REVOKE) {
        next.remove(event.getTenantName());
      } else {
        next.add(event.getTenantName());
      }
      return Set.copyOf(next);
    });

    log.info("Applied entitlement change event: moduleId = {}, tenant = {}, type = {}, enabledTenants = {}",
      moduleId, event.getTenantName(), event.getType(), updated);
  }
}
