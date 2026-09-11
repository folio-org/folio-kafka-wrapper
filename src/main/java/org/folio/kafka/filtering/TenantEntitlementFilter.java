package org.folio.kafka.filtering;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;
import org.folio.okapi.common.XOkapiHeaders;

/**
 * Decides whether a Kafka record should be skipped because its tenant is not currently entitled
 * to the current module.
 */
@Log4j2
public class TenantEntitlementFilter {

  /**
   * How long a single record waits for the cache to load before giving up and accepting it unfiltered.
   */
  private static final long CACHE_WAIT_RETRY_INTERVAL_MS = 200;
  private static final long CACHE_WAIT_TIMEOUT_MS = 10000;

  private final String moduleId;
  private final TenantEntitlementService tenantEntitlementService;
  private final DisabledTenantStrategy tenantDisabledStrategy;
  private final DisabledTenantStrategy allTenantsDisabledStrategy;
  private final Runnable initialLoadTrigger;
  private final Vertx vertx;
  private final AtomicBoolean initialLoadTriggered = new AtomicBoolean(false);

  /**
   * Creates a tenant entitlement filter.
   *
   * @param initialLoadTrigger kicks off the initial entitlement fetch; invoked at most once, lazily,
   *     on the first record seen while the cache is unpopulated
   */
  public TenantEntitlementFilter(String moduleId, TenantEntitlementService tenantEntitlementService,
    DisabledTenantStrategy tenantDisabledStrategy, DisabledTenantStrategy allTenantsDisabledStrategy,
    Vertx vertx, Runnable initialLoadTrigger) {
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
    this.vertx = Objects.requireNonNull(vertx, "vertx must not be null");
    this.initialLoadTrigger = Objects.requireNonNull(initialLoadTrigger, "initialLoadTrigger must not be null");
  }

  /**
   * Returns whether the given record should be skipped rather than handed to the business handler.
   *
   * <p>If the entitlement cache isn't populated yet, this retries briefly (see {@link #CACHE_WAIT_TIMEOUT_MS})
   * instead of deciding blind. Once that bounded wait elapses, it falls back to accepting
   * the record unfiltered.
   *
   * @param consumerRecord the record to test
   * @return a future resolving to {@code true} if the record should be skipped, or failing with a
   *     {@link TenantIsDisabledException}/{@link TenantsAreDisabledException} for the {@code FAIL} strategy
   */
  public Future<Boolean> shouldSkip(KafkaConsumerRecord<?, ?> consumerRecord) {
    return shouldSkip(consumerRecord, 0);
  }

  private Future<Boolean> shouldSkip(KafkaConsumerRecord<?, ?> consumerRecord, long waitedMs) {
    var tenant = resolveTenant(consumerRecord);
    if (tenant == null) {
      return Future.succeededFuture(false);
    }

    var enabledTenants = tenantEntitlementService.getEnabledTenants();
    if (enabledTenants == null) {
      triggerInitialLoadIfNeeded();
      return waitForCacheOrAccept(consumerRecord, waitedMs, tenant);
    }

    try {
      var skip = filterByEnabledTenants(enabledTenants, tenant);
      log.debug("Message for tenant is {}: moduleId = {}, tenant = {}", skip ? "skipped" : "accepted",
        moduleId, tenant);
      return Future.succeededFuture(skip);
    } catch (RuntimeException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> waitForCacheOrAccept(KafkaConsumerRecord<?, ?> consumerRecord, long waitedMs,
    String tenant) {
    if (waitedMs >= CACHE_WAIT_TIMEOUT_MS) {
      log.warn("Tenant entitlement cache still not populated after {} ms: moduleId = {}, tenant = {}. "
        + "Accepting record.", waitedMs, moduleId, tenant);
      return Future.succeededFuture(false);
    }

    Promise<Boolean> promise = Promise.promise();
    vertx.setTimer(CACHE_WAIT_RETRY_INTERVAL_MS,
      id -> shouldSkip(consumerRecord, waitedMs + CACHE_WAIT_RETRY_INTERVAL_MS).onComplete(promise));
    return promise.future();
  }

  private void triggerInitialLoadIfNeeded() {
    if (initialLoadTriggered.compareAndSet(false, true)) {
      initialLoadTrigger.run();
    }
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
