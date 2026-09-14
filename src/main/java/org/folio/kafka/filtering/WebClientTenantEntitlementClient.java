package org.folio.kafka.filtering;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link TenantEntitlementClient} that calls {@code GET {okapiUrl}/entitlements/modules/{moduleId}}.
 *
 * <p>For a module's own id, this path is answered by that module's colocated folio-module-sidecar
 * from its in-memory, Kafka-fed entitlement cache rather than by mgr-tenant-entitlements' database,
 * so this is a cheap, local call, not a cross-service/DB round trip.
 */
public class WebClientTenantEntitlementClient implements TenantEntitlementClient {

  @SuppressWarnings("java:S1075")
  private static final String PATH_TEMPLATE = "/entitlements/modules/%s";

  private final WebClient webClient;
  private final String okapiUrl;

  /**
   * Bounds a stuck lookup (connection accepted but no response) so it fails instead of hanging
   * forever, letting the caller's retry-with-backoff logic run. Configured via
   * {@link TenantEntitlementFilterProperties#ENTITLEMENT_LOOKUP_TIMEOUT_SECONDS}.
   */
  private final long requestTimeoutMs;

  public WebClientTenantEntitlementClient(Vertx vertx, String okapiUrl) {
    this(vertx, okapiUrl, TenantEntitlementFilterProperties.entitlementLookupTimeoutMs());
  }

  WebClientTenantEntitlementClient(Vertx vertx, String okapiUrl, long requestTimeoutMs) {
    this.webClient = WebClient.create(vertx);
    this.okapiUrl = okapiUrl;
    this.requestTimeoutMs = requestTimeoutMs;
  }

  @Override
  public Future<Set<String>> lookupTenantsByModuleId(String moduleId) {
    var url = okapiUrl + PATH_TEMPLATE.formatted(moduleId);
    return webClient.getAbs(url)
      .timeout(requestTimeoutMs)
      .send()
      .compose(response -> {
        if (response.statusCode() != 200) {
          return Future.failedFuture("Unexpected status code %d from entitlements lookup for module '%s': %s"
            .formatted(response.statusCode(), moduleId, response.bodyAsString()));
        }
        var tenants = response.bodyAsJsonArray().stream()
          .map(String.class::cast)
          .collect(Collectors.toUnmodifiableSet());
        return Future.succeededFuture(tenants);
      });
  }
}
