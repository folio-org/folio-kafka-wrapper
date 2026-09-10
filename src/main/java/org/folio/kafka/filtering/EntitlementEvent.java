package org.folio.kafka.filtering;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Entitlement change event consumed from the {@code entitlement} Kafka topic, published by
 * mgr-tenant-entitlements whenever a tenant is entitled to or revoked from a module. Mirrors the
 * shape produced by mgr-tenant-entitlements / consumed by folio-module-sidecar's own
 * {@code TenantEntitlementEvent}, minus the {@code tenantId} field this filter has no use for.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntitlementEvent {

  @JsonProperty("moduleId")
  private String moduleId;

  @JsonProperty("tenantName")
  private String tenantName;

  @JsonProperty("type")
  private Type type;

  public enum Type {
    ENTITLE,
    UPGRADE,
    REVOKE
  }
}
