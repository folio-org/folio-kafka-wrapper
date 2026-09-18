package org.folio.kafka.filtering;

import io.vertx.core.Future;
import java.util.Set;

/**
 * Resolves tenants entitled to a given module.
 */
public interface TenantEntitlementClient {

  /**
   * Returns tenants entitled to the provided module id.
   *
   * @param moduleId module id, for example {@code mod-foo-1.0.0}
   * @return entitled tenant ids
   */
  Future<Set<String>> lookupTenantsByModuleId(String moduleId);
}
