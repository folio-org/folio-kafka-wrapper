package org.folio.kafka.filtering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TenantEntitlementServiceTest {

  private static final String MODULE_ID = "mod-foo-1.0.0";

  @Mock
  private TenantEntitlementClient client;

  private TenantEntitlementService service;

  @BeforeEach
  void setUp() {
    service = new TenantEntitlementService(MODULE_ID, client);
  }

  @Test
  void constructor_shouldRejectBlankModuleId() {
    assertThrows(IllegalArgumentException.class, () -> new TenantEntitlementService(" ", client));
  }

  @Test
  void getEnabledTenants_shouldReturnNull_whenNotYetRefreshed() {
    assertNull(service.getEnabledTenants());
    verifyNoMoreInteractions(client);
  }

  @Test
  void refresh_shouldPopulateCacheFromClient() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.succeededFuture(Set.of("diku", "college")));

    service.refresh();

    assertEquals(Set.of("diku", "college"), service.getEnabledTenants());
  }

  @Test
  void refresh_shouldCacheEmptySet_whenClientReturnsNull() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.succeededFuture(null));

    service.refresh();

    assertEquals(Set.of(), service.getEnabledTenants());
  }

  @Test
  void refresh_shouldLeaveCacheUnset_whenClientFails() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.failedFuture("boom"));

    service.refresh();

    assertNull(service.getEnabledTenants());
  }

  @Test
  void applyEntitlementEvent_shouldBeIgnored_whenCacheNotYetPopulated() {
    var event = entitleEvent("diku");

    service.applyEntitlementEvent(event);

    assertNull(service.getEnabledTenants());
  }

  @Test
  void applyEntitlementEvent_shouldBeIgnored_whenForAnotherModule() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.succeededFuture(Set.of("diku")));
    service.refresh();

    var event = new EntitlementEvent();
    event.setModuleId("mod-bar-1.0.0");
    event.setTenantName("college");
    event.setType(EntitlementEvent.Type.ENTITLE);
    service.applyEntitlementEvent(event);

    assertEquals(Set.of("diku"), service.getEnabledTenants());
  }

  @Test
  void applyEntitlementEvent_shouldAddTenant_onEntitle() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.succeededFuture(Set.of("diku")));
    service.refresh();

    service.applyEntitlementEvent(entitleEvent("college"));

    assertEquals(Set.of("diku", "college"), service.getEnabledTenants());
  }

  @Test
  void applyEntitlementEvent_shouldRemoveTenant_onRevoke() {
    when(client.lookupTenantsByModuleId(MODULE_ID)).thenReturn(Future.succeededFuture(Set.of("diku", "college")));
    service.refresh();

    var event = new EntitlementEvent();
    event.setModuleId(MODULE_ID);
    event.setTenantName("college");
    event.setType(EntitlementEvent.Type.REVOKE);
    service.applyEntitlementEvent(event);

    assertEquals(Set.of("diku"), service.getEnabledTenants());
  }

  private static EntitlementEvent entitleEvent(String tenant) {
    var event = new EntitlementEvent();
    event.setModuleId(MODULE_ID);
    event.setTenantName(tenant);
    event.setType(EntitlementEvent.Type.ENTITLE);
    return event;
  }
}
