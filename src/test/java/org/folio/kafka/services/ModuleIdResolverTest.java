package org.folio.kafka.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ModuleIdResolverTest {

  @AfterEach
  void tearDown() {
    System.clearProperty(ModuleIdResolver.MODULE_NAME);
    System.clearProperty(ModuleIdResolver.MODULE_VERSION);
  }

  @Test
  void resolve_shouldCombineFallbackNameAndVersion_whenNoEnvVarsSet() {
    assertEquals("mod-foo-1.0.0", ModuleIdResolver.resolve("mod-foo", "1.0.0"));
  }

  @Test
  void resolveNoArgs_shouldUseEnvVarsAlone_whenBothSet() {
    System.setProperty(ModuleIdResolver.MODULE_NAME, "mod-foo");
    System.setProperty(ModuleIdResolver.MODULE_VERSION, "1.0.0");

    assertEquals("mod-foo-1.0.0", ModuleIdResolver.resolve());
  }

  @Test
  void resolveNoArgs_shouldThrow_whenEnvVarsNotSet() {
    assertThrows(IllegalArgumentException.class, ModuleIdResolver::resolve);
  }

  @Test
  void resolveNameOnly_shouldCombineFallbackNameWithVersionEnvVar() {
    System.setProperty(ModuleIdResolver.MODULE_VERSION, "1.0.0");

    assertEquals("mod-foo-1.0.0", ModuleIdResolver.resolve("mod-foo"));
  }

  @Test
  void resolveNameOnly_shouldThrow_whenVersionEnvVarNotSet() {
    assertThrows(IllegalArgumentException.class, () -> ModuleIdResolver.resolve("mod-foo"));
  }

  @Test
  void resolve_shouldPreferNameEnvVar_overFallback() {
    System.setProperty(ModuleIdResolver.MODULE_NAME, "mod-bar");

    assertEquals("mod-bar-1.0.0", ModuleIdResolver.resolve("mod-foo", "1.0.0"));
  }

  @Test
  void resolve_shouldPreferVersionEnvVar_overFallback() {
    System.setProperty(ModuleIdResolver.MODULE_VERSION, "2.0.0");

    assertEquals("mod-foo-2.0.0", ModuleIdResolver.resolve("mod-foo", "1.0.0"));
  }

  @Test
  void resolve_shouldUseFallbackName_whenNameEnvVarNotSet() {
    System.setProperty(ModuleIdResolver.MODULE_VERSION, "1.0.0");

    assertEquals("mod-foo-1.0.0", ModuleIdResolver.resolve("mod-foo", null));
  }

  @Test
  void resolve_shouldUseFallbackVersion_whenVersionEnvVarNotSet() {
    System.setProperty(ModuleIdResolver.MODULE_NAME, "mod-foo");

    assertEquals("mod-foo-1.0.0", ModuleIdResolver.resolve(null, "1.0.0"));
  }

  @Test
  void resolve_shouldThrow_whenNameMissingEverywhere() {
    assertThrows(IllegalArgumentException.class, () -> ModuleIdResolver.resolve(null, "1.0.0"));
  }

  @Test
  void resolve_shouldThrow_whenVersionMissingEverywhere() {
    assertThrows(IllegalArgumentException.class, () -> ModuleIdResolver.resolve("mod-foo", null));
  }

  @Test
  void resolve_shouldThrow_whenEnvVarValueIsBlank() {
    System.setProperty(ModuleIdResolver.MODULE_VERSION, "  ");

    assertThrows(IllegalArgumentException.class, () -> ModuleIdResolver.resolve("mod-foo", "1.0.0"));
  }
}
