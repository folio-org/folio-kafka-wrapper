package org.folio.kafka.services;

import org.apache.commons.lang3.StringUtils;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.SimpleConfigurationReader;

/**
 * Resolves a module's true entitlements id (see {@link KafkaConsumerWrapper#start(AsyncRecordHandler,
 * String, String)}) from the {@code MODULE_NAME}/{@code MODULE_VERSION} env vars folio-module-sidecar
 * already requires every module to set. Env vars always win when present, since the sidecar only answers
 * entitlement lookups for its own module id, computed from those same env vars.
 */
public final class ModuleIdResolver {

  public static final String MODULE_NAME = "MODULE_NAME";
  public static final String MODULE_VERSION = "MODULE_VERSION";

  private ModuleIdResolver() {
  }

  /**
   * Resolves the module id from {@code MODULE_NAME}/{@code MODULE_VERSION} alone.
   */
  public static String resolve() {
    return resolve(null, null);
  }

  /**
   * Resolves the module id, falling back to {@code fallbackModuleName} if {@code MODULE_NAME} isn't set.
   */
  public static String resolve(String fallbackModuleName) {
    return resolve(fallbackModuleName, null);
  }

  /**
   * Resolves the module id as {@code <name>-<version>}, using {@code fallbackModuleName}/
   * {@code fallbackModuleVersion} for whichever env var isn't set.
   *
   * @throws IllegalArgumentException if the name or version is blank in both the env var and the fallback
   */
  public static String resolve(String fallbackModuleName, String fallbackModuleVersion) {
    var name = SimpleConfigurationReader.getValue(MODULE_NAME, fallbackModuleName);
    var version = SimpleConfigurationReader.getValue(MODULE_VERSION, fallbackModuleVersion);

    if (StringUtils.isBlank(name) || StringUtils.isBlank(version)) {
      throw new IllegalArgumentException(
        "Unable to resolve module id: name = '%s', version = '%s'".formatted(name, version));
    }
    return name + "-" + version;
  }
}
