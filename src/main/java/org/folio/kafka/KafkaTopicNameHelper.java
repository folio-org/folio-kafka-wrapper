package org.folio.kafka;

import static java.lang.String.join;

import org.apache.commons.lang3.StringUtils;
import org.folio.kafka.exception.KafkaConfigValidationException;

public final class KafkaTopicNameHelper {

  private static final String DEFAULT_NAMESPACE = "Default";
  private static final String TENANT_COLLECTION_TOPICS_ENV_VAR_NAME = "KAFKA_PRODUCER_TENANT_COLLECTION";
  private static final String TENANT_COLLECTION_MATCH_REGEX = "[A-Z][A-Z0-9]{0,30}";
  private static boolean tenantCollectionTopicsEnabled;
  private static String tenantCollectionTopicQualifier;

  static {
    tenantCollectionTopicQualifier = System.getenv(TENANT_COLLECTION_TOPICS_ENV_VAR_NAME);
    setTenantCollectionTopicsQualifier(tenantCollectionTopicQualifier);
  }

  private KafkaTopicNameHelper() {
    super();
  }

  public static boolean isTenantCollectionTopicsEnabled() {
    return tenantCollectionTopicsEnabled;
  }

  public static String formatTopicName(String env, String nameSpace, String tenant, String eventType) {
    String tenantId = tenant;
    if (tenantCollectionTopicsEnabled) {
      tenantId = tenantCollectionTopicQualifier;
    }
    return join(".", env, nameSpace, tenantId, eventType);
  }

  public static String formatTopicName(String env, String tenant, String eventType) {
    String tenantId = tenant;
    if (tenantCollectionTopicsEnabled) {
      tenantId = tenantCollectionTopicQualifier;
    }
    return join(".", env, tenantId, eventType);
  }

  public static String getEventTypeFromTopicName(String topic) {
    int eventTypeIndex = StringUtils.isBlank(topic) ? -1 : topic.lastIndexOf('.') + 1;
    if (eventTypeIndex > 0) {
      return topic.substring(eventTypeIndex);
    } else {
      throw new KafkaConfigValidationException(
        "Invalid topic name: " + topic + " - expected env.nameSpace.tenant.eventType");
    }
  }

  public static String formatGroupName(String eventType, String moduleName) {
    return join(".", eventType, moduleName);
  }

  public static String formatSubscriptionPattern(String env, String nameSpace, String eventType) {
    return join("\\.", env, nameSpace, "\\w{1,}", eventType);
  }

  public static String getDefaultNameSpace() {
    return DEFAULT_NAMESPACE;
  }

  public static SubscriptionDefinition createSubscriptionDefinition(String env, String nameSpace, String eventType) {
    return SubscriptionDefinition.builder()
      .eventType(eventType)
      .subscriptionPattern(formatSubscriptionPattern(env, nameSpace, eventType))
      .build();
  }

  static void setTenantCollectionTopicsQualifier(String value) {
    tenantCollectionTopicQualifier = value;
    tenantCollectionTopicsEnabled = !StringUtils.isEmpty(tenantCollectionTopicQualifier);

    if (tenantCollectionTopicsEnabled && !tenantCollectionTopicQualifier.matches(TENANT_COLLECTION_MATCH_REGEX)) {
      throw new KafkaConfigValidationException(
        String.format("%s environment variable does not match %s",
          TENANT_COLLECTION_TOPICS_ENV_VAR_NAME,
          TENANT_COLLECTION_MATCH_REGEX));
    }
  }
}
