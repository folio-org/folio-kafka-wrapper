package org.folio.kafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.okapi.common.XOkapiHeaders.REQUEST_ID;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;

/**
 * Integration tests for SASL_PLAINTEXT authentication with Kafka.
 * These tests verify that the KafkaConfig SASL configuration works correctly
 * when connecting to a Kafka broker configured with SASL_PLAIN authentication.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaSaslPlaintextIntegrationTest {

  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
  private static final String MODULE_NAME = "test_module";

  private static final String KAFKA_USER = "testuser";
  private static final String KAFKA_PASSWORD = "testpassword";
  private static final String ADMIN_USER = "admin";
  private static final String ADMIN_PASSWORD = "admin-secret";

  @Rule
  public TestName testName = new TestName();

  @Rule
  public KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0")
    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT")
    .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
    .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
    .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
    .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
      format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\" " +
        "user_%s=\"%s\" " +
        "user_%s=\"%s\";",
        ADMIN_USER, ADMIN_PASSWORD,
        ADMIN_USER, ADMIN_PASSWORD,
        KAFKA_USER, KAFKA_PASSWORD))
    .withStartupAttempts(3);

  private Vertx vertx = Vertx.vertx();
  private KafkaConfig kafkaConfig;
  private KafkaAdminClient kafkaAdminClient;
  private KafkaProducer<String, String> producer;

  @Before
  public void setUp() {
    // Set SASL configuration via system properties
    System.setProperty(KafkaConfig.KAFKA_SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    System.setProperty(KafkaConfig.KAFKA_SASL_MECHANISM_CONFIG, "PLAIN");
    System.setProperty(KafkaConfig.KAFKA_SASL_JAAS_CONFIG,
      format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\";", KAFKA_USER, KAFKA_PASSWORD));

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafka.getHost())
      .kafkaPort(kafka.getFirstMappedPort() + "")
      .build();

    // Create admin client with SASL configuration
    Map<String, String> adminConfig = new HashMap<>();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaUrl());
    adminConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    adminConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    adminConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
      format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\";", KAFKA_USER, KAFKA_PASSWORD));

    kafkaAdminClient = KafkaAdminClient.create(vertx, adminConfig);

    // Create producer with SASL configuration
    Map<String, String> producerConfig = new HashMap<>();
    producerConfig.put("bootstrap.servers", kafka.getHost() + ":" + kafka.getFirstMappedPort());
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("acks", "1");
    producerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    producerConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
      format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\";", KAFKA_USER, KAFKA_PASSWORD));

    producer = KafkaProducer.create(vertx, producerConfig);
  }

  @After
  public void tearDown(TestContext testContext) {
    // Clear system properties
    System.clearProperty(KafkaConfig.KAFKA_SECURITY_PROTOCOL_CONFIG);
    System.clearProperty(KafkaConfig.KAFKA_SASL_MECHANISM_CONFIG);
    System.clearProperty(KafkaConfig.KAFKA_SASL_JAAS_CONFIG);

    kafkaAdminClient.close()
      .onComplete(x -> producer.close())
      .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void shouldProduceAndConsumeWithSaslPlainAuth(TestContext testContext) {
    Async async = testContext.async();
    String expectedPayload = "test_payload_sasl";
    AtomicReference<String> receivedPayload = new AtomicReference<>();

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
      .createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), eventType());
    String topicName = KafkaTopicNameHelper
      .formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, eventType());

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    kafkaConsumerWrapper
      .start(record -> {
        receivedPayload.set(record.value());
        async.complete();
        return Future.succeededFuture(record.key());
      }, MODULE_NAME)
      .compose(v -> sendRecord("key1", expectedPayload, topicName))
      .onFailure(testContext::fail);

    async.await(30000);
    testContext.assertEquals(expectedPayload, receivedPayload.get());
  }

  @Test
  public void shouldVerifyKafkaConfigHasSaslProperties(TestContext testContext) {
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    Map<String, String> producerProps = kafkaConfig.getProducerProps();

    // Verify SASL properties are set in consumer config
    testContext.assertEquals("SASL_PLAINTEXT",
      consumerProps.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    testContext.assertEquals("PLAIN",
      consumerProps.get(SaslConfigs.SASL_MECHANISM));
    testContext.assertNotNull(consumerProps.get(SaslConfigs.SASL_JAAS_CONFIG));

    // Verify SASL properties are set in producer config
    testContext.assertEquals("SASL_PLAINTEXT",
      producerProps.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    testContext.assertEquals("PLAIN",
      producerProps.get(SaslConfigs.SASL_MECHANISM));
    testContext.assertNotNull(producerProps.get(SaslConfigs.SASL_JAAS_CONFIG));
  }

  private String eventType() {
    return testName.getMethodName();
  }

  private Future<Void> sendRecord(String key, String recordPayload, String topicName) {
    KafkaProducerRecord<String, String> kafkaRecord =
      KafkaProducerRecord.create(topicName, key, recordPayload);
    kafkaRecord.addHeader(TENANT, TENANT_ID);
    kafkaRecord.addHeader(REQUEST_ID, "request-id");
    kafkaRecord.addHeader(USER_ID, "user-id");

    return producer.send(kafkaRecord).mapEmpty();
  }
}
