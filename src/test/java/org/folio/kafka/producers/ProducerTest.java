package org.folio.kafka.producers;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.folio.kafka.KafkaConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

@RunWith(VertxUnitRunner.class)
public class ProducerTest {
  private Vertx vertx;
  private EmbeddedKafkaCluster kafkaCluster;
  private KafkaConfig kafkaConfig;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    kafkaCluster = provisionWith(EmbeddedKafkaClusterConfig.defaultClusterConfig());
    kafkaCluster.start();
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .build();
  }

  @After
  public void tearDown(TestContext ctx) {
    kafkaCluster.close();
    vertx.close().onComplete(ctx.asyncAssertSuccess());
  }

  @Test
  public void testInterceptor(TestContext testCtx) throws Exception {
    String topicName = "testInterceptor";
    String producerName = "testProducer";

    KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());

    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, "key", "value");

    Async async = testCtx.async();

    vertx.runOnContext(v1 -> {
      producer.write(record)
        .onComplete(ar -> {
          testCtx.assertTrue(ar.succeeded());
          async.complete();
        });
    });
  }
}
