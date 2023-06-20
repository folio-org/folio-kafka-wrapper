package org.folio.kafka.interceptors;

import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.List;

public class TenantIdCheckInterceptorTest {

  private static TestAppender appender;

  @BeforeClass
  public static void classSetup() {
    Logger logger = LogManager.getLogger(TenantIdCheckInterceptor.class.getName());
    appender = new TestAppender("TestAppender", null);
    ((LoggerContext) LogManager.getContext(false)).getConfiguration().addAppender(appender);
    ((org.apache.logging.log4j.core.Logger) logger).addAppender(appender);
    appender.start();
  }

  @Test
  public void onSend() {
    String topicName = "topicName";
    String key = "key-0";
    String value = "value-0";
    //
    // create a record with no headers, message should be logged
    ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topicName, 0, key, value);
    TenantIdCheckInterceptor tenantIdCheckInterceptor = new TenantIdCheckInterceptor();

    tenantIdCheckInterceptor.onSend(kafkaRecord);

    Assert.assertEquals(1, appender.getMessages().size());
    Assert.assertEquals(MessageFormatter.format(TenantIdCheckInterceptor.TENANT_ID_ERROR_MESSAGE, topicName).getMessage()
      , appender.getMessages().get(0));

    // clear logged messages
    appender.clear();

    //
    // create record with necessary headers, message should not be logged
    KafkaProducerRecord<String, String> vertxRecord = new KafkaProducerRecordBuilder<String, String>("tenant")
      .topic(topicName)
      .key(key)
      .value(value)
      .build();

    tenantIdCheckInterceptor.onSend(vertxRecord.record());

    Assert.assertEquals(0, appender.getMessages().size());
  }

  private static class TestAppender extends AbstractAppender {

    private final List<String> messages = new ArrayList<>();

    TestAppender(String name, org.apache.logging.log4j.core.Filter filter) {
      super(name, filter, null);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    List<String> getMessages() {
      return messages;
    }

    void clear() {
      messages.clear();
    }
  }

}
