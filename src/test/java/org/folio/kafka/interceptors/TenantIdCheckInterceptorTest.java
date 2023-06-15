package org.folio.kafka.interceptors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.List;

public class TenantIdCheckInterceptorTest {

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
  }

  @Test
  public void onSend() {
    // get logger belonging to the class and add test appender
    Logger logger = LogManager.getLogger(TenantIdCheckInterceptor.class.getName());
    TestAppender appender = new TestAppender("TestAppender", null);
    ((LoggerContext) LogManager.getContext(false)).getConfiguration().addAppender(appender);
    ((org.apache.logging.log4j.core.Logger) logger).addAppender(appender);
    appender.start();
    String topicName = "topicName";
    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0, "key-0", "value-0");
    TenantIdCheckInterceptor tenantIdCheckInterceptor = new TenantIdCheckInterceptor();

    tenantIdCheckInterceptor.onSend(record);

    Assert.assertEquals(1, appender.getMessages().size());
    Assert.assertEquals(MessageFormatter.format(TenantIdCheckInterceptor.TENANT_ID_ERROR_MESSAGE, topicName).getMessage()
      ,appender.getMessages().get(0));
  }
}
