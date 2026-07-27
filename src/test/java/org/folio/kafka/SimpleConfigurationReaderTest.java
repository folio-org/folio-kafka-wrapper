package org.folio.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class SimpleConfigurationReaderTest {

  @Test
  void shouldReadValueFromSystemProperty() {
    String expectedValue = "testValue";
    System.setProperty("test.props", expectedValue);
    String actualValue = SimpleConfigurationReader.getValue("test.props", null);
    assertEquals(expectedValue, actualValue);
  }

  @Test
  void shouldReturnDefaultValueIfNoSysPropertyOrEnvVariable() {
    String defaultValue = "testValue";
    String actualValue = SimpleConfigurationReader.getValue("test.props", defaultValue);
    assertEquals(defaultValue, actualValue);
  }

  @Test
  void shouldReadValueFromSystemPropertyBySecondKey() {
    String expectedValue = "testValue";
    System.setProperty("test.props2", expectedValue);
    String actualValue = SimpleConfigurationReader.getValue(List.of("test.props1", "test.props2"), null);
    assertEquals(expectedValue, actualValue);
  }

  @Test
  void shouldReturnDefaultValueIfNoValueForSpecifiedKeys() {
    String defaultValue = "testValue";
    String actualValue = SimpleConfigurationReader.getValue(List.of("test.props1", "test.props2"), defaultValue);
    assertEquals(defaultValue, actualValue);
  }
}
