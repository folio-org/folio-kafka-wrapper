package org.folio.kafka;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the simplest solution to track the load
 */


public class GlobalLoadSensor {
  private AtomicInteger index;

  public GlobalLoadSensor() {
    index = new AtomicInteger();
  }

  protected GlobalLoadSensor(int size) {
    index = new AtomicInteger(size);
  }

  public int increment() {
    return index.incrementAndGet();
  }

  public int decrement() {
    return index.decrementAndGet();
  }

  public int current() {
    return index.get();
  }


  public static class GlobalLoadSensorNA extends GlobalLoadSensor {
    @Override
    public int increment() {
      return -1;
    }

    @Override
    public int decrement() {
      return -1;
    }

    @Override
    public int current() {
      return -1;
    }
  }

}
