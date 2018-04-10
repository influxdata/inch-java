package org.influxdb.tool.inch;

import java.util.concurrent.LinkedBlockingQueue;

public class ClosableBlockingQueue extends LinkedBlockingQueue<Object> {

  private static final long serialVersionUID = -6963113886199394071L;
  private static final Object closingSignal = new Object();
  private boolean closed = false;

  public ClosableBlockingQueue() {
    super();
  }

  public ClosableBlockingQueue(int capacity) {
    super(capacity);
  }

  @Override
  public Object take() throws InterruptedException {
    if (closed) {
      return closingSignal;
    }
    
    Object o = super.take();
    if (isClosingSinal(o)) {
      closed = true;
      //put back the closingSinal to make sure enqueued take() invocations can be returned
      put(o);
    }
    return o;
  }
  
  public void close() throws InterruptedException {
    put(closingSignal);
  }

  public static boolean isClosingSinal(Object o) {
    return o == closingSignal;
  }
}
