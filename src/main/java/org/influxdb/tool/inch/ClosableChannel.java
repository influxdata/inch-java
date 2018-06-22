package org.influxdb.tool.inch;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is a implementation of Golang channel in Java
 * @author hoan.le [at] bonitoo.io
 *
 */
public class ClosableChannel {

  private final LinkedBlockingQueue<Object> queue;
  private static final Object CLOSING_SIGNAL = new Object();
  private boolean closed = false;

  public ClosableChannel() {
    queue = new LinkedBlockingQueue<>();
  }

  public ClosableChannel(int capacity) {
    queue = new LinkedBlockingQueue<>(capacity);
  }

  public Object take() throws InterruptedException {
    if (closed) {
      return CLOSING_SIGNAL;
    }
    
    Object o = queue.take();
    if (isClosingSinal(o)) {
      closed = true;
      //put back the closingSinal to make sure enqueued take() invocations can be returned
      queue.put(o);
    }
    return o;
  }
  
  public void put(Object o) throws InterruptedException {
    queue.put(o);
  }
  
  public void close() throws InterruptedException {
    queue.put(CLOSING_SIGNAL);
  }

  public static boolean isClosingSinal(Object o) {
    return o == CLOSING_SIGNAL;
  }
}
