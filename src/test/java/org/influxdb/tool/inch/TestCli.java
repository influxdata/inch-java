package org.influxdb.tool.inch;

import java.io.IOException;

/**
 * 
 * @author hoan.le [at] bonitoo.io
 *
 */
public class TestCli {

  public static void main(String[] args) throws InterruptedException, IOException {
    System.out.println("press Enter to start");
    System.in.read();

    Inch.main(args);

    System.out.println("press Enter to exit");
    System.in.read();
    System.in.read();
  }

}
