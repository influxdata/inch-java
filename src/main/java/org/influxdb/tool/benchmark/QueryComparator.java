package org.influxdb.tool.benchmark;

import java.io.IOException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.Query;
import org.influxdb.tool.Utils;

/**
 * 
 * @author hoan.le [at] bonitoo.io
 *
 */
public class QueryComparator {

  public static void main(String[] args) throws InterruptedException, IOException {
    System.out.println("response format = " + args[0].toUpperCase());
    InfluxDB influxDB = Utils.connectToInfluxDB(ResponseFormat.valueOf(args[0].toUpperCase()));
    
    long now = System.currentTimeMillis();
    String statement = "select * from m0 where tag0='value0'";
    influxDB.query(new Query(statement, "j_stress"));
    long elapsed = System.currentTimeMillis() - now;
    System.out.println("statement = " + statement + ", elapsed = " + elapsed);

    now = System.currentTimeMillis();
    statement = "select * from m0";
    influxDB.query(new Query(statement, "j_stress"));
    elapsed = System.currentTimeMillis() - now;

    System.out.println("statement = " + statement + ", elapsed = " + elapsed);
  }
  
  
}
