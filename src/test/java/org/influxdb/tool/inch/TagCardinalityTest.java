package org.influxdb.tool.inch;

import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class TagCardinalityTest {
  
  private static InfluxDB influxDB;
  private static String host = "http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true);
  private static String user = "root";
  private static String password = "root";
  private static final String CLI = "-host " + host + " -user " + user + " -password " + password + " "; 

  @BeforeAll
  public static void setUp() {
    influxDB = InfluxDBFactory.connect(host, user, password);
    influxDB.setDatabase("j_stress");
  }
  
  @BeforeEach
  public void setUpEach() {
    dropStressDB();
  }
  
  /**
   * Test writing a dataset with a tag set of 10k element = 50 * 10 *20 = 10 * 10 *10 *10
   * @throws InterruptedException
   */
  @Test
  public void testTagSetOf10k() throws InterruptedException {
    
    String cli = CLI + "-t 50,10,20"; 
    Inch.main(cli.split(" "));
    
    QueryResult result = influxDB.query(new Query("select count(*) from m0", "j_stress"));
    double d = Double.parseDouble(result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString());
    assertEquals(1000000, d);
    
    /*dropStressDB();
    
    cli = CLI + "-t 10,10,10,10";
    Inch.main(cli.split(" "));
    
    result = influxDB.query(new Query("select count(*) from m0", "j_stress"));
    d = Double.parseDouble(result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString());
    assertEquals(1000000, d);*/
  }
  
  /**
   * Test writing a dataset with a tag set of 20k element = 50 * 20 *20 = 10 * 10 * 20 *10
   * @throws InterruptedException
   */
  //@Test
  public void testTagSetOf20kInParallel() throws InterruptedException {
    
    String cli = CLI + "-t 50,20,20 -c 2";//2 threads 
    Inch.main(cli.split(" "));
    
    QueryResult result = influxDB.query(new Query("select count(*) from m0", "j_stress"));
    double d = Double.parseDouble(result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString());
    assertEquals(2000000, d);
    
    dropStressDB();
    
    cli = CLI + "-t 10,10,20,10 -c 3";//3 threads
    Inch.main(cli.split(" "));
    
    result = influxDB.query(new Query("select count(*) from m0", "j_stress"));
    d = Double.parseDouble(result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString());
    assertEquals(2000000, d);
  }
  
  @AfterAll
  public static void tearDown() {
    dropStressDB();
    influxDB.close();
  }
  
  private static void dropStressDB() {
    influxDB.query(new Query("drop database j_stress", null));    
  }
}
