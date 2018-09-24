package org.influxdb.tool.jmh;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.tool.Utils;

import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for async query simple/multiple property query, json/msgpack comparison.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AsyncQueryBenchmark {

  private static final String singlePropertyQuery = "SELECT StringLoggedProperty FROM ValueStreamServicesTests_121234 " +
      "WHERE valuestreamname = 'ValueStreamServicesTests.ValueStream_872672' ORDER BY time %s";

  private static final String multiplePropertyQuery = "SELECT IntegerLoggedProperty, LongLoggedProperty, NumberLoggedProperty, BooleanLoggedProperty, StringLoggedProperty " +
      " FROM ValueStreamServicesTests_121234 " +
      " WHERE valuestreamname = 'ValueStreamServicesTests.ValueStream_872672' ORDER BY time %s";

  @Param({"AsyncQueryBench1"})
  public String dbName;

  @Param({"singleProperty", "multiProperty"})
  public String queryType;

  @Param({"100"})
  public int chunkSize;

  @Param({"100", "1000", "10000"})
  public int limit;

  @Param({"ASC", "DESC"})
  public String order;

  @Param({"JSON", "MSGPACK"})
  public String responseType;

  private InfluxDB influxDB;
  private Dispatcher dispatcher;

  @Setup
  public void setup() throws Exception {

    dispatcher = new Dispatcher();
    OkHttpClient.Builder client = new OkHttpClient.Builder().dispatcher(dispatcher);
     influxDB = Utils.connectToInfluxDB(client, null, InfluxDB.ResponseFormat.valueOf(responseType));
  }

  @TearDown
  public void tearDown() {
    System.out.println("Closing influx db.");
    influxDB.close();
    dispatcher.executorService().shutdownNow();
  }


  @Benchmark
  public void query(Blackhole blackhole) throws Exception {

    String statement = null;
    switch (queryType) {
      case "singleProperty":
        statement = singlePropertyQuery;
        break;
      case "multiProperty":
        statement = multiplePropertyQuery;
        break;
    }

    Query query = new Query(String.format(statement + " LIMIT " + limit, order), dbName);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    influxDB.query(query, chunkSize,
        // process query result
        (cancellable, queryResult) -> blackhole.consume(queryResult.toString()),
        // onComplete
        countDownLatch::countDown);

    countDownLatch.await(5, TimeUnit.SECONDS);

/*
//old API
    LongAdder adder = new LongAdder();
    boolean isMessagePack = InfluxDB.ResponseFormat.MSGPACK.equals(format);
    influxDB.query(query, chunkSize,
        new java.util.function.Consumer<QueryResult>() {
          @Override
          public void accept(QueryResult queryResult) {

            // JSON END
            if (queryResult.getError() != null && queryResult.getError().equals("DONE")) {
              countDownLatch.countDown();
            } else {
              List<List<Object>> values = queryResult.getResults().get(0).getSeries().get(0).getValues();
              adder.add(values.size());

              // MessagePack end
              if (isMessagePack && adder.longValue() == limit) {
                countDownLatch.countDown();
              }
            }
          }
        });

 */

  }
}
