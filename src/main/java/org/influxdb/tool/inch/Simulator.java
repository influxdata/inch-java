package org.influxdb.tool.inch;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

/**
 * 
 * @author hoan.le [at] bonitoo.io
 *
 */
public class Simulator {

  String host = "http://localhost:8086";
  String user;
  String password;
  String consistency = "any";
  ConsistencyLevel consistencyLevel;
  int concurrency = 1;
  long measurements = 1; // Number of measurements
  List<Long> tags = Arrays.asList(10L, 10L, 10L); // tag cardinalities
  long pointsPerSeries = 100;
  long fieldsPerPoint = 1;
  long batchSize = 5000;
  AtomicLong writtenN = new AtomicLong();
  String database = "j_stress";
  String retentionPolicy = "autogen";
  String shardDuration = "7d"; // Set a custom shard duration.
  Long startTime; // Set a custom start time.
  long now;
  long baseTime;
  long timePerSeries;
  long timeSpan = 3600000000000L; // The length of time to span writes over.
  long delay; // A delay inserted in between writes.
  AtomicLong latestValues = new AtomicLong();

  boolean verbose = false;
  String reportHost;
  String reportUser;
  String reportPassword;
  Map<String, String> ReportTags;
  boolean dryRun = false;
  long MaxErrors;
  boolean lowLevelApi = false;
  boolean sequentialBatchGen = false;

  InfluxDB influxDB;
  private void setup() {
    //OkHttpClient.Builder builder = new OkHttpClient.Builder();
    //builder.connectionSpecs(Arrays.asList(ConnectionSpec.CLEARTEXT));
    if (user == null || password == null) {
      influxDB = InfluxDBFactory.connect(host);
    } else {
      influxDB = InfluxDBFactory.connect(host, user, password);
    }
    //influxDB.enableGzip();
    influxDB.query(new Query("create database " + database + " with duration " + shardDuration, null));
    
    consistencyLevel = ConsistencyLevel.valueOf(consistency.toUpperCase());
  }

  private void sendBatch(InfluxDB influxDB, BatchPoints batchPoints) {
    if (dryRun) {
      return;
    }

    influxDB.write(batchPoints);
  }
  
  private void sendBatch(InfluxDB influxDB, String lineProtocol) {
    if (dryRun) {
      return;
    }

    influxDB.write(database, retentionPolicy, consistencyLevel, lineProtocol);
  }
  
  private void sendPoints(InfluxDB influxDB, BatchPoints batchPoints) {
    if (dryRun) {
      return;
    }

    for (Point point : batchPoints.getPoints()) {
      influxDB.write(point);      
    }
  }
  
  

  private void validate() {

  }

  void run(InchContext context) throws InterruptedException {
    validate();

    // Print settings.
    System.out.println("Host: " + host);
    System.out.println("Concurrency: " + concurrency);
    System.out.println("Measurements: " + measurements);
    System.out.println("Tag cardinalities: " + tags);
    System.out.println("points per series: " + pointsPerSeries);
    System.out.println("Total series: " + seriesN());
    System.out.println("Total points: " + pointN());
    System.out.println("Total fields per point: " + fieldsPerPoint);
    System.out.println("Batch Size: " + batchSize);
    System.out.println("Database: " + database + " (Shard duration: " + shardDuration + ")");
    System.out.println("Write Consistency: " + consistency);

    setup();

    ClosableChannel queue = generateBatches();
    
    now = System.currentTimeMillis();
    baseTime = now * 1000000L;

    if (startTime == null) {
      startTime = baseTime;
    }

    if (timeSpan != 0) {
      long absTimeSpan = Math.abs(timeSpan);
      timePerSeries = absTimeSpan / pointN();

      if (timeSpan < 0) {
        startTime += timeSpan;
      }
    }

    System.out.println("Start time: " + startTime);
    if (timeSpan < 0) {
      System.out.println("Approx End time: " + baseTime);
    } else if (timeSpan > 0) {
      System.out.println("Approx End time: " + (startTime + timeSpan));
    } else {
      System.out.println("Time span: off");
    }

    CountDownLatch doneSignal = new CountDownLatch(concurrency);
    for (int i = 0; i < concurrency; i++) {
      new Thread(new Runnable() {
        public void run() {
          try {
            runClient(context, queue);
          } finally {
            doneSignal.countDown();
          }
        }
      }).start();
    }

    Thread monitorThread = new Thread(new Runnable() {
      public void run() {
        runMonitor(context);
      }
    });

    monitorThread.start();
    doneSignal.await();
    context.put("Done", true);
    monitorThread.join();

    // Report stats.
    long elapsed = System.currentTimeMillis() - now;
    System.out.println("Total time: " + (elapsed / 1000) + " seconds");
    influxDB.close();

  }

  private void runClient(InchContext context, ClosableChannel queue) {
    /*influxDB.setDatabase(database);
    influxDB.setConsistency(ConsistencyLevel.valueOf(consistency.toUpperCase()));
    BatchOptions options = BatchOptions.DEFAULTS.actions((int) batchSize).bufferLimit(1000000);
    influxDB.enableBatch(options);*/

    try {
      while (true) {
        if ((Boolean) context.get("Done")) {
          return;
        }
        Object o = queue.take();
        if (ClosableChannel.isClosingSinal(o)) {
          return;
        } else if (!lowLevelApi ){
          sendBatch(influxDB, (BatchPoints) o);
        } else {
          sendBatch(influxDB, (String) o);
        }

        writtenN.addAndGet(batchSize);
        // Update current throughput
        latestValues.addAndGet(batchSize);
      }

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // runMonitor periodically prints the current status.
  private void runMonitor(InchContext context) {
    while (true) {
      long last = System.currentTimeMillis();
      long d = (System.currentTimeMillis() - last) / 1000;
      if (d == 0) {
        d = 1;
      }
      long throughput = latestValues.get() / d;
      latestValues.set(0);
      printMonitorStats(throughput);
      if (reportHost != null && !reportHost.isEmpty()) {
        sendMonitorStats(true, throughput);
      }
      if ((Boolean) context.get("Done")) {
        return;
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  private void printMonitorStats(long latestThroughput) {
    long writtenN = writtenN();
    long elapsed = (System.currentTimeMillis() - now) / 1000;
    

    String message = MessageFormat.format(
        "T={0} {1} points written. Total throughput: {2} pt/sec | {3} val/sec. Current throughput: {4} val/sec",
        elapsed, writtenN, (float) writtenN / elapsed, (float) fieldsPerPoint * ((float) writtenN / elapsed),
        latestThroughput);
    System.out.println(message);
  }

  private void sendMonitorStats(boolean bool, long latestThroughput) {
    // TBD
  }

  // WrittenN returns the total number of points written.
  long writtenN() {
    return writtenN.get();
  }

  // TagsN returns the total number of tags.
  long tagsN() {
    long tagTotal = 1;
    for (long tag : tags) {
      tagTotal *= tag;
    }
    return tagTotal;
  }

  // SeriesN returns the total number of series to write.
  long seriesN() {
    return tagsN() * measurements;
  }

  // pointN returns the total number of points to write.
  long pointN() {
    return pointsPerSeries * seriesN();
  }

  // BatchN returns the total number of batches.
  long batchN() {
    long n = pointN() / batchSize;
    if (pointN() % batchSize != 0) {
      n++;
    }
    return n;
  }

  ClosableChannel generateBatches() {
    ClosableChannel queue = new ClosableChannel();
    
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        long[] values = new long[tags.size()];
        long lastWrittenTotal = writtenN();

        BatchPoints.Builder batchBuilder = BatchPoints.database(database).retentionPolicy(retentionPolicy)
            .consistency(consistencyLevel);
        BatchPoints batchPoints = batchBuilder.build();
        for (long i = 0; i < pointN(); i++) {
          long lastMN = i % measurements;
          Point.Builder pointBuilder = Point.measurement("m" + lastMN);
          for (int j = 0; j < values.length; j++) {
            pointBuilder = pointBuilder.tag("tag" + j, "value" + values[j]);
          }
          for (int j = 0; j < fieldsPerPoint; j++) {
            pointBuilder = pointBuilder.addField("v" + j, "1");
          }

          if (timePerSeries != 0) {
            long delta = (lastWrittenTotal + i) * timePerSeries;
            pointBuilder = pointBuilder.time(startTime + delta, TimeUnit.NANOSECONDS);
          }
          Point point = pointBuilder.build();

          // Increment next tag value.
          for (int j = 0; j < values.length; j++) {
            values[j]++;
            if (values[j] < tags.get(j)) {
              break;
            } else {
              values[j] = 0; // reset to zero, increment next value
              continue;
            }
          }

          batchPoints.point(point);
          // Start new batch, if necessary.
          if (i > 0 && i % batchSize == 0) {
            try {
              if (!lowLevelApi) {
                queue.put(batchPoints);
              } else {
                queue.put(batchPoints.lineProtocol());
              }
              batchBuilder = BatchPoints.database(database).retentionPolicy(retentionPolicy)
                  .consistency(ConsistencyLevel.valueOf(consistency.toUpperCase()));
              batchPoints = batchBuilder.build();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }

        // Add final batch.
        if (!batchPoints.getPoints().isEmpty()) {
          try {
            if (!lowLevelApi) {
              queue.put(batchPoints);
            } else {
              queue.put(batchPoints.lineProtocol());
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        try {
          // Close queue.
          queue.close();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    
    if (!sequentialBatchGen) {
      new Thread(runnable).start();
    } else {
      runnable.run();
    }

    return queue;
  }
}
