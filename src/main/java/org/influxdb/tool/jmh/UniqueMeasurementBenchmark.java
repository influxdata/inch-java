package org.influxdb.tool.jmh;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.influxdb.tool.Utils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarking of lineProtocol serialization.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)

public class UniqueMeasurementBenchmark {

  @Param({"1000000"})
  public int totalPoints;

  @Param({"100", "1000", "10000", "50000", "100000"})
  public int uniqueMeasurementCount;

  @Setup
  public void setup() {
    System.out.println("Client version: "+ Utils.getClientVersion());
  }

  @Benchmark
  public void uniqueMeasurementBenchmark(Blackhole blackhole) {
    int iterations = 0;

    while (iterations < totalPoints) {
      Point p = createPoint(uniqueMeasurementCount);
      iterations++;
      blackhole.consume(p.lineProtocol());
    }
  }

  private Point createPoint(int uniqueMeasurementCount) {
    return Point.measurement("measurement_" + ThreadLocalRandom.current().nextInt(0, uniqueMeasurementCount))
        .addField("field_" + randomString(2), randomString(500))
        .addField("tag_" + randomString(2), randomString(3))
        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS).build();
  }

  private String randomString(int length) {
    byte[] array = new byte[length];
    new Random().nextBytes(array);
    return new String(Base64.getEncoder().encode(array), StandardCharsets.UTF_8);
  }

}
