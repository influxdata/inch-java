package org.influxdb.tool.jmh;

import java.util.concurrent.TimeUnit;

import org.influxdb.tool.Utils;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Defaults;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * You can use this class to run benchmarks in your IDE.
 */
public class ClientBenchmarksRunner {

  public static void main(String[] args) throws RunnerException {
    Options opts = new OptionsBuilder()
        // regexp which benchmark to run
        .include(".Async*")
        .mode(Mode.AverageTime)
        //no warmup while debugging
        .warmupIterations(0)
        .warmupTime(TimeValue.seconds(1))
        .measurementIterations(1)
        .measurementTime(TimeValue.seconds(1))
        .timeUnit(TimeUnit.MILLISECONDS)
        .jvmArgs("-Xms2g", "-Xmx2g")
        //specify custom property
        .param("queryType","singleProperty")
        .resultFormat(ResultFormatType.JSON)
        .result(Defaults.RESULT_FILE_PREFIX + "-" + Utils.getClientVersion() + ".json")
        .shouldDoGC(true)
        // run the benchmark in the same thread - for debugging purpose
        .forks(0)
        .build();

    new Runner(opts).run();

  }

}
