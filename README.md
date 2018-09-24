# inch-java
Java-based implementation of an InfluxDB benchmarking tool.

inch-java is the port to JAVA of [Inch](https://github.com/influxdata/inch) - a benchmarking tool for testing with different tag
cardinalities.

## Running
Clone and run
```
mvn install
```

## Running

The inch-java program accepts several flags to adjust the number of points and
tag values.

```
usage: java -jar inch-java-1.0-jar-with-dependencies.jar [-b <arg>] [-c <arg>] [-consistency <arg>] [-db <arg>] [-dry]
       [-f <arg>] [-help] [-host <arg>] [-lowLevelApi] [-m <arg>] [-p
       <arg>] [-password <arg>] [-sequentialBatchGen] [-shardDuration
       <arg>] [-t <arg>] [-time <arg>] [-user <arg>]
 -b <arg>               Batch size - int (default 5000)
 -c <arg>               Concurrency - int (default 1)
 -consistency <arg>     Write consistency - String (default any) (default
                        "any")
 -db <arg>              Database to write to - String (default "j_stress")
 -dry                   Dry run (maximum writer perf of inch on box)
 -f <arg>               Fields per point - int (default 1)
 -help                  Print this help
 -host <arg>            Host - String (default "http://localhost:8086")
 -lowLevelApi           Use low-level API
 -m <arg>               Measurements - int (default 1)
 -p <arg>               Points per series - int (default 100)
 -password <arg>        Password - String
 -sequentialBatchGen    Run the Batch Point Generator sequentially
 -shardDuration <arg>   Set shard duration - String (default 7d)
 -t <arg>               Tag cardinality -  String (default "10,10,10")
 -time <arg>            Time span to spread writes over - String (default
                        "1h")
 -user <arg>            User Name - String
```

The `-t` flag specifies the number of tags and the cardinality by using a
comma-separated list of integers. For example, the value `"100,20,4"` means 
that 3 tag keys should be used. The first one has 100 values, the second one
has 20 values, and the last one has 4 values. `inch` will insert a series for
each combination of these values so the total number of series can be computed
by multiplying the values (`100 * 20 * 4`).


## Inlfuxdb-java client benchmarking

inch-java uses OpenJdk code benchmarking tool [JMH](http://openjdk.java.net/projects/code-tools/jmh/). 
Benchmark code is located in `org.influxdb.tool.jmh` package.

It is possible to implement a new be benchmark by adding `org.openjdk.jmh.annotations.Benchmark` annotated class
into that package.

To run all benchmarks for multiple version of java clients run:

```bash
./run-client-bechmarks.sh '2.13' '2.14-SPAPSHOTS'
```

### Benchmark report visualization
For result visualization you can use (https://jmh.morethan.io/). Just drag and drop all `target/*.json` reports into it. 