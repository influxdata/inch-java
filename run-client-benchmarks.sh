#!/usr/bin/env bash
# Usage: $0 '2.13' '2.14' ....

### Drop database
influx -execute "drop database AsyncQueryBench1"

### Import database
influxd restore -portable -db AsyncQueryBench1 data/AsyncQueryBench1

totalPoints=1000000

if [ $# -lt 0 ]
then
  echo "Usage: $0 [client versions]"
  echo "Usage: $0 '2.13' '2.14' .... "
  exit
fi

for client in "$@"
do
    echo "Running benchmark for client: "${client}
    mvn -DskipTests=true -Dclient.version=${client} install
    java -cp target/benchmarks-${client}.jar org.openjdk.jmh.Main \
     -rff target/jmh-result-${client}.json -rf json \
     -r 1 -w 1 -wi 1 -bm avgt -f 1 -tu ms \
     -p totalPoints=${totalPoints} \
     -p clientVersion=${client}
done
