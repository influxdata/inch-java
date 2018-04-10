package org.influxdb.tool.inch;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Inch {
  public static void main(String[] args) throws InterruptedException {
    
    Options options = new Options();
    
    options.addOption(Option.builder("help").desc("Print this help").hasArg(false).build());
    options.addOption(Option.builder("b").desc("Batch size - int (default 5000)").hasArg().build());
    options.addOption(Option.builder("c").desc("Concurrency - int (default 1)").hasArg().build());
    options.addOption(Option.builder("consistency").desc("Write consistency - String (default any) (default \"any\")").hasArg().build());
    options.addOption(Option.builder("db").desc("Database to write to - String (default \"j_stress\")").hasArg().build());
    options.addOption(Option.builder("dry").desc("Dry run (maximum writer perf of inch on box)").hasArg(false).build());
    options.addOption(Option.builder("f").desc("Fields per point - int (default 1)").hasArg().build());
    options.addOption(Option.builder("host").desc("Host - String (default \"http://localhost:8086\")").hasArg().build());
    options.addOption(Option.builder("user").desc("User Name - String").hasArg().build());
    options.addOption(Option.builder("password").desc("Password - String").hasArg().build());
    options.addOption(Option.builder("m").desc("Measurements - int (default 1)").hasArg().build());
    options.addOption(Option.builder("shardDuration").desc("Set shard duration - String (default 7d)").hasArg().build());
    options.addOption(Option.builder("t").desc("Tag cardinality -  String (default \"10,10,10\")").hasArg().build());
    options.addOption(Option.builder("time").desc("Time span to spread writes over - String (default \"1h\")").hasArg().build());
    
    Simulator simulator = null;
    // create the parser
    CommandLineParser parser = new DefaultParser();
    try {
        // parse the command line arguments
        CommandLine line = parser.parse(options, args);
        if (line.hasOption("help")) {
          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp("inch", options );
          return;
        }
        simulator = new Simulator();

        if (line.hasOption("b")) {
          simulator.batchSize = Long.parseLong(line.getOptionValue("b"));
        }
        if (line.hasOption("c")) {
          simulator.concurrency = Integer.parseInt(line.getOptionValue("c"));
        }
        if (line.hasOption("consistency")) {
          simulator.consistency = line.getOptionValue("consistency");
        }
        if (line.hasOption("db")) {
          simulator.database = line.getOptionValue("db");
        }
        if (line.hasOption("dry")) {
          simulator.dryRun = true;
        }
        if (line.hasOption("f")) {
          simulator.fieldsPerPoint = Long.parseLong(line.getOptionValue("f"));
        }
        if (line.hasOption("host")) {
          simulator.host = line.getOptionValue("host");
        }
        if (line.hasOption("user")) {
          simulator.user = line.getOptionValue("user");
        }
        if (line.hasOption("password")) {
          simulator.password = line.getOptionValue("password");
        }
        if (line.hasOption("m")) {
          simulator.measurements = Long.parseLong(line.getOptionValue("m"));
        }
        if (line.hasOption("shardDuration")) {
          simulator.shardDuration = line.getOptionValue("shardDuration");
        }
        if (line.hasOption("t")) {
          String val = line.getOptionValue("t");
          ArrayList<Long> list = new ArrayList<>();
          for (String t : val.split(",")) {
            list.add(Long.parseLong(t));
          }
          simulator.tags = list;
        }
        if (line.hasOption("time")) {
          String val = line.getOptionValue("time");
          Pattern pattern = Pattern.compile("\\d+");
          Matcher matcher = pattern.matcher(val);
          matcher.find();

          long number = Long.parseLong(matcher.group());
          String unit = val.substring(matcher.end());
          TimeUnit timeUnit = null;
          switch (unit) {
          case "ns":
              timeUnit = TimeUnit.NANOSECONDS;
            break;
          case "ms":
            timeUnit = TimeUnit.MICROSECONDS;
          break;
          case "milli":
            timeUnit = TimeUnit.MILLISECONDS;
          break;
          case "s":
            timeUnit = TimeUnit.SECONDS;
          break;
          case "m":
            timeUnit = TimeUnit.MINUTES;
          break;
          case "h":
            timeUnit = TimeUnit.HOURS;
          break;
          case "d":
            timeUnit = TimeUnit.DAYS;
          break;
          default:
            break;
          } 
          
          simulator.timeSpan = TimeUnit.NANOSECONDS.convert(number, timeUnit);
        }
        
    }
    catch( ParseException exp ) {
        System.err.println("Parsing failed.  Reason: " + exp.getMessage());
    }
    
    InchContext context = new InchContext();
    context.put("Done", false);
    simulator.run(context);
  }

}
