
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import java.io.*;

import storm.starter.twitter.TweetSpout;
import storm.starter.twitter.ParseTweetBolt;
import storm.starter.Bolts.CombinerBolt;
import storm.starter.Bolts.ReduceBolt;
import storm.starter.Groupings.CombinerGrouping;
import storm.starter.Groupings.CommandGrouping;
import storm.starter.TestSpouts.CommandSpout;

public class CommandReductionTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    TweetSpout tweetSpout = new TweetSpout(
        "hY6rgk7n387pw48XrRTodw5S5",
        "LSF41BtAOFmS1k8XmUHvXEOPHcNIMPIGI9p2oDTk7qNZxGScfH",
        "1565672916-53qrz4XRuof7HcyiqKqQ9KoSSQ3hXty1WRjNbEK",
        "AEqxYdIQ22JMwwb2rWFhcxur7LY8Xib1it8UBv5IGU6LC"
    );
    int reduceSplit = 6;

    //builder.setSpout("tweets", tweetSpout, 1);
    //builder.setBolt("commands", new ParseTweetBolt(), 10).shuffleGrouping("tweets");
    builder.setSpout("commands", new CommandSpout(), 1);
    builder.setBolt("reducer",   new ReduceBolt(), reduceSplit).customGrouping("commands", new CommandGrouping());
    builder.setBolt("combiner",  new CombinerBolt(reduceSplit,args[0]), 10).customGrouping("reducer", new CombinerGrouping());
    Config conf = new Config();
    conf.setDebug(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    
    Utils.sleep(10000000);
    cluster.killTopology("test");
    cluster.shutdown();
  }



/*
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    */
 private static void resetOutput() {
        StringBuffer output = new StringBuffer();
        Process p;
        try {
             	p = Runtime.getRuntime().exec("hadoop fs -rm -r /user/bas30/output && hadoop fs -mkdir /user/bas30/output");
                p.waitFor();
                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line = "";
                while ((line = reader.readLine())!= null) {
                        output.append(line + "\n");
                }
                System.out.println(output);
        } catch (Exception e) {
                e.printStackTrace();
        }
    }

}
