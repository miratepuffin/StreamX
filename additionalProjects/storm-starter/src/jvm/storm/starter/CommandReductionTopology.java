
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

public class CommandReductionTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    int reduceSplit = 6;
    builder.setSpout("commands", new CommandSpout(), 1);
    builder.setBolt("reducer",   new ReduceBolt(), reduceSplit).customGrouping("commands", new CommandGrouping());
    builder.setBolt("combiner",  new CombinerBolt(reduceSplit), 3).customGrouping("reducer", new CombinerGrouping());
    Config conf = new Config();
    conf.setDebug(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    
    Utils.sleep(10000000);
    cluster.killTopology("test");
    cluster.shutdown();
  }
}


/*
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    */