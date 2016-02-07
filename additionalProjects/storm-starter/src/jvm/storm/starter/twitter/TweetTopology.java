package storm.starter.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.FilterQuery;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetTopology {

  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
        "hY6rgk7n387pw48XrRTodw5S5",
        "LSF41BtAOFmS1k8XmUHvXEOPHcNIMPIGI9p2oDTk7qNZxGScfH",
        "1565672916-53qrz4XRuof7HcyiqKqQ9KoSSQ3hXty1WRjNbEK",
        "AEqxYdIQ22JMwwb2rWFhcxur7LY8Xib1it8UBv5IGU6LC"
    );

    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);
    builder.setBolt("test",new ParseTweetBolt(),1).allGrouping("tweet-spout");

    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      // run it in a live cluster
      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      // run it in a simulated local cluster
      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      Utils.sleep(30000);

      cluster.killTopology("tweet-word-count");
      cluster.shutdown();
    }
  }
}