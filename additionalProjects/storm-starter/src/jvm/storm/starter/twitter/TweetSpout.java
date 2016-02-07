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


public class TweetSpout extends BaseRichSpout{
    // Twitter API authentication credentials
  String custkey, custsecret;
  String accesstoken, accesssecret;
  int count =0;

    // To output tuples from spout to the next stage bolt
  SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
  TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
  LinkedBlockingQueue<Status> queue = null;

    // Class for listening on the tweet stream - for twitter4j
  private class TweetListener implements StatusListener {

      // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status)
    {
        // add the tweet into the queue buffer
      count++;
      queue.offer(status);
    }
    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn){}
    @Override
    public void onTrackLimitationNotice(int i){}
    @Override
    public void onScrubGeo(long l, long l1){}
    @Override
    public void onStallWarning(StallWarning warning){}
    @Override
    public void onException(Exception e){e.printStackTrace();}
  };

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public TweetSpout(String key,String secret,String token,String tokensecret)
    {
      custkey = key;
      custsecret = secret;
      accesstoken = token;
      accesssecret = tokensecret;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
    {
      // create the buffer to block tweets
      queue = new LinkedBlockingQueue<Status>(100000);

      // save the output collector for emitting tuples
      collector = spoutOutputCollector;

      // build the config with credentials for twitter 4j
      ConfigurationBuilder config =
      new ConfigurationBuilder()
      .setOAuthConsumerKey(custkey)
      .setOAuthConsumerSecret(custsecret)
      .setOAuthAccessToken(accesstoken)
      .setOAuthAccessTokenSecret(accesssecret);

      // create the twitter stream factory with the config
      TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

      // get an instance of twitter stream
      twitterStream = fact.getInstance();

      // provide the handler for twitter stream
      twitterStream.addListener(new TweetListener());

      //filter the stream
      FilterQuery query = new FilterQuery();
      query = query.count(100000);
      //String[] languages = {"en"};
      //query.language(languages);
      twitterStream.filter(query);
      // start the sampling of tweets
      twitterStream.sample();
    }

    @Override
    public void nextTuple()
    {
      // try to pick a tweet from the buffer
      Status ret = queue.poll();
      if (ret==null){return;}
      collector.emit(new Values(ret));
      Utils.sleep(30000);
    }

    @Override
    public void close(){ twitterStream.shutdown();}

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
      // create the component config
      Config ret = new Config();
      // set the parallelism for this spout to be 1 as otherwise twitter throws a fit
      ret.setMaxTaskParallelism(1);
      return ret;
    }

    @Override
    public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
    {
      // tell storm the schema of the output tuple for this spout
      // tuple consists of a single column called 'tweet'
      outputFieldsDeclarer.declare(new Fields("Status"));
    }
  }
