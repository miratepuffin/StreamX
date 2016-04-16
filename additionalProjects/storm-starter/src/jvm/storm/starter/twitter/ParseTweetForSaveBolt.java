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
  import twitter4j.UserMentionEntity;
  import twitter4j.FilterQuery;
  import twitter4j.StatusDeletionNotice;
  import twitter4j.StatusListener;
  import twitter4j.StallWarning;

  import java.util.HashMap;
  import java.util.ArrayList;
  import java.util.Map;
  import java.util.concurrent.LinkedBlockingQueue;

  import java.io.BufferedReader;
  import java.io.InputStreamReader;
  import java.util.ArrayList;
  import java.util.List;

  import org.apache.http.HttpResponse;
  import org.apache.http.NameValuePair;
  import org.apache.http.client.HttpClient;
  import org.apache.http.client.entity.UrlEncodedFormEntity;
  import org.apache.http.client.methods.HttpGet;
  import org.apache.http.client.methods.HttpPost;
  import org.apache.http.impl.client.DefaultHttpClient;
  import org.apache.http.message.BasicNameValuePair;

  import java.util.Random;



  public class ParseTweetForSaveBolt extends BaseRichBolt
  {
    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext,OutputCollector outputCollector)
    {
      // save the output collector for emitting tuples
      collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
      Status status = (Status) tuple.getValue(0);
      ArrayList<Long> usersInvolved = getInvolvedUsers(status);
      userCommands(usersInvolved,status);
    }

    public ArrayList<Long> getInvolvedUsers(Status status){

      ArrayList<Long> usersInvolved = new ArrayList<>();
      User user = status.getUser();
      usersInvolved.add(user.getId());
      long[] contributers = status.getContributors();
      if((contributers!=null) && (contributers.length >0)){
        for(int j=0;j<contributers.length;j++){
          usersInvolved.add(contributers[j]);
        }
      }
      UserMentionEntity[] userMentions = status.getUserMentionEntities();
      if((userMentions!=null) && (userMentions.length >0)){
        for(int j=0;j<userMentions.length;j++){
          usersInvolved.add(userMentions[j].getId());
        }
      }
      return usersInvolved;
    }
    public void userCommands(ArrayList<Long> usersInvolved,Status status){
      User user = status.getUser();
      usersInvolved.remove(0);
      generateCommands(user.getId(),usersInvolved,status.getText());

    }
    private void generateCommands(Long id, ArrayList<Long> followers,String tweet){
      String safeTweet = tweet.replaceAll(" ","_").replaceAll("\n", "").replaceAll("\r", "");;
      for(int i =0;i<followers.size();i++){
        collector.emit(new Values("addEdge "+id+" "+safeTweet+" "+ followers.get(i)+"\n"));
      }
    }

 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
      declarer.declare(new Fields("commands"));
    }
  }