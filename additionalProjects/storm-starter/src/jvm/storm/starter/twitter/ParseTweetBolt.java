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



  public class ParseTweetBolt extends BaseRichBolt
  {
    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext,OutputCollector outputCollector)
    {
      // save the output collector for emitting tuples
      collector = outputCollector;
    }

    /****TO USE****

    Status--

    getText
    getUser
    isRetweet
    getRetweetedStatus
    getContributors
    getUserMentionEntities

    ***************/

    @Override
    public void execute(Tuple tuple)
    {
      System.out.println();
      System.out.println();
      System.out.println();
      System.out.println("TWEEET START");


      Status status = (Status) tuple.getValue(0);
      
      ArrayList<Long> usersInvolved = getInvolvedUsers(status);
      userCommands(usersInvolved,status);

      System.out.println(status.getText());
      System.out.println(usersInvolved.size());
      System.out.println("TWEET END");
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
      int followerCount = user.getFollowersCount();
      if(followerCount>5000){followerCount=5000;}
      
      ArrayList<Long> mainUserFollowers = getUserFollowers(usersInvolved.get(0),followerCount);
      generateCommands(user.getId(),mainUserFollowers,status.getText());

      Random random = new Random(user.getId());
      for(int i=1; i<usersInvolved.size();i++){
        int genFollowerCount = random.nextInt(5001);
        ArrayList<Long> secondaryUserFollowers = getUserFollowers(usersInvolved.get(i),genFollowerCount);
        generateCommands(usersInvolved.get(i),secondaryUserFollowers,status.getText());
      }

    }
    private void generateCommands(Long id, ArrayList<Long> followers,String tweet){
      String safeTweet = tweet.replaceAll(" ","_");
      for(int i =0;i<followers.size();i++){
        collector.emit(new Values("addEdge "+id+" "+safeTweet+" "+ followers.get(i)));
        System.out.println("addEdge "+id+" "+safeTweet+" "+ followers.get(i));
      }
    }

    public ArrayList<Long> getUserFollowers(Long userID,int followerCount){
      String oauth_consumer_key = "hY6rgk7n387pw48XrRTodw5S5";
      String oauth_nonce ="4d1046c38d6f3a4ffab690aa54c059f5";
      String oauth_signature ="PCtft0c6DaNIRfuafAyqWBV5u%2F8%3D";
      String oauth_timestamp = "1454804634";
      String oauth_token = "1565672916-53qrz4XRuof7HcyiqKqQ9KoSSQ3hXty1WRjNbEK";
      String auth = "OAuth oauth_consumer_key=\""+oauth_consumer_key+"\", oauth_nonce=\""+oauth_nonce+"\", oauth_signature=\""+oauth_signature+"\", oauth_signature_method=\"HMAC-SHA1\", oauth_timestamp=\""+oauth_timestamp+"\", oauth_token=\""+oauth_token+"\", oauth_version=\"1.0\"";
      
      String url = "https://api.twitter.com/1.1/followers/ids.json?count=5000&cursor=-1&user_id="+userID;
      //url = "http://www.google.com/search?q=developer";
      try{
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);

      // add request header
        request.addHeader("User-Agent", "Mozilla/5.0");
        request.addHeader("Authorization",auth);

        HttpResponse response = client.execute(request);

        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + 
         response.getStatusLine().getStatusCode());
        if(response.getStatusLine().getStatusCode() == 401){
          return getFakeFollowers(userID,followerCount);
        }
        BufferedReader rd = new BufferedReader(
         new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        int count =0;
        while ((line = rd.readLine()) != null) {
          count ++;
          result.append(line);
        }
        String resultString = result.toString();
        if(resultString.equals("{ \"errors\": [ { \"code\": 88, \"message\": \"Rate limit exceeded\" } ] }")){
          return getFakeFollowers(userID,followerCount);
        }

        int openBracket = resultString.indexOf('[')+1;
        int closeBracket = resultString.indexOf(']');
        resultString = resultString.substring(openBracket,closeBracket);
        String[] resultArray  = resultString.split(",");
        ArrayList<Long> longResults = new ArrayList<>();
        for(int i =0;i<resultArray.length;i++){
          try{
           longResults.add(Long.parseLong(resultArray[i]));
          }
          catch(NumberFormatException e ){}
        }
        return longResults;
      }catch(Exception e){}
      return new ArrayList<Long>();
    }
    public ArrayList<Long> getFakeFollowers(Long userID,int followerCount){
      Random random = new Random(userID); // use user Id as the seed so that the same id's are generated every time that user comes through
      ArrayList<Long> genUsers = new ArrayList<>();
      long range = 500000000L;
      for(int i =0; i<followerCount; i++){
        genUsers.add((long)(random.nextDouble()*range));
      }
      return genUsers;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
      declarer.declare(new Fields("command"));
    }
  }