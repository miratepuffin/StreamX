
package storm.starter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.*;
import java.util.Map;
import java.util.Random;

public class CommandSpout extends BaseRichSpout {
  SpoutOutputCollector collector;
  Random random;
  long count;
  int id;
  boolean flip;
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    random = new Random();
    count = 0;
    id = context.getThisTaskId();
    flip =true;
  }

  @Override
  public void nextTuple() {
    if(flip){
      readTest();
      flip=!flip;  
    }
    
  }
  public void readTest(){
     try {
            FileReader fileReader = new FileReader("input/inputset0.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = "";
            while((line = bufferedReader.readLine()) != null) {
              collector.emit(new Values(id,count+" "+line));
              count++;
            }
            bufferedReader.close();         
        }
        catch(Exception ex) {}
  }
  public void genRow(){
    double probability = random.nextDouble();
    String command = count + " " ;
    if(probability<=0.40)
      command += "addEdge "+random.nextInt(5) + " Relation " + random.nextInt(5)+ "\n";
    else if(probability>0.40 && probability <= 0.7)
      command += "addNode "+random.nextInt(5) +"\n";
    else if(probability>0.70 && probability <= 0.9)
      command += "rmvEdge "+random.nextInt(5) + " Relation " + random.nextInt(5)+ "\n";
    else 
      command += "rmvNode "+random.nextInt(5) +"\n";
    count++;
    collector.emit(new Values(id,command));
  }
  @Override public void ack(Object id) {}
  @Override public void fail(Object id) {}
  @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("spoutID","commands"));}
}
