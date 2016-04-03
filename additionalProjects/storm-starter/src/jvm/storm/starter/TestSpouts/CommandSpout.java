
package storm.starter.TestSpouts;

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
    flip =false;
  }
  @Override
  public void nextTuple() {
    if(flip){
      readTest();
      flip=!flip;  
    }
   genRow();
    
  }
  public void genRow(){
   // Utils.sleep(500);
    double probability = random.nextDouble();
    String command = "" ;
    if(probability<=0.70)
      command += "addEdge "+random.nextInt(500000) + " Relation " + random.nextInt(500000)+ "\n";
    else if(probability>0.70 && probability <= 0.8)
      command += "addNode "+random.nextInt(500000) +"\n";
    else if(probability>0.80 && probability <= 0.95)
      command += "rmvEdge "+random.nextInt(500000) + " Relation " + random.nextInt(500000)+ "\n";
    else 
      command += "rmvNode "+random.nextInt(500000) +"\n";
    collector.emit(new Values(command));
    count++;
  }
  public void readTest(){
     try {
            FileReader fileReader = new FileReader("input/inputset0.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = "";
            while((line = bufferedReader.readLine()) != null) {
              //System.out.println(line);
              collector.emit(new Values(line));
              count++;
            }
            bufferedReader.close();         
        }
        catch(Exception ex) {}
  }
  @Override public void ack(Object id) {}
  @Override public void fail(Object id) {}
  @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("commands"));}
}
