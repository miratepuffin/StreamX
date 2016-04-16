package storm.starter.Bolts;

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
import backtype.storm.Constants;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TwitterOutBolt extends TickBolt {
  int fileCount = 0;
  @Override
  public void execute(Tuple tuple) {
    if (isTickTuple(tuple)) {
      output();
    }
    else {
      commands.add(tuple);
    }
     
  }
  public void output(){
    try{
      FileWriter fw = new FileWriter(new File("twitter/"+fileCount+".txt"));
      BufferedWriter bw = new BufferedWriter(fw);
      for(int i =0; i< commands.size(); i++){
        bw.write(commands.get(i).getString(0));
      }
      bw.close();
    }catch(IOException e){}
    commands = new ArrayList<>();
    fileCount++;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("batch","command"));
  }

}
