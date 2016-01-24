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
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

  public class CombinerBolt extends BaseRichBolt {
    OutputCollector collector;
    TopologyContext context;
    int id;
    int fileCount;
    HashMap<String,String> commands;
    int splitCount;
    int receivedCount;
    int totalCommands;
    int receivedCommands;
    
    public CombinerBolt(int splitCount){
      this.splitCount  = splitCount;
      reset();
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
      this.context = context;
      id = context.getThisTaskId();
      fileCount =0;
    }

    @Override
    public void execute(Tuple tuple) {
      collector.ack(tuple);
      String command = tuple.getString(1);
      if(receivedCount!=splitCount){
        if(command.contains("commandCount")){
           totalCommands += Integer.parseInt(command.split(" ")[1].trim());
           receivedCount++;
        }
        else{
          addToMap(command);
        }
      }
      else if (command.contains("commandCount")){
        System.out.println("BROKE BROKE");
      }
      else{
        addToMap(command);
        if(receivedCommands == totalCommands){
          output();
        }
      }
      if((receivedCount!=splitCount)&&(totalCommands==0)){
        reset();
        fileCount++;
      }
    }
    public void output(){
      try{
        System.out.println("ID: "+id);
        FileWriter fw = new FileWriter(new File("output/id="+id+"batch="+fileCount));
        BufferedWriter bw = new BufferedWriter(fw);
        for (Map.Entry<String, String> command : commands.entrySet()){
          bw.write(command.getValue());
        }
        bw.close();
      }catch(IOException e){}
      reset();
      fileCount++;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("finished"));
    }
    private void reset(){
      receivedCount    = 0;
      totalCommands    = 0;
      receivedCommands = 0;
      commands = new HashMap<>();
    }
    private void addToMap(String command){
      if(command.equals("REMOVED")){}
      else if(commands.get(command)==null){
        commands.put(command,command);  
      }
      receivedCommands++;
    }
  }