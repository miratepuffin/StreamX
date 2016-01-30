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
    int localid;
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
      localid = context.getThisTaskIndex();
    }

    @Override
    public void execute(Tuple tuple) {
      collector.ack(tuple);
      String command = tuple.getString(1);
      if(receivedCount!=splitCount){
        if(command.contains("commandCount")){
           totalCommands += Integer.parseInt(command.split(" ")[1].trim());
           System.out.println(command.split(" ")[1].trim()+ "NEW VALUES: "+totalCommands);
           //System.out.println(fileCount+" Total commands:"+totalCommands);
           receivedCount++;
           if((receivedCount==splitCount)&&(totalCommands==receivedCommands)){ //This is the case of if the last ReduceBolt has 0, as this would mean output was never called.
              output();
           }
        }
        else{
          addToMap(command);
        }
      }
      else if (command.contains("commandCount")){
        System.out.println(localid+ ": BROKE BROKE "+ receivedCommands+ " "+totalCommands); 
      }
      else{
        addToMap(command);
        //System.out.println(localid+": RE: "+ receivedCommands+" TC: "+totalCommands);
        if(receivedCommands == totalCommands){
          output();
        }
      }
      if((receivedCount==splitCount)&&(totalCommands==0)){
        System.out.println("EMPTY");
        reset();
        fileCount++;
      }
    }


    public void output(){
      try{
        //System.out.println("ID: "+id);
        FileWriter fw = new FileWriter(new File("output/id="+id+"batch="+fileCount));
        BufferedWriter bw = new BufferedWriter(fw);
        for (Map.Entry<String, String> command : commands.entrySet()){
          bw.write(command.getValue());
        }
        bw.write("END OF FILE");
        bw.close();
        System.out.println("output/id="+id+"batch="+fileCount+" commands: "+receivedCommands);
      }catch(IOException e){}
      reset();
      fileCount++;
    }


    private void reset(){
      //System.out.println(localid+": Resetting");
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
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("finished"));
    }

  }