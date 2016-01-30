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
import backtype.storm.Constants;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ReduceBolt extends TickBolt {

  HashMap<String,String> reducedCommands;
  ArrayList<String> rmvGenList;
  ArrayList<String> batchEdges;
  HashMap<String,Long> edges;
  public ReduceBolt(){
    edges = new HashMap<>();
    rmvGenList = new ArrayList<String>();
    batchEdges = new ArrayList<String>();
  }

  @Override
  public void execute(Tuple tuple) {
    if (isTickTuple(tuple)) {
      //collector.emit(new Values(batchNumber,"commandCount 1"));
      //collector.emit(new Values(batchNumber,"addNode 1"));
      reduce();
      updateEdges();
      edgeCheck();
    }
    else {
      commands.add(tuple);
    }
     
  }
  private void reduce(){
    reduceCommands();
    //System.out.println("ID: "+localid+" Message: "+reducedCommands.size());
    collector.emit(new Values(batchNumber,"commandCount "+reducedCommands.size()));
    for (Map.Entry<String, String> command : reducedCommands.entrySet()){
      if(command.getValue().split(" ")[1].trim().equals("rmvNode")){
        if(localid == 0){
          //System.out.println("ID: "+localid+" Message: "+command.getValue());
          collector.emit(new Values(batchNumber,command.getValue()));
        }
        else{
          //System.out.println("ID: "+localid+" Message: "+command.getValue());
          collector.emit(new Values(batchNumber,"REMOVED"));
        }   
      }
      else {
        //System.out.println("ID: "+localid+" Message: "+command.getValue());
        collector.emit(new Values(batchNumber,command.getValue()));
      }
    }
    commands = new ArrayList<>();
    batchNumber++;
    reducedCommands = null;
  }


  public void reduceCommands() {
    HashMap<String,String> commandMap = new HashMap<>(); // as we will not care about order once we are finished
    
    for (int i =(commands.size()-1);i>=0;i--) {
      String commandFull = commands.get(i).getString(1);
      String[] split = commandFull.split(" ");
      String command = split[0];
      String src = split[1];
      String msg = "";
      String dest ="";
      if(split.length > 2) {
       msg  = split[2];
       dest = split[3];
      }
      if (command.equals("addEdge")) {
        addEdge(commandFull,command,src,msg,dest, commandMap);
      }
      //------------------Check if Add Node command happens later or is negated by a remove ------------------//
      else if (command.equals("addNode")) {
        if (commandMap.get("rmvNode " + src)==null) {commandMap.put("addNode "+src.trim(),commandFull.trim()+"\n");}
      }
      //------------------Check if Remove edge command happens later or is negated by an add ------------------//
      else if (command.equals("rmvEdge")) {
        rmvEdge(commandFull,command,src,msg,dest,commandMap);
       }

      //------------------Check if Remove node command happens later ------------------//
      else if (command.equals("rmvNode")) {
          commandMap.put("rmvNode " + src.trim(),commandFull.trim()+"\n"); //again as set no need to check if contains
      }
    }
    for(int i =0;i<rmvGenList.size();i++){
      String[] split = rmvGenList.get(i).split(" ");
      String command = split[0];
      String src  = split[1];
      String msg  = split[2];
      String dest = split[3];
      rmvEdge(rmvGenList.get(i),command,src,msg,dest,commandMap); 

    }
    rmvGenList = new ArrayList<String>();
    reducedCommands = commandMap;
    //System.out.println("ID: " + localid + " Before: "+commands.size() +" After:"+reducedCommands.size());
  }

  private void addEdge(String commandFull, String command, String src, String msg, String dest, HashMap<String,String> commandMap){
    if (commandMap.get("rmvNode " + src)!=null) {
      // check if the src Id is removed lower down
      if (commandMap.get("rmvNode " + dest)==null) {
      // if it is then we check if the dest node is also removed, otherwise add it
        commandMap.put("addNode " + dest.trim(),"addNode " + dest.trim()+"\n");
      }
    }
    else if (commandMap.get("rmvNode " + dest)!=null) {
      // check if the dest Id is removed lower down
      commandMap.put("addNode " + src.trim(),"addNode " + src.trim()+"\n"); // no need to check src rmv as we know it is not there from above
    }

    else if (commandMap.get("rmvEdge " + src + " " + msg + " " + dest)!=null) {
      commandMap.put("addNode " + src.trim(), "addNode " + src.trim()+"\n"); //no need to check if they are negated as it is checked above
      commandMap.put("addNode " + dest.trim(),"addNode " + dest.trim()+"\n"); //.5 command as the singular is split into 2
    }
    else {
      //if there are no remove nodes or edges then we can add the command to the subset
      commandMap.put("addEdge " + src + " " + msg + " " + dest.trim(),commandFull.trim()+"\n");
      batchEdges.add("addEdge " + src + " " + msg + " " + dest.trim()+"\n");
      //System.out.println("Adding: "+commandFull);
    }
  }
  private void rmvEdge(String commandFull, String command, String src, String msg, String dest, HashMap<String,String> commandMap){
     if (commandMap.get("addEdge " + src + " " + msg + " " + dest)!=null) {} // check if it is negated
     else if (commandMap.get("rmvNode " + src)!=null) {} // check if negated by a node remove below
     else if (commandMap.get("rmvNode " + dest)!=null) {} // check if negated by a node remove below
     else {commandMap.put("rmvEdge " + src + " " + msg + " " + dest.trim(),commandFull.trim()+"\n");}
  }


  public void updateEdges(){
    Long time = System.currentTimeMillis();
    for(int i =0; i<batchEdges.size();i++){
      edges.put(batchEdges.get(i),time);
    }
    batchEdges = new ArrayList<>();
  }

  public void edgeCheck(){
    Long time = System.currentTimeMillis();
    HashSet<String> toRemove  = new HashSet<>();
    for (Map.Entry<String, Long> command : edges.entrySet()){
      if((time-command.getValue())>3000){
        String[] split = command.getKey().trim().split(" ");
        rmvGenList.add("rmvEdge "+split[1]+" "+split[2]+" "+split[3]+"\n");
        //System.out.println("Removing Node: "+command.getKey());
        toRemove.add(command.getKey());
      }
    }
    edges.keySet().removeAll(toRemove);
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("batch","command"));
  }

}