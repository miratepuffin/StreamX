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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

  public class ReduceBolt extends BaseRichBolt {
    OutputCollector collector;
    TopologyContext context;
    int id;
    int otherid;
    ArrayList<Tuple> commands;
    HashMap<String,String> reducedCommands;
    int batchNumber;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
      commands = new ArrayList<>();
      this.context = context;
      id = context.getThisTaskId();
      otherid = context.getThisTaskIndex();
      batchNumber =0;
    }

    @Override
    public void execute(Tuple tuple) {
      if (isTickTuple(tuple)) {
        reduce();
      }
      else if(!(tuple.getValue(1) instanceof String)){
        System.out.println("Sdasdasdas");
      }
      else {
        commands.add(tuple);
      }
      
    }
    private void reduce(){
      reduceCommands();
      collector.emit(new Values(batchNumber,"commandCount "+reducedCommands.size()));
      for (Map.Entry<String, String> command : reducedCommands.entrySet()){
        if(command.getValue().split(" ")[1].trim().equals("rmvNode")){
          if(otherid == 0){
            collector.emit(new Values(batchNumber,command.getValue()));
          }
          else{
            collector.emit(new Values(batchNumber,"REMOVED"));
          }   
        }
        else {
          collector.emit(new Values(batchNumber,command.getValue()));
        }
      }
      commands = new ArrayList<>();
      reducedCommands = null;
      batchNumber++;
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
        }
      }
      //------------------Check if Add Node command happens later or is negated by a remove ------------------//
      else if (command.equals("addNode")) {
        if (commandMap.get("rmvNode " + src)==null) {
          commandMap.put("addNode "+src.trim(),commandFull.trim()+"\n");
        }
      }
      //------------------Check if Remove edge command happens later or is negated by an add ------------------//
      else if (command.equals("rmvEdge")) {
        if (commandMap.get("addEdge " + src + " " + msg + " " + dest)!=null) {} // check if it is negated
        else if (commandMap.get("rmvNode " + src)!=null) {} // check if negated by a node remove below
        else if (commandMap.get("rmvNode " + dest)!=null) {} // check if negated by a node remove below
        else {commandMap.put("rmvEdge " + src + " " + msg + " " + dest.trim(),commandFull.trim()+"\n");}
        }

      //------------------Check if Remove node command happens later ------------------//
      else if (command.equals("rmvNode")) {
          commandMap.put("rmvNode " + src.trim(),commandFull.trim()+"\n"); //again as set no need to check if contains
        }
    }
    reducedCommands = commandMap;
    System.out.println("ID: " + otherid + " Before: "+commands.size() +" After:"+reducedCommands.size());
  }

  private static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("batch","command"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    int tickFrequencyInSeconds = 1;
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
    return conf;
  }

}