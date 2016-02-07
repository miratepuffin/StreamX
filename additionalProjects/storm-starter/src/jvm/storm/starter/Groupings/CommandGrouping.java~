
package storm.starter;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.generated.GlobalStreamId;
import java.util.List;
import java.util.ArrayList;

public class CommandGrouping implements CustomStreamGrouping{
	List<Integer> targetTasks;
	int count;
	WorkerTopologyContext context;
	GlobalStreamId stream;
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks){
		this.targetTasks = targetTasks;
		this.context = context;
		this.stream = stream;
		count = targetTasks.size();
	}
	public List<Integer> chooseTasks(int taskId, List<Object> values){
		List<Integer> boltIds = new ArrayList();
		String command = (String) values.get(1);
		String[] commandSplit = command.split(" ");
		if(commandSplit[0].trim().equals("rmvNode")){
			return targetTasks;
		}
		else{
			int id = Integer.parseInt(commandSplit[1].trim());
			// if(commandSplit[1].trim().equals("addNode")){
			// 	System.out.println("num: "+commandSplit[0]+ " id: "+id +" split: "+ targetTasks.get(id%count));
			// }
			boltIds.add(targetTasks.get(id%count));
			return boltIds;
		}
	}
}