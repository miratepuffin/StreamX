
package storm.starter.Groupings;

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
	ArrayList<Integer> order;
	int lastnumber =0;
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks){
		this.targetTasks = targetTasks;
		this.context = context;
		this.stream = stream;
		count = targetTasks.size();
		order = new ArrayList<>();
	}
	public List<Integer> chooseTasks(int taskId, List<Object> values){
		List<Integer> boltIds = new ArrayList();
		String command = (String) values.get(0);
		String[] commandSplit = command.split(" ");
		int num = (int) values.get(1);

		if(commandSplit[0].trim().equals("rmvNode")){
			return targetTasks;
		}
		else{
			long id = Long.parseLong(commandSplit[1].trim());
			long temp = (id%count);
			boltIds.add(targetTasks.get((int) temp));
			return boltIds;
		}
	}
}
