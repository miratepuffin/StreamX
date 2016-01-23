
package storm.starter;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.generated.GlobalStreamId;
import java.util.List;
import java.util.ArrayList;

public class CombinerGrouping implements CustomStreamGrouping{
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
		int batch = (int) values.get(0);
		boltIds.add(targetTasks.get(batch%count));
		return boltIds;
	}
}