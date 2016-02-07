
package storm.starter.Groupings;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.generated.GlobalStreamId;
import java.util.List;
import java.util.ArrayList;

public class CombinerGrouping implements CustomStreamGrouping{
	List<Integer> targetTasks; //list of combiner tasks available
	int count; // number of combiner tasts
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
		int batch = (int) values.get(0); // extract the batch number from the tuple
		boltIds.add(targetTasks.get(batch%count)); // select the combiner dealing with this batch
		return boltIds; // return the combiner id
	}
}
