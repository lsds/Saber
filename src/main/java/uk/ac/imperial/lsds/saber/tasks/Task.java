package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.atomic.AtomicMarkableReference;

import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowBatchFactory;
import uk.ac.imperial.lsds.saber.handlers.ResultCollector;
import uk.ac.imperial.lsds.saber.handlers.ResultHandler;

public class Task extends AbstractTask {
	
	private WindowBatch batch1, batch2;
	
	public Task() {
		this(0, null, null);
	}
	
	public Task (int taskid, WindowBatch batch1, WindowBatch batch2) {
		
		this.taskid = taskid;
		this.batch1 = batch1;
		this.batch2 = batch2;
		
		/* Either batch would do */
		if (batch1 != null)
			queryid = batch1.getQuery().getId();
		else
			queryid = -1;
		
		this.next = new AtomicMarkableReference<AbstractTask>(null, false);
	}
	
	public void set (int taskid, WindowBatch batch1, WindowBatch batch2) {
		
		this.taskid = taskid;
		
		this.batch1 = batch1;
		this.batch2 = batch2;
		
		/* Either batch would do */
		if (batch1 != null)
			queryid = batch1.getQuery().getId();
		else
			queryid = -1;
		
		this.next.set(null, false);
	}
	
	@Override
	public int run() {
		
		Query query = batch1.getQuery();
		QueryOperator next = query.getMostUpstreamOperator();
		
		if (next.getDownstream() != null)
			throw new RuntimeException ("error: execution of chained query operators is not yet tested");
		
		if (batch2 == null)
			next.process(batch1, this, GPU);
		else
			next.process(batch1, batch2, this, GPU);
		
		/* Operator `next` calls `outputWindowBatchResult()` and updates `batch1`; `batch2`, if not null, is no longer needed */
		WindowBatchFactory.free (batch2);
		
		if (batch1 == null)
			return 0;
		
		ResultHandler handler = batch1.getQuery().getTaskDispatcher().getHandler();
		ResultCollector.forwardAndFree (handler, batch1);
		
		WindowBatchFactory.free(batch1);
		return 0;
	}
	
	public void outputWindowBatchResult (WindowBatch windowBatchResult) {
		
		batch1 = windowBatchResult; /* Control returns to run() method */
	}
	
	public void free () {
		TaskFactory.free(this);
	}
}
