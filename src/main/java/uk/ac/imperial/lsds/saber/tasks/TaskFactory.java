package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.WindowBatch;

public class TaskFactory {
	
	private static int _pool_size = 1;
	
	public static AtomicLong count;
	
	private static ConcurrentLinkedQueue<Task> pool = new ConcurrentLinkedQueue<Task>();
	
	static {
		int i = _pool_size;
		while (i-- > 0)
			pool.add (new Task());
		count = new AtomicLong(_pool_size);
	}
	
	public static Task newInstance (int taskid, WindowBatch batch1, WindowBatch batch2) {
		Task task;
		task = pool.poll();
		if (task == null) {
			count.incrementAndGet();
			return new Task(taskid, batch1, batch2);
		}
		task.set(taskid, batch1, batch2);
		return task;
	}
	
	public static void free (Task task) {
		/* The pool is ever growing based on peek demand */
		pool.offer (task);
	}
}
