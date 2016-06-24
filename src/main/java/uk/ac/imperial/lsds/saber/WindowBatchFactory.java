package uk.ac.imperial.lsds.saber;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;

public class WindowBatchFactory {
	
	private static final int _pool_size = 0; /* Initial pool size */
	
	public static AtomicLong count;
	
	private static ConcurrentLinkedQueue<WindowBatch> pool = new ConcurrentLinkedQueue<WindowBatch>();
	
	static {
		
		int i = _pool_size;
		while (i-- > 0)
			pool.add (new WindowBatch());
		
		count = new AtomicLong(_pool_size);
	}
	
	public static WindowBatch newInstance 
		(
			int size, 
			int taskId, 
			int freePointer1,
			int freePointer2,
			Query query,
			IQueryBuffer buffer, 
			WindowDefinition window, 
			ITupleSchema schema, 
			int latencyMark
		) {
		
		WindowBatch batch = pool.poll();
		if (batch == null) {
			count.incrementAndGet();
			return new WindowBatch (size, taskId, freePointer1, freePointer2, query, buffer, window, schema, latencyMark);
		}
		batch.set(size, taskId, freePointer1, freePointer2, query, buffer, window, schema, latencyMark);
		return batch;
	}
	
	public static void free (WindowBatch batch) {
		if (batch == null)
			return;
		batch.clear();
		/* The pool is ever growing based on peek demand */
		pool.offer (batch);
	}
}
