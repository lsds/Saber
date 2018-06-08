package uk.ac.imperial.lsds.saber.dispatchers;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowBatchFactory;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.CircularQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.handlers.ResultHandler;
import uk.ac.imperial.lsds.saber.tasks.Task;
import uk.ac.imperial.lsds.saber.tasks.TaskFactory;
import uk.ac.imperial.lsds.saber.tasks.TaskQueue;

public class TaskDispatcher implements ITaskDispatcher {
	
	private TaskQueue workerQueue;
	
	private IQueryBuffer buffer;
	
	private WindowDefinition window;
	private ITupleSchema schema;
	
	private ResultHandler handler;
	
	private Query parent;
	
	private int batchSize;
	private int tupleSize;
	
	/* Task Identifier */
	private int nextTask = 1;
	
	/* Pointers */
	private long f;
	private long mask;
	
	private int latencyMark;
	
	private long accumulated = 0;
	
	private long thisBatchStartPointer;
	private long nextBatchEndPointer;
	
	public TaskDispatcher (Query query) {
		
		parent = query;
		
		buffer = new CircularQueryBuffer(parent.getId(), SystemConf.CIRCULAR_BUFFER_SIZE, false);
		
		window = this.parent.getWindowDefinition ();
		schema = this.parent.getSchema ();
		
		handler = null;
		
		batchSize = parent.getQueryConf().getBatchSize();
		
		tupleSize = schema.getTupleSize();
		
		/* Initialize constants */
		System.out.println(String.format("[DBG] %d bytes/batch %d panes/slide %d panes/window", 
				batchSize, window.panesPerSlide(), window.numberOfPanes()));
		
		mask = buffer.capacity() - 1;
		
		latencyMark = -1;
		
		thisBatchStartPointer = 0;
		nextBatchEndPointer = batchSize;
		
		workerQueue = null;
	}
	
	public void setup () {
		handler = new ResultHandler (parent, buffer, null);
		/* The single, system-wide task queue for either CPU or GPU tasks */
		workerQueue = parent.getExecutorQueue();
	}
	
	public void dispatch (byte [] data, int length) {
		int idx;
		while ((idx = buffer.put(data, length)) < 0) {
			Thread.yield();
		}
		assemble (idx, length);
	}
	
	public boolean tryDispatch (byte [] data, int length) {
		int idx;
		if ((idx = buffer.put(data, length)) < 0) {
			return false;
		}
		assemble (idx, length);
		return true;
	}
	
	public void dispatchToFirstStream (byte[] data, int length) {
		dispatch (data, length);
	}
	
	public boolean tryDispatchToFirstStream (byte[] data, int length) {
		return tryDispatch (data, length);
	}
	
	public void dispatchToSecondStream (byte [] data, int length) {
		
		throw new UnsupportedOperationException("error: cannot dispatch to a second stream buffer");
	}

	public boolean tryDispatchToSecondStream (byte[] data, int length) {
		
		throw new UnsupportedOperationException("error: cannot dispatch to a second stream buffer");
	}
	
	public IQueryBuffer getBuffer () {
		return buffer;
	}
	
	public IQueryBuffer getFirstBuffer () {
		return getBuffer();
	}
	
	public IQueryBuffer getSecondBuffer () {
		return null;
	}
	
	public long getBytesGenerated () {
		return handler.getTotalOutputBytes();
	}
	
	public ResultHandler getHandler () {
		return handler;
	}

	public void setAggregateOperator (IAggregateOperator operator) {
		handler.setAggregateOperator(operator);
	}
	
	private void assemble (int index, int length) {
		
		if (SystemConf.LATENCY_ON) {
			if (latencyMark < 0)
				latencyMark = index;
		}
		
		accumulated += (length);
		
		if ((! window.isRangeBased()) && (! window.isRowBased())) {
			throw new UnsupportedOperationException("error: window is neither row-based nor range-based");
		}
			
		while (accumulated >= nextBatchEndPointer) {
				
			f = nextBatchEndPointer & mask;
			f = (f == 0) ? buffer.capacity() : f;
			f--;
			/* Launch task */
			this.newTaskFor (
				thisBatchStartPointer & mask, 
				nextBatchEndPointer   & mask, 
				f, 
				thisBatchStartPointer, 
				nextBatchEndPointer
			);
			
			thisBatchStartPointer += batchSize;
			nextBatchEndPointer   += batchSize;
		}
	}
	
	private void newTaskFor (long p, long q, long free, long b_, long _d) {
		
		Task task;
		WindowBatch batch;
		
		int taskid;
		
		taskid = this.getTaskNumber();
		
		// Debugging
		//
		// long size = 0;
		// size = (q <= p) ? (q + buffer.capacity()) - p : q - p;
		// System.out.println(
		// 	String.format("[DBG] Query %d task %6d [%10d, %10d), free %10d, [%6d, %6d] size %10d", 
		//			parent.getId(), taskid, p, q, free, b_, _d, size));
		
		if (q <= p) {
			q += buffer.capacity();
		}
		
		/* Find latency mark */
		int mark = -1;
		if (SystemConf.LATENCY_ON) {
			if (latencyMark >= 0) {
				mark = latencyMark;
				latencyMark = -1;
			}
		}
		
		/* Update free pointer */
		free -= schema.getTupleSize();
		if (free < 0) {
			System.err.println(String.format("error: negative free pointer (%d) for query %d", free, parent.getId()));
			System.exit(1);
		}
		
		batch = WindowBatchFactory.newInstance (batchSize, taskid, (int) (free), Integer.MIN_VALUE, parent, buffer, window, schema, mark);
		
		if (window.isRangeBased()) {
			long startTime = getTimestamp(buffer, (int) (p));
			long endTime = getTimestamp(buffer, (int) (q - tupleSize));
			batch.setBatchTimestamps(startTime, endTime);
		} else {
			batch.setBatchTimestamps(-1, -1);
		}
		
		batch.setBufferPointers((int) p, (int) q);
		batch.setStreamPointers (b_, _d);
		
		task = TaskFactory.newInstance(taskid, batch, null);
		
		workerQueue.add(task);
	}
	
	private int getTaskNumber () {
		int id = nextTask ++;
		if (nextTask == Integer.MAX_VALUE)
			nextTask = 1;
		return id;
	}
	
	private long getTimestamp (IQueryBuffer buffer, int index) {
		long value = buffer.getLong(index);
		if (SystemConf.LATENCY_ON)
			return (long) Utils.getTupleTimestamp(value);
		else 
			return value;
	}
}
