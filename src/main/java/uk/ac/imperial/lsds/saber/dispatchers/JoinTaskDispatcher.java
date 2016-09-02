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
// import uk.ac.imperial.lsds.saber.monetdb.MonetDBExperimentalSetup;
import uk.ac.imperial.lsds.saber.tasks.Task;
import uk.ac.imperial.lsds.saber.tasks.TaskFactory;
import uk.ac.imperial.lsds.saber.tasks.TaskQueue;

public class JoinTaskDispatcher implements ITaskDispatcher {
	
	private TaskQueue workerQueue;
	
	private Query parent;
	
	private IQueryBuffer firstBuffer;
	private IQueryBuffer secondBuffer;
	
	private WindowDefinition firstWindow;
	private ITupleSchema firstSchema;
	
	private int firstTupleSize;
	
	private WindowDefinition secondWindow;
	private ITupleSchema secondSchema;
	
	private int secondTupleSize;
	
	private ResultHandler handler;
	
	private int batchSize;
	
	private int nextTask = 1;
	
	private int firstStartIndex      = 0;
	private int firstNextIndex       = 0;
	private int firstEndIndex        = 0;
	private int firstLastEndIndex    = 0;
	private int firstToProcessCount  = 0;
	
	private long firstNextTime;
	private long firstEndTime;
	
	private int secondStartIndex     = 0;
	private int secondNextIndex      = 0;
	private int secondLastEndIndex   = 0;
	private int secondEndIndex       = 0;
	private int secondToProcessCount = 0;
	
	private long secondNextTime;
	private long secondEndTime;
	
	private int mask;
	
	private int latencyMark = -1;
	
	private Object lock = new Object();

	public JoinTaskDispatcher (Query query) {
		
		parent = query;
		
		firstBuffer  = new CircularQueryBuffer(parent.getId(), SystemConf.CIRCULAR_BUFFER_SIZE, false);
		secondBuffer = new CircularQueryBuffer(parent.getId(), SystemConf.CIRCULAR_BUFFER_SIZE, false);
		
		firstWindow = parent.getFirstWindowDefinition();
		firstSchema = parent.getFirstSchema();
		
		firstTupleSize = firstSchema.getTupleSize();
		
		secondWindow = parent.getSecondWindowDefinition();
		secondSchema = parent.getSecondSchema();
		
		secondTupleSize = secondSchema.getTupleSize();
		
		handler = null;
		
		firstLastEndIndex = -firstTupleSize;
		firstEndIndex = -firstTupleSize;
		
		secondLastEndIndex = -secondTupleSize;
		
		/* The assumption is that both first and second buffer are of the same size */
		mask = firstBuffer.capacity() - 1;
		
		batchSize = parent.getQueryConf().getBatchSize();
		
		workerQueue = null;
	}
	
	public void setup () {
		handler = new ResultHandler (parent, firstBuffer, secondBuffer);
		workerQueue = parent.getExecutorQueue();
	}
	
	public void dispatch (byte [] data, int length) {
		int idx;
		while ((idx = firstBuffer.put(data, length)) < 0) {
			Thread.yield();
		}
		assembleFirst (idx, length);
	}
	
	public boolean tryDispatch (byte [] data, int length) {
		int idx;
		if ((idx = firstBuffer.put(data, length)) < 0) {
			return false;
		}
		assembleFirst (idx, length);
		return true;
	}
	
	public void dispatchToFirstStream (byte [] data, int length) {
		dispatch (data, length);
	}
	
	public boolean tryDispatchToFirstStream (byte [] data, int length) {
		return tryDispatch (data, length);
	}
	
	public void dispatchToSecondStream (byte [] data, int length) {
		int idx;
		while ((idx = secondBuffer.put(data, length)) < 0) {
			Thread.yield();
		}
		assembleSecond (idx, length);
	}
	
	public boolean tryDispatchToSecondStream (byte [] data, int length) {
		int idx;
		if ((idx = secondBuffer.put(data, length)) < 0) {
			return false;
		}
		assembleSecond (idx, length);
		return true;
	}
	
	public IQueryBuffer getBuffer () {
		return firstBuffer;
	}
	
	public IQueryBuffer getFirstBuffer () {
		return getBuffer();
	}

	public IQueryBuffer getSecondBuffer () {
		return secondBuffer;
	}
	
	public long getBytesGenerated () {
		return handler.getTotalOutputBytes();
	}
	
	public ResultHandler getHandler () {
		return handler;
	}
	
	public void setAggregateOperator (IAggregateOperator operator) {
		throw new IllegalStateException ("error: cannot set aggregate operator of a join task dispatcher");
	}
	
	private void assembleFirst (int idx, int length) {
		
		// System.out.println(String.format("[DBG] assemble 1: idx %10d length %10d", idx, length));
		
		if (SystemConf.LATENCY_ON) {
			if (latencyMark < 0)
				latencyMark = idx;
		}
		
		firstEndIndex = idx + length - firstTupleSize;
		
		synchronized (lock) {
			
			if (firstEndIndex < firstStartIndex)
				firstEndIndex += firstBuffer.capacity();
			
			firstToProcessCount = 
					(firstEndIndex - firstStartIndex + firstTupleSize) / firstTupleSize;
					
			/* 
			 * Check whether we have to move the pointer that indicates the oldest window 
			 * in this buffer that has not yet been closed. If we grab the data to create 
			 * a task, the start pointer will be set to this next pointer.
			 * 
			 */
			if (firstWindow.isRowBased()) {
				
				while ((firstNextIndex + firstWindow.getSize() * firstTupleSize) < firstEndIndex) {
					
					firstNextIndex += firstTupleSize * firstWindow.getSlide();
				}
			
			} else 
			if (firstWindow.isRangeBased()) {
				
				firstNextTime = getTimestamp (firstBuffer, firstNextIndex);
				firstEndTime  = getTimestamp (firstBuffer,  firstEndIndex);
				
				while ((firstNextTime + firstWindow.getSize()) < firstEndTime) {
					
					firstNextIndex += firstTupleSize;
					firstNextTime = getTimestamp (firstBuffer, firstNextIndex);
				}
				
			} else {
				throw new UnsupportedOperationException("error: window is neither row-based nor range-based");
			}
			
			/* Check whether we have enough data to create a task */
			int size = 
					( firstToProcessCount *  firstTupleSize) + 
					(secondToProcessCount * secondTupleSize);
			
			if (size >= batchSize)
				createTask (true);
		}
	}
	
	private void assembleSecond (int idx, int length) {
		
		// System.out.println(String.format("[DBG] assemble 2: idx %10d length %10d", idx, length));
		
		secondEndIndex = idx + length - secondTupleSize;
		
		synchronized (lock) {
			
			if (secondEndIndex < secondStartIndex)
				secondEndIndex += secondBuffer.capacity();
			
			secondToProcessCount = 
					(secondEndIndex - secondStartIndex + secondTupleSize) / secondTupleSize;
					
			if (secondWindow.isRowBased()) {
				
				while ((secondNextIndex + secondWindow.getSize() * secondTupleSize) < secondEndIndex) {
					
					secondNextIndex += secondTupleSize * secondWindow.getSlide();
				}
			
			} else 
			if (secondWindow.isRangeBased()) {
				
				secondNextTime = getTimestamp (secondBuffer, secondNextIndex);
				secondEndTime  = getTimestamp (secondBuffer,  secondEndIndex);
				
				while ((secondNextTime + secondWindow.getSize()) < secondEndTime) {
					
					secondNextIndex += secondTupleSize;
					secondNextTime = getTimestamp(secondBuffer, secondNextIndex);
				}
				
			} else {
				throw new UnsupportedOperationException("error: window is neither row-based nor range-based");
			}
			
			/* Check whether we have enough data to create a task */
			int size = 
					( firstToProcessCount *  firstTupleSize) + 
					(secondToProcessCount * secondTupleSize);
			
			if (size >= this.batchSize)
				createTask (false);
		}
	}
	
	private static int normaliseIndex (IQueryBuffer buffer, int p, int q) {
		if (q < p)
			return (q + buffer.capacity());
		return q;
	}
	
	private void createTask (boolean assembledFirst) {
		
		int taskid = this.getTaskNumber();
		/*
		if (MonetDBExperimentalSetup.enabled) {
			if (taskid == 1) {
				MonetDBExperimentalSetup.startTime = System.nanoTime();
				System.out.println("[DBG] MonetDB comparison experiment starts...");
			}
		}
		*/
		// if (taskid > 6)
		//	return;
		
		int  firstFreePointer = Integer.MIN_VALUE;
		int secondFreePointer = Integer.MIN_VALUE;
		
		if (firstNextIndex != firstStartIndex)
			firstFreePointer = (firstNextIndex - firstTupleSize) & mask;
		
		if (secondNextIndex != secondStartIndex)
			secondFreePointer = (secondNextIndex - secondTupleSize) & mask;
		
		/* Find latency mark */
		int mark = -1;
		if (SystemConf.LATENCY_ON) {
			if (latencyMark >= 0) {
				mark = latencyMark;
				latencyMark = -1;
			}
		}
		
		WindowBatch batch1 = WindowBatchFactory.newInstance
			(
				this.batchSize, 
				taskid, 
				firstFreePointer,
				secondFreePointer,
				parent,
				firstBuffer, 
				firstWindow, 
				firstSchema, 
				mark
			);
		
		WindowBatch batch2 = WindowBatchFactory.newInstance
			(
				this.batchSize, 
				taskid, 
				Integer.MIN_VALUE,
				Integer.MIN_VALUE,
				parent,
				secondBuffer, 
				secondWindow, 
				secondSchema, 
				-1
			);
		
		if (assembledFirst) {
			
			batch1.setBufferPointers 
				(
					firstLastEndIndex + firstTupleSize, 
					normaliseIndex (firstBuffer, firstLastEndIndex, firstEndIndex)
				);
			
			batch2.setBufferPointers 
				(
					secondStartIndex, 
					normaliseIndex (secondBuffer, secondStartIndex, secondEndIndex)
				);
			
		} else {
			
			batch1.setBufferPointers
				(
					firstStartIndex, 
					normaliseIndex (firstBuffer, firstStartIndex, firstEndIndex)
				);
			
			batch2.setBufferPointers 
				(
					secondLastEndIndex + secondTupleSize, 
					normaliseIndex (secondBuffer, secondLastEndIndex, secondEndIndex)
				);
		}
		
		// System.out.println(String.format("[DBG] dispatch task %d batch-1 [%6d,%6d] batch-2 [%6d, %6d]",
		// 		taskid, 
		// 		batch1.getBufferStartPointer(), batch1.getBufferEndPointer(), 
		// 		batch2.getBufferStartPointer(), batch2.getBufferEndPointer()
		// ));
		
		firstLastEndIndex  =  firstEndIndex;
		secondLastEndIndex = secondEndIndex;
		
		Task task = TaskFactory.newInstance(taskid, batch1, batch2);

		workerQueue.add(task);
		
		/*
		 * First, reduce the number of tuples that are ready for processing by the number 
		 * of tuples that are fully processed in the task that was just created.
		 */
		if (firstNextIndex != firstStartIndex)
			firstToProcessCount -= (firstNextIndex - firstStartIndex) / firstTupleSize;
		
		if (secondNextIndex != secondStartIndex)
			secondToProcessCount -= (secondNextIndex - secondStartIndex) / secondTupleSize;
		
		/*
		 * Second, move the start pointer for the next task to the next pointer.
		 */
		if (firstNextIndex > mask)
			firstNextIndex = firstNextIndex & mask;
		
		if (secondNextIndex > mask)
			secondNextIndex = secondNextIndex & mask;
		
		 firstStartIndex =  firstNextIndex;
		secondStartIndex = secondNextIndex;
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

