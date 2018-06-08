package uk.ac.imperial.lsds.saber.handlers;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;

import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class ResultAggregator {
	
	private static final int  FREE = -1;
	private static final int  WAIT =  0; /* A thread is populating the slot */
	private static final int  AGGR =  1; /* The slot can be aggregated with its next one */
	// private static final int  PROC =  2; /* The slot is being processed */
	private static final int READY =  3;
	private static final int  BUSY =  4; /* A thread is busy forwarding the results of this slot */
	
	/*
	 * A ResultAggregatorNode encapsulates the complete and partial window results 
	 * of a batch. Nodes are statically linked together to form a list.
	 */
	int size;
	IQueryBuffer freeBuffer1, freeBuffer2;
	Query query;
	ResultHandler handler;
	
	AtomicIntegerArray slots;
	PartialResultSlot [] nodes;
	
	Semaphore semaphore;
	// Lock lock;
	Semaphore lock;
	
	IAggregateOperator operator = null;
	
	/* Sentinel pointers */
	int nextToAggregate;
	int nextToForward;
	
	public ResultAggregator (int size, IQueryBuffer freeBuffer1, IQueryBuffer freeBuffer2, Query query, ResultHandler handler) {
		this.size = size;
		this.freeBuffer1 = freeBuffer1;
		this.freeBuffer2 = freeBuffer2;
		this.query = query;
		this.handler = handler;
		
		slots = new AtomicIntegerArray(size);
		nodes = new PartialResultSlot [size];
		
		for (int i = 0, j = i - 1; i < size; i++, j++) {
			slots.set(i, FREE);
			nodes[i] = new PartialResultSlot (i);
			if (j >= 0)
				nodes[j].connectTo(nodes[i]);
		}
		nodes[size - 1].connectTo(nodes[0]);
		
		nextToAggregate = 0;
		nextToForward   = 0;
		
		semaphore = new Semaphore(1, false);
		lock = new Semaphore(1, false);
		
		operator = null;
	}
	
	public void setOperator (IAggregateOperator operator) {
		this.operator = operator;
	}
	
	public void add (WindowBatch batch) {
		
		int taskId = batch.getTaskId();
		if (taskId < 0) /* Invalid task id */
			return ;
		
		int idx = ((taskId - 1) % size);
		while (! slots.compareAndSet(idx, FREE, WAIT)) {
			
			System.err.println(String.format("warning: result aggregator (%s) blocked: query %d task %4d slot %4d", 
					Thread.currentThread(), query.getId(), taskId, idx));
				
			LockSupport.parkNanos(1L);
		}
		
		/* Slot `idx` has been reserved for this task id */
		PartialResultSlot node = nodes[idx];
		node.init (batch);
		
		// System.out.println(node);
		
		slots.set(idx, AGGR);
		
		/* Try aggregate slots pair-wise */
		if (lock.tryAcquire()) {
			
			PartialResultSlot p;
			PartialResultSlot q;
			
			while (true) {
				p = nodes[nextToAggregate];
				q = p.next;
				
				int stateP = slots.get(nextToAggregate);
				if (stateP > AGGR)
					throw new IllegalStateException("error: inconsistent state in next result slot to aggregate");
				if (stateP < AGGR)
					break;
				
				int stateQ = slots.get(q.index);
				if (stateQ > AGGR)
					throw new IllegalStateException("error: inconsistent state in next result slot to aggregate");
				if (stateQ < AGGR)
					break;
				
				/* Both p and q nodes are ready to aggregate. */
				p.aggregate(q, operator);
				
				if (! p.isReady())
					throw new IllegalStateException("error: result slot aggregated but is not ready");
				
				nextToAggregate = q.index;
				slots.set(p.index, READY);
				
				if (q.isReady()) {
					nextToAggregate = q.next.index;
					slots.set(q.index, READY);
				}
			}
			lock.release();
		}
		
		/* Forward and free */
		
		if (! semaphore.tryAcquire())
			return;
		
		/* No other thread can enter this section */
		
		/* Is slot `nextToForward` occupied? */
		if (! slots.compareAndSet(nextToForward, READY, BUSY)) {
			semaphore.release();
			return ;
		}
		
		boolean busy = true;
		while (busy) {
			
			PartialResultSlot p = nodes [nextToForward];
			
			// System.out.println(p);
			
			IQueryBuffer buffer = p.completeWindows.getBuffer();
			byte [] arr = buffer.array();
			int length = buffer.position();
			
			/* Forward results */
			
			if (length > 0 && query.getNumberOfDownstreamQueries() > 0) {
				
				/* Forward the latency mark downstream... */
				
				if (SystemConf.LATENCY_ON && (p.mark != -1)) {
					
					long t1 = (long) Utils.getSystemTimestamp (freeBuffer1.getLong (p.mark));
					long t2 = (long) Utils.getTupleTimestamp (buffer.getLong(0));
					buffer.putLong(0, Utils.pack (t1, t2));
				}
				
				int nextQuery = p.latch;
				
				for (int q = nextQuery; q < query.getNumberOfDownstreamQueries(); ++q) {
					
					if (query.getDownstreamQuery(q) != null) {
						
						boolean success = false;
						if (query.isLeft())
							success = query.getDownstreamQuery(q).getTaskDispatcher().tryDispatchToFirstStream  (arr, length);
						else
							success = query.getDownstreamQuery(q).getTaskDispatcher().tryDispatchToSecondStream (arr, length);
						
						if (! success) {
							p.latch = q;
							slots.set(nextToForward, READY);
							semaphore.release();
							return;
						}
					}
				}
			}
			
			/* Forward to the distributed API */

			/* Measure latency */
			if (query.isMostDownstream())
				if (SystemConf.LATENCY_ON && (p.mark != -1))
					query.getLatencyMonitor().monitor(freeBuffer1, p.mark);
			
			/* 
			 * Before releasing the result buffer, increment bytes generated. It is  important 
			 * all operators set the position of the buffer accordingly. Assume that the start
			 * position is 0.
			 */
			handler.incTotalOutputBytes(length);
			
			/* Free input buffer */
			if (p.freePointer1 != Integer.MIN_VALUE)
				freeBuffer1.free (p.freePointer1);
			
			if (p.freePointer2 != Integer.MIN_VALUE)
				freeBuffer2.free (p.freePointer2);
			
			p.release ();
			
			/* Release the current slot */
			slots.set (nextToForward, FREE);
			
			/* Increment next */
			nextToForward = nextToForward + 1;
			nextToForward = nextToForward % size;
			
			/* Check if next is ready to be pushed */
			if (! slots.compareAndSet (nextToForward, READY, BUSY)) {
				busy = false;
			}
		}
		
		/* Thread exit critical section */
		semaphore.release();
	}
}