package uk.ac.imperial.lsds.saber.handlers;

import java.util.concurrent.locks.LockSupport;

import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
// import uk.ac.imperial.lsds.saber.monetdb.MonetDBExperimentalSetup;

public class ResultCollector {
	
	public static void forwardAndFree (ResultHandler handler, WindowBatch batch1) {
		
		if (batch1.containsFragmentedWindows()) {
			
			if (handler == null) {
				System.err.println("Null handler...");
				System.exit(1);
			}
			if (handler.resultAggregator == null) {
				System.err.println("Null aggregator...");
				System.exit(1);
			}
			handler.resultAggregator.add(batch1);
			
		} else {
		
			int taskid = batch1.getTaskId();
			
			Query query = batch1.getQuery();
			
			IQueryBuffer results = batch1.getBuffer();
			
			int freePtr1 =  batch1.getFirstFreePointer();
			int freePtr2 = batch1.getSecondFreePointer();
			
			int mark = batch1.getLatencyMark();
			
			forwardAndFree (taskid, handler, query, results, freePtr1, freePtr2, mark);
		}
	}
	
	public static void forwardAndFree (
			int taskid, 
			ResultHandler handler,
			Query query,
			IQueryBuffer results, 
			int freePtr1, 
			int freePtr2,
			int mark
		) {
		
		if (taskid < 0) { /* Invalid task id */
			return;
		}
		
		int idx = ((taskid - 1) % handler.numberOfSlots);
		
		try {
			
			while (! handler.slots.compareAndSet(idx, -1, 0)) {

				System.err.println(String.format("warning: result collector (%s) blocked: query %d task %4d slot %4d", 
						Thread.currentThread(), query.getId(), taskid, idx));
				
				LockSupport.parkNanos(1L);
			}
			
			handler.freePointers1[idx] = freePtr1;
			handler.freePointers2[idx] = freePtr2;
			
			handler.results[idx] = results;

			handler.latch [idx] = 0;
			handler.mark  [idx] = mark;
			
			/* No other thread can modify this slot. */
			handler.slots.set(idx, 1);
			
			/* Forward and free */

			if (! handler.semaphore.tryAcquire())
				return;

			/* No other thread can enter this section */

			/* Is slot `next` occupied? */
			if (! handler.slots.compareAndSet(handler.next, 1, 2)) {
				handler.semaphore.release();
				return;
			}
			
			boolean busy = true;
			while (busy) {
				
				IQueryBuffer buffer = handler.results[handler.next];
				byte [] arr = buffer.array();
				int length = buffer.position();
				
				/* Forward results */
				
				if (length > 0 && query.getNumberOfDownstreamQueries() > 0) {
					
					/* Forward the latency mark downstream... */
					
					if (SystemConf.LATENCY_ON && (handler.mark[handler.next] != -1)) {
						
						long t1 = (long) Utils.getSystemTimestamp (handler.freeBuffer1.getLong (handler.mark[handler.next]));
						long t2 = (long) Utils.getTupleTimestamp (buffer.getLong(0));
						buffer.putLong(0, Utils.pack (t1, t2));
					}
					
					int nextQuery = handler.latch[handler.next];
					
					for (int q = nextQuery; q < query.getNumberOfDownstreamQueries(); ++q) {
						
						if (query.getDownstreamQuery(q) != null) {
							
							boolean success = false;
							if (query.isLeft())
								success = query.getDownstreamQuery(q).getTaskDispatcher().tryDispatchToFirstStream  (arr, length);
							else
								success = query.getDownstreamQuery(q).getTaskDispatcher().tryDispatchToSecondStream (arr, length);
							
							if (! success) {
								// System.out.println("[DBG] failed to forward results to next query...");
								handler.latch[handler.next] = q;
								handler.slots.set(handler.next, 1);
								handler.semaphore.release();
								return;
							}
						}
					}
				}
				
				/* Forward to the distributed API */

				/* Measure latency */
				if (query.isMostDownstream()) {
					if (SystemConf.LATENCY_ON && (handler.mark[handler.next] != -1)) {
						query.getLatencyMonitor().monitor(handler.freeBuffer1, handler.mark[handler.next]);
					}
				}
				
				/* 
				 * Before releasing the result buffer, increment bytes generated. It is  important 
				 * all operators set the position of the buffer accordingly. Assume that the start
				 * position is 0.
				 */
				handler.incTotalOutputBytes(length);
				buffer.release();
				
				/* Free input buffer */
				int f1, f2;
				
				f1 = handler.freePointers1[handler.next];
				if (f1 != Integer.MIN_VALUE)
					handler.freeBuffer1.free (f1);
				
				f2 = handler.freePointers2[handler.next];
				if (f2 != Integer.MIN_VALUE)
					handler.freeBuffer2.free (f2);
				
				/* Release the current slot */
				handler.slots.set(handler.next, -1);
				
				/*
				if (MonetDBExperimentalSetup.enabled) {
					if (handler.next == MonetDBExperimentalSetup.numberOfTasks - 1) {
						long dt = System.nanoTime() - MonetDBExperimentalSetup.startTime;
						System.out.println("[DBG] MonetDB comparison experiment run in " + ((double) dt / 1000000.) + " msec");
						System.out.println(String.format("[DBG] %d output bytes", handler.getTotalOutputBytes()));
					}
				}
				*/
				
				/* Increment next */
				handler.next = handler.next + 1;
				handler.next = handler.next % handler.numberOfSlots;
				
				/* Check if next is ready to be pushed */

				if (! handler.slots.compareAndSet(handler.next, 1, 2)) {
					busy = false;
				}
			}
			/* Thread exit critical section */
			handler.semaphore.release();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
