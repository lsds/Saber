package uk.ac.imperial.lsds.saber.handlers;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicIntegerArray;

import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PaddedAtomicLong;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class ResultHandler {
	
	public final int numberOfSlots = SystemConf.SLOTS;
	
	public Query query;
	
	public IQueryBuffer freeBuffer1, freeBuffer2;
	
	/*
	 * Flags:
	 *  -1: slot is free
	 *   0: slot is being populated by a thread
	 *   1: slot is occupied, but "unlocked"
	 *   2: slot is occupied, but "locked" (somebody is working on it)
	 */
	public AtomicIntegerArray slots;
	
	public PaddedAtomicLong paddedSlots [];

	/*
	 * Structures to hold the actual data
	 */
	public IQueryBuffer [] results = new IQueryBuffer [numberOfSlots];
	
	public int [] freePointers1 = new int [numberOfSlots];
	public int [] freePointers2 = new int [numberOfSlots];
	
	/* A query can have more than one downstream queries. */
	public int [] latch = new int [numberOfSlots];
	
	public int [] mark  = new int [numberOfSlots];
	
	Semaphore semaphore; /* Protects next */
	int next;
	
	private long totalOutputBytes;
	
	public ResultAggregator resultAggregator;
	
	public ResultHandler (Query query, IQueryBuffer freeBuffer1, IQueryBuffer freeBuffer2) {
		
		this.query = query;
		
		this.freeBuffer1 = freeBuffer1;
		this.freeBuffer2 = freeBuffer2;
		
		slots = new AtomicIntegerArray (numberOfSlots);
		paddedSlots = new PaddedAtomicLong [numberOfSlots];

		for (int i = 0; i < numberOfSlots; i++) {
			
			slots.set(i, -1);
            paddedSlots[i] = new PaddedAtomicLong(-1);

			freePointers1[i] = Integer.MIN_VALUE;
			freePointers2[i] = Integer.MIN_VALUE;
			
			latch[i] = 0;
			mark [i] =-1;
		}
		
		next = 0;
		semaphore = new Semaphore(1, false);
		
		totalOutputBytes = 0L;
		
		resultAggregator = null;
	}
	
	public long getTotalOutputBytes () {
		
		return totalOutputBytes;
	}
	
	public void incTotalOutputBytes (int bytes) {
		
		totalOutputBytes += ((long) bytes);
	}
	
	public void setAggregateOperator (IAggregateOperator operator) {
		System.out.println("[DBG] set aggregate operator");
		resultAggregator = new ResultAggregator (numberOfSlots, freeBuffer1, freeBuffer2, query, this);
		resultAggregator.setOperator (operator);
	}
}
