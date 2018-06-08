package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Latch {

	AtomicInteger latch;
	
	public Latch (int counter) {
		latch = new AtomicInteger(counter);
	}
	
    public void setLatch (int counter) {
		latch = new AtomicInteger(counter);
    }
	
	/* The method blocks until all writers have finished
     * (the model is write-locked when readers < 0).
     */
    public int incReaders () {
        int val = (int) latch.get();
        while ((val < 0) || (! latch.compareAndSet(val, val + 1)))
            val = latch.get();
        return val + 1;
    }
    
    /* Eventually the counter reaches 0, at which point 
     * the model becomes available to writers.
     *
     * The counter keeps decreasing based on the number
     * of writers.
     */
    public int decReaders () {
        int val = latch.get();
        while (! latch.compareAndSet(val, val - 1))
            val = latch.get();
        return val - 1;
    }
    
    public int getCount () {
    	return latch.get();
    }
}
