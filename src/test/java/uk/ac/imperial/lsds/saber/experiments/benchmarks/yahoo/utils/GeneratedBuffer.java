package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class GeneratedBuffer {
	
	ByteBuffer buffer;
	CountDownLatch latch;
	Latch bufferFilledLatch, bufferReadLatch;
	
	public GeneratedBuffer (int capacity, boolean direct, int numberOfThreads) {
		buffer = (direct) ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
		bufferFilledLatch = new Latch(numberOfThreads);
		bufferReadLatch = new Latch(1);
	}
	
	public ByteBuffer getBuffer() {
		return buffer;
	}
	
	public boolean isDirect () {
		return buffer.isDirect();
	}

	public boolean isFilled () {
		/* Latch is zero */
		while (bufferFilledLatch.getCount() != 0)
			;//Thread.yield();
		return true;
	}

	public GeneratedBuffer lock () {
		bufferReadLatch.decReaders();
		return this;
	}
	
	public void unlock() {
		bufferReadLatch.incReaders();
	}

	public boolean isLocked() {
		return (bufferReadLatch.getCount() == 0);
	}

	public void setLatch(int count) {
		/* Set latch to count */
		bufferFilledLatch.setLatch(count);
		// System.out.println("Latch is set to: " + bufferFilledLatch.getCount());
	}

	public void decrementLatch () {		
		bufferFilledLatch.decReaders();
		// System.out.println("Latch is decreased to: " + bufferFilledLatch.getCount());
	}
}
