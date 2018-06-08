package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.devices.TheCPU;

public class CircularBufferWorker implements Runnable {
	
	private CircularQueryBuffer circularBuffer;
	volatile boolean started = false;
	private final int id;
	public int value = 0;
	private ByteBuffer helper;

	public CircularBufferWorker (CircularQueryBuffer circularBuffer, int id) {
		this.circularBuffer = circularBuffer;
		this.id = id;
	}
	
	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {
		
	}
	
	@Override
	public void run() {
		
		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, id));

		int curr;
		int prev = -1;
		long timestamp;
		
		started = true;
		
		while (true) {
			
			while ( (curr = this.circularBuffer.isReady.get()) == prev)
				Thread.yield();						
			
			/* Fill buffer... */
			timestamp = this.circularBuffer.timestamp;
			int step = this.circularBuffer.globalLength; // power of two
			int startPos = this.circularBuffer.globalIndex + (id - 1)*step;
			int endPos = startPos + step;
			
			int startIndex = (id - 1)*step;
			
			int size = this.circularBuffer.size;
			
			if (started) {
				helper = ByteBuffer.allocate(32*32768);
				started = false;
			}

			ByteBuffer buffer = this.circularBuffer.getByteBuffer().duplicate();
			if (step > (size - startPos)) {
				int right = size - startPos;
				int left  = step - (size - startPos);
				
				System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, right);
				System.arraycopy(this.circularBuffer.inputBuffer, size - startPos, buffer.array(), 0, left);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), right, size, timestamp);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);

			} else {
				System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, step);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);
			}
			
			prev = curr;
			this.circularBuffer.isBufferFilledLatch.decReaders();
		}
	}
}
