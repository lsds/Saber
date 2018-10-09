package uk.ac.imperial.lsds.saber.buffers;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.devices.TheCPU;

import java.nio.ByteBuffer;

public class LRBRewriterInCircular implements Runnable {


	private CircularQueryBuffer circularBuffer;
	volatile boolean started = false;
	private final int id;
    private final int coreToBind;
	public int value = 0;
	private ByteBuffer helper;

	public LRBRewriterInCircular (CircularQueryBuffer circularBuffer, int id, int coreToBind) {
		this.circularBuffer = circularBuffer;
		this.id = id;
		this.coreToBind = coreToBind;
	}

	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {

	}

	@Override
	public void run() {

		TheCPU.getInstance().bind(coreToBind);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, coreToBind));

		int curr;
		int prev = -1;
		long timestamp;

        int step = SystemConf.BATCH_SIZE/this.circularBuffer.numberOfThreads; // power of two
        long timeBase = (long)(id-1)*step/this.circularBuffer.dataLength;

		started = true;

		while (true) {

			while ( (curr = this.circularBuffer.isReady.get()) == prev)
				Thread.yield();

			/* Fill buffer... */
			int startPos = this.circularBuffer.globalIndex + (id - 1)*step;
			int endPos = startPos + step;

			int startIndex = (id - 1)*step;

            timestamp = this.circularBuffer.timestamp + timeBase;

            int size = this.circularBuffer.size;

			ByteBuffer buffer = this.circularBuffer.getByteBuffer().duplicate();

			//System.out.println("id " + id + ", startPos " + startPos + ", endPos " + endPos + ", timestamp " + timestamp + ", timeBase " + timeBase);

			if (step > (size - startPos)) {
				System.err.println("error: corner case");
				int right = size - startPos;
				int left  = step - (size - startPos);

				System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, right);
				System.arraycopy(this.circularBuffer.inputBuffer, size - startPos, buffer.array(), 0, left);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), right, size, timestamp);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);

			} else {
			    TheCPU.getInstance().changeTimestamps(buffer, startPos, endPos, this.circularBuffer.dataLength, timestamp);
				//System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, step);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);
			}

			prev = curr;
			this.circularBuffer.isBufferFilledLatch.decReaders();
		}
	}
}
