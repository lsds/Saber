package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.utils;

import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class LRBGeneratorWorker implements Runnable {
	
	LRBGenerator generator;
	volatile boolean started = false;
	
	private int isFirstTime = 2;
	private final int dataRange;
	private final int startPos;
	private final int endPos;
	private final int id;
    private Random rand;

    public LRBGeneratorWorker(LRBGenerator generator, int startPos, int endPos, int id, int dataRange) {
		this.generator = generator;
		this.dataRange = dataRange;
		this.startPos = startPos;
		this.endPos = endPos;
		this.id = id;
        this.rand = new Random(dataRange);
    }
	
	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {
		
	}
	
	@Override
	public void run() {
		
		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker LRBGenerator thread %2d to core %2d", id, id));

		int curr;
		GeneratedBuffer buffer;
		int prev = 0;
		long timestamp;
		
		started = true;
		
		while (true) {
			
			while ((curr = generator.next) == prev)
				;
			
			// System.out.println("Filling buffer " + curr);
			
			buffer = generator.getBuffer (curr);
			
			/* Fill buffer... */
			timestamp = generator.getTimestamp ();
			generate(buffer, startPos, endPos, timestamp);
			
			buffer.decrementLatch ();
			prev = curr;
			// System.out.println("done filling buffer " + curr);
			// break;
		}
		// System.out.println("worker exits " );
	}
	
	private void generate(GeneratedBuffer generatedBuffer, int startPos, int endPos, long timestamp) {
		ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		/* Fill the buffer */
		
		if (isFirstTime!=0 ) {
			int value = 0;

			buffer.position(startPos);
			while (buffer.position()  < endPos) {

			    buffer.putLong (timestamp);
			    //buffer.putInt((this.rand.nextInt() & Integer.MAX_VALUE) % dataRange); // vehicle
				//buffer.putInt(value % dataRange);
				buffer.putInt(value % 4);
                buffer.putFloat((float) value);                 // speed
                buffer.putInt(0);                               // highway
                buffer.putInt(0);                               // lane
                buffer.putInt(0);                               // direction
                buffer.putInt(value);                           // position

				// buffer padding
				// buffer.position(buffer.position() + 60);
				value ++;
			}
			isFirstTime --;
		} else {
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
				// change the timestamp
			    buffer.putLong (timestamp);
				// skip the rest of the values
				buffer.position(buffer.position() + 24);
			}
		}	
	}
}
