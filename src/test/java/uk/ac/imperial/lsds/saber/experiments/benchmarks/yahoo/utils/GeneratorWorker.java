package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.devices.TheCPU;


public class GeneratorWorker implements Runnable {
	
	Generator generator;
	volatile boolean started = false;
	
	private int isFirstTime = 2;
	private ByteBuffer bufferHelper;
	private final int adsPerCampaign;
	private final long [][] ads;
	private final int startPos;
	private final int endPos;
	private final int id;
	
	private boolean isV2 = false;
	
	public GeneratorWorker (Generator generator, int startPos, int endPos, int id) {
		this(generator, startPos, endPos, id, false);
	}
	public GeneratorWorker (Generator generator, int startPos, int endPos, int id, boolean isV2) {
		this.generator = generator;
		this.adsPerCampaign = generator.getAdsPerCampaign();
		this.ads = generator.getAds();
		this.startPos = startPos;
		this.endPos = endPos;
		this.id = id;
		
		this.isV2 = isV2;
		
		bufferHelper = ByteBuffer.allocate(32);
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
			if (isV2)
				generateV2(buffer, startPos, endPos, timestamp);  
			else
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
		/* Fill the buffer */	
		
		if (isFirstTime!=0 ) {
			UUID user_id = UUID.randomUUID(); 
			UUID page_id = UUID.randomUUID();
			int value = 0;
			
			bufferHelper.clear();
			bufferHelper.putLong(user_id.getMostSignificantBits());                            // user_id
			bufferHelper.putLong(user_id.getLeastSignificantBits());
			bufferHelper.putLong(page_id.getMostSignificantBits());                            // page_id
			bufferHelper.putLong(page_id.getLeastSignificantBits());
			
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
	
			    buffer.putLong (timestamp);		    
			    buffer.put(bufferHelper.array());
				buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][0]); // ad_id
				buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][1]);			
				buffer.putInt((value % 100000) % 5);                                         // ad_type: (0, 1, 2, 3, 4) => 
				                                                                             // ("banner", "modal", "sponsored-search", "mail", "mobile")
				buffer.putInt((value % 100000) % 3);                                         // event_type: (0, 1, 2) => 
																							 // ("view", "click", "purchase")
				
				buffer.putInt(1);                                                            // ip_address
				
				// buffer padding
				buffer.position(buffer.position() + 60);
				value ++;
			}
			isFirstTime --;
		} else {
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
	
			    buffer.putLong (timestamp);
				
				// buffer padding
				buffer.position(buffer.position() + 120);
			}
		}	
	}

	private void generateV2(GeneratedBuffer generatedBuffer, int startPos, int endPos, long timestamp) {	
		ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		/* Fill the buffer */	
		
		if (isFirstTime!=0 ) {
			long user_id = 0L; 
			long page_id = 0L;
			int value = 0;
			
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
			    buffer.putLong (timestamp);		    
				buffer.putLong(user_id);                            
				buffer.putLong(page_id);				buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][1]);			
				buffer.putInt((value % 100000) % 5);
				buffer.putInt((value % 100000) % 3);                                 
				buffer.putInt(1);                                                            
				// buffer padding
				buffer.position(buffer.position() + 20);
				value ++;
			}
			isFirstTime --;
		} else {
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
			    buffer.putLong (timestamp);
				// buffer padding
				buffer.position(buffer.position() + 56);
			}
		}	
	}

}
