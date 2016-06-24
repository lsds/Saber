package uk.ac.imperial.lsds.saber.microbenchmarks;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import uk.ac.imperial.lsds.saber.SystemConf;

/*
 * Executed once to compress data set
 */
public class DemoWithGoogleClusterDataPreprocessor {
	
	public static final String usage = "usage: DemoWithGoogleClusterDataPreprocessor";

	public static void main (String [] args) throws Exception {
		
		String dataDir = SystemConf.SABER_HOME + "/datasets/google-cluster-data/";
		
		String outputFile = "saber-debs-demo.data";
		
		int tupleSize = 64;
		
		int ATTRIBUTE_OFFSET = 36;
		
		int tuplesPerInsert = 16384; /* x64 = 1MB */
		
		/* Parse command line arguments */
		
		int i, j;
		
		for (i = 0; i < args.length; ) {
			
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			
			if (args[i].equals("--data-dir")) {
				
				dataDir = args[j];
			} else
			if (args[i].equals("--data-dir")) {
					
				outputFile = args[j];
			} else {
				
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
		/* Load attributes from files
		 * 
		 * There are 144,370,688 events in the dataset.
		 * 
		 * A 1MB task contains 16,384 events (a tuple is 64 bytes).
		 * 
		 * The dataset fits in 8812 tasks. The last task is short
		 * by 5120 events.
		 * 
		 * We appended (5120 x 64 =) 327,680 bytes to the stream to 
		 * complete the last task.
		 * 
		 */
		
		int MAX_LINES = 144370688;
		int numberOfTasks =  8812;
		
		int taskSize = 1048576;
		int tailSize =  327680;
		
		ByteBuffer [] data = new ByteBuffer [numberOfTasks];
		for (i = 0; i < numberOfTasks; i++)
			data[i] = ByteBuffer.allocate(taskSize);
		
		ByteBuffer buffer; /* Pointer to byte buffer array `data` */
		
		/* Initialise buffers */
		
		for (i = 0; i < numberOfTasks; i++) {
			
			buffer = data[i];
			
			buffer.clear(); /* Reset position to 0. */
			
			while (buffer.hasRemaining()) {
				
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putInt   (1);
				
				/* Attributes of interest start at attribute offset is 36 */
				buffer.putInt   (1); /* Event type */
				buffer.putInt   (1); /* Category   */
				buffer.putInt   (1); /* Priority   */
				buffer.putFloat (1); /* CPU usage  */
				
				buffer.putFloat (1);
				buffer.putFloat (1);
				buffer.putInt   (1);
			}
		}
		
		String [] filenames = {
			dataDir + "norm-event-types.txt",
			dataDir +       "categories.txt",
			dataDir +       "priorities.txt",
			dataDir +  "cpu-utilisation.txt",
		};
		
		boolean [] containsInts = { true, true, true, false };
		
		FileInputStream f;
		DataInputStream d;
		BufferedReader  b;
		
		String line = null;
		long lines = 0;
		
		long percent_, _percent;
		
		int bufferIndex, tupleIndex, attributeIndex;
		
		/* Load each attribute file in memory, one-by-one */
		
		for (i = 0; i < 4; i++) {
			
			/* Reset counters */
			lines = 0;
			bufferIndex = tupleIndex = 0;
			percent_ = _percent = 0;
			
			buffer = data[bufferIndex];
			
			/* Load file into memory */
			
			System.out.println(String.format("Loading file %s", filenames[i]));
			
			f = new FileInputStream(filenames[i]);
			d = new DataInputStream(f);
			b = new BufferedReader(new InputStreamReader(d));

			while ((line = b.readLine()) != null) {
				
				lines += 1;
				
				percent_ = (lines * 100) / MAX_LINES;
				if (percent_ == (_percent + 1)) {
					System.out.print(String.format("Loading file...%3d%%\r", percent_));
					_percent = percent_;
				}
				
				if (tupleIndex >= tuplesPerInsert) {
					
					tupleIndex = 0;
					bufferIndex ++; /* Move to next task buffer */
				}
				
				buffer = data[bufferIndex];
				
				attributeIndex = tupleIndex * tupleSize + ATTRIBUTE_OFFSET + (i * 4);
				
				if (containsInts[i])
					buffer.putInt   (attributeIndex, Integer.parseInt(line));
				else
					buffer.putFloat (attributeIndex, Float.parseFloat(line));
				
				tupleIndex ++;
			}

			b.close();

			System.out.println(String.format("%d lines processed. Last buffer position at %d: %d bytes remaining", 
					lines, buffer.position(), buffer.remaining()));

			/* Fill in the extra lines */
			if (i != 0) {
				int destPos = buffer.capacity() - tailSize;
				System.arraycopy(buffer.array(), 0, buffer.array(), destPos, tailSize);
			}
		}
		
		/* Writing in-memory data to file */
		System.out.println(String.format("Writing data to %s", outputFile));
		
		File datafile = new File (dataDir + outputFile);
		
		FileOutputStream f_ = new FileOutputStream (datafile);
		BufferedOutputStream output_ = new BufferedOutputStream(f_);
		
		long written = 0L;
		long offsets = 0L;
		
		for (ByteBuffer buf: data) {
			
			int length = buf.array().length;
			ByteBuffer L = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
			
			L.putInt(length);
			
			output_.write(L.array());
			output_.write(buf.array());
			output_.flush();
			
			written += length;
			offsets += L.array().length;
		}
		
		output_.close();
		
		System.out.println(String.format("%d bytes written (%d)", written, (written + offsets)));
		System.out.println("Bye.");
	}
}
