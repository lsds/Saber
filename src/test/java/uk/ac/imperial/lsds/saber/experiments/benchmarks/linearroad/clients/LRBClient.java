package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.clients;

import java.net.InetSocketAddress;
import java.net.InetAddress;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class LRBClient {
	
	private static final String usage = "usage: java LRBClient";
	
	public static void main (String[] args) {
		
		String hostname = "localhost";
		int port = 6667;
		
		int reports = 11928635; /* 3h dataset */
		int tupleSize = 32;
		
		int _BUFFER_ = tupleSize * reports;
		ByteBuffer data = ByteBuffer.allocate(_BUFFER_);
		
		int bundle = 512;
		
		int L = 1;
		
		String filename = "datafile3hours.dat";
		
		FileInputStream f;
		DataInputStream d;
		BufferedReader  b;
		
		String line = null;
		long lines = 0;
		long MAX_LINES = 12048577L;
		long percent_ = 0L, _percent = 0L;
		
		/* Time measurements */
		long start = 0L;
		long bytes = 0L;
		double dt;
		double rate; /* tuples/sec */
		double _1MB = 1024. * 1024.;
		double MBps; /* MB/sec */
		long totalTuples = 0;
		
		long wrongTuples = 0L;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("-h")) { 
				hostname = args[j];
			} else
			if (args[i].equals("-p")) { 
				port = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-b")) { 
				bundle = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-L")) { 
				L = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-f")) { 
				filename = args[j];
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
		LRBTuple tuple = new LRBTuple ();
		
		try {
			/* Establish connection to the server */
			SocketChannel channel = SocketChannel.open();
			channel.configureBlocking(true);
			InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(hostname), port);
			/* System.out.println(address); */
			channel.connect(address);
		
			while (! channel.finishConnect())
				;
			
			/* Load file into memory */
			f = new FileInputStream(filename);
			d = new DataInputStream(f);
			b = new BufferedReader(new InputStreamReader(d));
			
			start = System.currentTimeMillis();
			
			while ((line = b.readLine()) != null) {
				lines += 1;
				bytes += line.length() + 1; // +1 for '\n'
				
				percent_ = (lines * 100) / MAX_LINES;
				if (percent_ == (_percent + 1)) {
					System.out.print(String.format("Loading file...%3d%%\r", percent_));
					_percent = percent_;
				}
				
				LRBTuple.parse(line, tuple);
				
				if (tuple.getPosition() < 0) {
					wrongTuples += 1;
					continue;
				}

				if (tuple.getType() != 0) {
					continue;
				}
				
				totalTuples += 1;
				
				/* Populate data */
				data.putLong  (tuple.getTimestamp());
				data.putInt   (tuple.getVehicleId());
				data.putFloat (tuple.getSpeed()    );
				data.putInt   (tuple.getHighway()  );
				data.putInt   (tuple.getLane()     );
				data.putInt   (tuple.getDirection());
				data.putInt   (tuple.getPosition() ); /* Tuple size is 32 bytes */
			}
			
			d.close();
			dt = (double ) (System.currentTimeMillis() - start) / 1000.;
			/* Statistics */
			rate =  (double) (lines) / dt;
			MBps = ((double) bytes / _1MB) / dt;
			
			System.out.println(String.format("[DBG] %10d lines read", lines));
			System.out.println(String.format("[DBG] %10d bytes read", bytes));
			System.out.println(String.format("[DBG] %10d tuples in data buffer", totalTuples));
			System.out.println();
			System.out.println(String.format("[DBG] %10.1f seconds", (double) dt));
			System.out.println(String.format("[DBG] %10.1f tuples/s", rate));
			System.out.println(String.format("[DBG] %10.1f MB/s", MBps));
			System.out.println(String.format("[DBG] %10d tuples ignored", wrongTuples));
			System.out.println();
		
			/* Prepare data for reading */
			data.flip();
			
			/* Buffer to sent */
			ByteBuffer buffer = ByteBuffer.allocate(tupleSize * bundle);
			System.out.println(String.format("[DBG] %6d bytes/buffer", _BUFFER_));
			/* Tuple buffer */
			byte [] t = new byte [tupleSize];
			
			totalTuples = 0L;
			bytes = 0L;
			
			long _bundles = 0L;
			
			while (data.hasRemaining()) {
				data.get(t);
				totalTuples += 1;
				for (i = 0; i < L; i++) {
					buffer.put(t);
					ByteBuffer.wrap(t).putInt(20, i + 1); /* Position 20 is the highway id */
					
					if (! buffer.hasRemaining()) {
						/* Bundle assembled. Send and rewind. */
						_bundles ++;
						buffer.flip();
						/* 
 						 * System.out.println(String.format("[DBG] send bundle %d (%d bytes remaining)", 
						 * _bundles, buffer.remaining()));
						 */
						bytes += channel.write(buffer);
						/* Rewind buffer and continue populating with tuples */
						buffer.clear();
					}
				}
			}
			
			/* Sent last (incomplete) bundle */
			buffer.flip();
			bytes += channel.write(buffer);
			
			System.out.println(String.format("[DBG] %10d tuples processed %d bundles (%d bytes) sent", 
					totalTuples, _bundles, bytes));
			System.out.println("Bye.");
			
		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
}

