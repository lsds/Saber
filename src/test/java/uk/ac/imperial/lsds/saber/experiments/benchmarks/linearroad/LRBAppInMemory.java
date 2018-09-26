package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.utils.LRBGenerator;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class LRBAppInMemory {

	public static final String usage = "usage: LRBApp";

	public static void main (String [] args) throws InterruptedException {

		BenchmarkQuery benchmarkQuery = null;
		int queryId = 1;

		String executionMode = "cpu";
		int numberOfThreads = 1;
		int batchSize = 64*1048576;

		boolean jni = true;

		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("--mode")) {
				executionMode = args[j];
			} else
			if (args[i].equals("--jni")) {
				jni = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--threads")) {
				numberOfThreads = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--batch-size")) {
				batchSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--query")) {
				queryId = Integer.parseInt(args[j]);
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}

		SystemConf.CIRCULAR_BUFFER_SIZE = 8 * 128 * 1048576;
		SystemConf.LATENCY_ON = false;

		SystemConf.PARTIAL_WINDOWS = 8 * 32 * 1024;

		// manually change the c code every time!!!
		SystemConf.HASH_TABLE_SIZE = 2*32*1024; //1 * 32768;

		SystemConf.UNBOUNDED_BUFFER_SIZE = 2*64 * 1048576;

		SystemConf.CPU = true;

		SystemConf.THREADS = numberOfThreads;

		QueryConf queryConf = new QueryConf (batchSize);

		benchmarkQuery = new LRB1 (queryConf, jni);

		/* Generate input stream */
		int numberOfGeneratorThreads = 1;

		int bufferSize = 1 * 131072; // set the timestamps with this buffer size
		int coreToBind = 3; //numberOfThre/ads + 1;
		int dataRange = 1024;
		SystemConf.C_HASH_TABLE_SIZE = dataRange;

		LRBGenerator generator = new LRBGenerator (bufferSize, numberOfGeneratorThreads, dataRange, coreToBind);
		long timeLimit = System.currentTimeMillis() + 10 * 10000;

		long tempTime = -1;
		while (true) {

			if (timeLimit <= System.currentTimeMillis()) {
				System.out.println("Terminating execution...");
				System.exit(0);
			}

			GeneratedBuffer b = generator.getNext();
			/*if (b.getBuffer().getLong(0) < tempTime)
			    System.exit(-1);
			tempTime = b.getBuffer().getLong(0);
            System.out.println("tempTime \n" + tempTime);*/

			benchmarkQuery.getApplication().processData (b.getBuffer().array());
			b.unlock();
		}

	}
}
