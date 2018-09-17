package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;

public class LRBApp {

	public static final String usage = "usage: LRBApp";

	public static void main (String [] args) {

		BenchmarkQuery benchmarkQuery = null;
		int queryId = 1;

		String executionMode = "cpu";
		int numberOfThreads = 1;
		int batchSize = 1048576;

		String hostname = "localhost";
		int port = 6667;

		int bundle = 512;

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
			if (args[i].equals("--threads")) {
				numberOfThreads = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--batch-size")) {
				batchSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--query")) {
				queryId = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--host")) {
				hostname = args[j];
			} else
			if (args[i].equals("--port")) {
				port = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--bundle-size")) {
				bundle = Integer.parseInt(args[j]);
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}

		SystemConf.CIRCULAR_BUFFER_SIZE = 64 * 1048576;
		SystemConf.LATENCY_ON = false;

		SystemConf.PARTIAL_WINDOWS = 64;
		SystemConf.HASH_TABLE_SIZE = 32768;

		SystemConf.UNBOUNDED_BUFFER_SIZE = 1048576;

		SystemConf.CPU = false;
		SystemConf.GPU = false;

		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;

		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;

		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;

		SystemConf.THREADS = numberOfThreads;

		QueryConf queryConf = new QueryConf (batchSize);

		if (queryId == 1) {
			benchmarkQuery = new LRB1 (queryConf, true);
		} else
		if (queryId == 2) {
			//benchmarkQuery = new LRB2 (queryConf);
		} else
		if (queryId == 3) {
			//benchmarkQuery = new LRB3 (queryConf);
		} else
		if (queryId == 4) {
			//benchmarkQuery = new LRB4 (queryConf);
		} else {
			System.err.println("error: invalid benchmark query id");
			System.exit(1);
		}

		int networkBufferSize = bundle * benchmarkQuery.getSchema().getTupleSize();
		System.out.println(String.format("[DBG] %6d bytes/buffer", networkBufferSize));

		try {
			ServerSocketChannel server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress (hostname, port));
			server.configureBlocking(false);

			Selector selector = Selector.open();
			/* (SelectionKey) */ server.register(selector, SelectionKey.OP_ACCEPT);

			System.out.println("[DBG] ^");
			ByteBuffer buffer = ByteBuffer.allocate (networkBufferSize);
			while (true) {

				if (selector.select() == 0)
					continue;

				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iterator = keys.iterator();
				while (iterator.hasNext()) {

					SelectionKey key = iterator.next();

					if (key.isAcceptable()) {

						System.out.println("[DBG] key is acceptable");
						ServerSocketChannel _server = (ServerSocketChannel) key.channel();
						SocketChannel client = _server.accept();
						if (client != null) {
							System.out.println("[DBG] accepted client");
							client.configureBlocking(false);
							/* (SelectionKey) */ client.register(selector, SelectionKey.OP_READ);
						}
					} else if (key.isReadable()) {

						SocketChannel client = (SocketChannel) key.channel();
						int bytes = 0;
						if ((bytes = client.read(buffer)) > 0) {

							if (! buffer.hasRemaining()) {
								buffer.rewind();
								benchmarkQuery.getApplication().processData (buffer.array(), buffer.capacity());
								buffer.clear();
							}
						}
						if (bytes < 0) {
							System.out.println("[DBG] client connection closed");
							client.close();
						}
					} else {
						System.err.println("error: unknown selection key");
						System.exit(1);
					}
					iterator.remove();
				}
			}
		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
}
