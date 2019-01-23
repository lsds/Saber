package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.Generator;

public class YahooBenchmarkApp {
	public static final String usage = "usage: YahooBenchmarkApp with in-memory generation";
	
	public static void main (String [] args) throws InterruptedException {

		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 1;
		int batchSize = 2 * 1048576;
		String executionMode = "cpu";
		int circularBufferSize = 16 * 1048576;
		int unboundedBufferSize = 2 * 1048576;
		int hashTableSize = 2*64*128;
		int partialWindows = 1024;
		int slots = 64 * 1024;

		// change the tuple size to half if set true
		//boolean isV2 = false;

		/* Parse command line arguments */
		if (args.length!=0)  
			numberOfThreads = Integer.parseInt(args[0]);
		
		// Set SABER's configuration				
		QueryConf queryConf = new QueryConf (batchSize);		
		SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;		
		SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;		
		SystemConf.HASH_TABLE_SIZE = hashTableSize;		
		SystemConf.PARTIAL_WINDOWS = partialWindows;
		SystemConf.SLOTS = slots;
		SystemConf.SWITCH_THRESHOLD = 10;	
		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;		
		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		SystemConf.THREADS = numberOfThreads;
		SystemConf.LATENCY_ON = false;


		/* Initialize the Operators of the Benchmark */
		benchmarkQuery = new YahooBenchmark (queryConf, true);
		
		/* Generate input stream */
		int numberOfGeneratorThreads = 1;
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();
		int bufferSize = 4 * 131072;
		int coreToBind = SystemConf.THREADS + 2;
		
		
		Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind, false);
		long timeLimit = System.currentTimeMillis() + 50*1000;

		//GeneratedBuffer b = generator.getNext();

		while (true) {
			
			if (timeLimit <= System.currentTimeMillis()) {
				System.out.println("Terminating execution...");
				System.exit(0);
			}						

			GeneratedBuffer b = generator.getNext();
			benchmarkQuery.getApplication().processData (b.getBuffer().array());
			b.unlock();
		}
	}
}
