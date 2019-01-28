package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.Generator;

public class PerformanceEngineeringApp {
    public static final String usage = "usage: PerformanceEngineeringApp with in-memory generation";

    public static void main(String[] args) throws InterruptedException {

        // choose a query between 0-3
        int queryNum = 0;
        boolean isShortRun = true; // define if the application runs for either 3 (isShortRun==true) or 60 seconds (isShortRun==false)



        // DO NOT CHANGE ANYHTING BELOW THIS LINE!!!
        PerformanceEngineeringQuery benchmarkQuery = null;
        int numberOfThreads = 1;
        int batchSize = 2 * 1048576;
        String executionMode = "cpu";
        int circularBufferSize = 16 * 1048576;
        int unboundedBufferSize = 2 * 1048576;
        int hashTableSize = 2 * 64 * 128;
        int partialWindows = 1024;
        int slots = 64 * 1024;

        // change the tuple size to half if set true
        //boolean isV2 = false;

        /* Parse command line arguments */
        if (args.length != 0)
            queryNum = Integer.parseInt(args[0]);

        if (queryNum > 3) {
            System.out.println("[DBG] This application supports only four queries. Enter a number between 0-3 to choose one of them");
            System.exit(1);
        } else {
            System.out.println("[DBG] Running query " + queryNum);
        }

        // Set SABER's configuration
        QueryConf queryConf = new QueryConf(batchSize);
        SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;
        SystemConf.UNBOUNDED_BUFFER_SIZE = unboundedBufferSize;
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
        SystemConf.FIRST_FILTER_SELECTIVITY = (queryNum == 1 || queryNum == 2) ? 0.50 : 0.25;


        /* Initialize the Operators of the Benchmark */
        benchmarkQuery = new PerformanceEngineering(queryConf, true, queryNum);

        /* Generate input stream */
        int numberOfGeneratorThreads = 1;
        int adsPerCampaign = ((PerformanceEngineering) benchmarkQuery).getAdsPerCampaign();
        long[][] ads = ((PerformanceEngineering) benchmarkQuery).getAds();
        int bufferSize = 8 * 131072;
        int coreToBind = SystemConf.THREADS + 2;

        boolean runOnce = true;
        Generator generator = new Generator(bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind, false, runOnce, SystemConf.FIRST_FILTER_SELECTIVITY);
        GeneratedBuffer b = generator.getNext();
        long offset = (isShortRun) ? 3 * 1000 : 60 * 1000;
        long timeLimit = System.currentTimeMillis() + offset;

        while (true) {

            if (timeLimit <= System.currentTimeMillis()) {
                System.out.println("Terminating execution...");
                System.exit(0);
            }

            //GeneratedBuffer b = generator.getNext();
            benchmarkQuery.getApplication().processData(b.getBuffer().array());
            //b.unlock();
        }
    }
}
