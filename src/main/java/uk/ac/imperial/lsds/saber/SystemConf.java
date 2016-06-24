package uk.ac.imperial.lsds.saber;

import java.util.Map;

public class SystemConf {

	public static boolean WWW = false;

	public static String SABER_HOME = "/";
	
	static {
		
		Map<String, String> env = System.getenv();
		String dir = (String) env.get("SABER_HOME");
		if (dir != null) {
			SABER_HOME = dir;
		}
	}
	
	public enum SchedulingPolicy { HLS, FIFO, STATIC };
	
	public static SchedulingPolicy SCHEDULING_POLICY = SchedulingPolicy.FIFO;
	
	public static int SWITCH_THRESHOLD = 10;

	public static int PARTIAL_WINDOWS = 65536;
	
	public static int HASH_TABLE_SIZE = 1048576;
	
	public static long THROUGHPUT_MONITOR_INTERVAL = 1000L;
	
	public static long PERFORMANCE_MONITOR_INTERVAL = 1000L;

	public static int MOST_UPSTREAM_QUERIES = 2;

	public static int PIPELINE_DEPTH = 4;
	
	public static int CIRCULAR_BUFFER_SIZE = 1073741824;
	
	public static int UNBOUNDED_BUFFER_SIZE = 1048576;
	
	public static int THREADS = 1;
	
	public static int SLOTS = 1024;
	
	public static boolean CPU =  true;
	public static boolean GPU = false;
	
	public static boolean HYBRID = (CPU && GPU);
	
	public static boolean LATENCY_ON = false;
}
