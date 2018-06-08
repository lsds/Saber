package uk.ac.imperial.lsds.saber;

import java.util.Map;

public class SystemConf {

	public static boolean WWW = false;

	public static String SABER_HOME = "/home/george/saber/yahoo_benchmark_saber";
	
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

	public static int PARTIAL_WINDOWS = 0;
	
	public static int HASH_TABLE_SIZE = 1048576;
	
	public static long THROUGHPUT_MONITOR_INTERVAL = 1000L;
	
	public static long PERFORMANCE_MONITOR_INTERVAL = 1000L;

	public static int MOST_UPSTREAM_QUERIES = 2;

	public static int PIPELINE_DEPTH = 4;
	
	public static int CIRCULAR_BUFFER_SIZE = 1073741824;
	
	public static int RELATIONAL_TABLE_BUFFER_SIZE = 1048576;
	
	public static int UNBOUNDED_BUFFER_SIZE = 1048576;
	
	public static int THREADS = 1;
	
	public static int SLOTS = 1024;
	
	public static boolean CPU =  true;
	public static boolean GPU = false;
	
	public static boolean HYBRID = false;
	
	public static boolean LATENCY_ON = false;
	
	public static long DURATION = 0;
	
	public static boolean parse (String opt, String arg) {
		/* 
		 * Parse command-line argument
		 */
		if (opt.equals("--execution-mode")) {
			if (arg.compareTo("cpu") == 0) {
				CPU = true; GPU = false; HYBRID = false;
			} else
			if (arg.compareTo("gpu") == 0) {
				GPU = true; CPU = false; HYBRID = false;
			} else {
				HYBRID = CPU = GPU = true;
			}
		} else
		if (opt.equals("--number-of-worker-threads")) {
			THREADS = Integer.parseInt(arg);
		} else
		if (opt.equals("--number-of-result-slots")) { 
			SLOTS = Integer.parseInt(arg);
		} else
		if (opt.equals("--home-directory")) {
			SABER_HOME = arg;
		} else
		if (opt.equals("--scheduling-policy")) {
			if (arg.compareTo("fcfs") == 0) {
				SCHEDULING_POLICY = SchedulingPolicy.FIFO;
			} else
			if (arg.compareTo("static") == 0) {
				SCHEDULING_POLICY = SchedulingPolicy.STATIC;
			} else {
				SCHEDULING_POLICY = SchedulingPolicy.HLS;
			}
		} else
		if (opt.equals("--switch-threshold")) {
			SWITCH_THRESHOLD = Integer.parseInt(arg);
		} else
		if (opt.equals("--number-of-partial-windows")) {
			PARTIAL_WINDOWS = Integer.parseInt(arg);
		} else
		if (opt.equals("--circular-buffer-size")) {
			CIRCULAR_BUFFER_SIZE = Integer.parseInt(arg);
		} else
		if (opt.equals("--intermediate-buffer-size")) {
			UNBOUNDED_BUFFER_SIZE = Integer.parseInt(arg);
		} else
		if (opt.equals("--hash-table-size")) {
			HASH_TABLE_SIZE = Integer.parseInt(arg);
		} else
		if (opt.equals("--throughput-monitor-interval")) {
			THROUGHPUT_MONITOR_INTERVAL = Long.parseLong(arg);
		} else
		if (opt.equals("--performance-monitor-interval")) {
			PERFORMANCE_MONITOR_INTERVAL = Long.parseLong(arg);
		} else
		if (opt.equals("--number-of-upstream-queries")) {
			MOST_UPSTREAM_QUERIES = Integer.parseInt(arg);
		} else
		if (opt.equals("--pipeline-depth")) {
			PIPELINE_DEPTH = Integer.parseInt(arg);
		} else
		if (opt.equals("--enable-latency-measurements")) {
			LATENCY_ON = Boolean.parseBoolean(arg);
		} else
		if (opt.equals("--launch-web-server")) {
			WWW = Boolean.parseBoolean(arg);
		} else
		if (opt.equals("--experiment-duration")) {
			DURATION = Long.parseLong(arg);
		} else {
			/* Not a valid system configuration option */
			return false;
		}
		return true;
	}
	
	public static void dump () {
			
		StringBuilder s = new StringBuilder("=== [System configuration dump] ===\n");
		
		s.append(String.format("Execution mode               : CPU %s GPU %s\n", CPU, GPU));
		s.append(String.format("Number of worker threads     : %d\n", THREADS));
		s.append(String.format("Number of result slots       : %d\n", SLOTS));
		s.append(String.format("Home directory               : %s\n", SABER_HOME));
		s.append(String.format("Scheduling policy            : %s\n", SCHEDULING_POLICY));
		s.append(String.format("Switch threshold             : %d tasks\n", SWITCH_THRESHOLD));
		s.append(String.format("Number of partial windows    : %d\n", PARTIAL_WINDOWS));
		s.append(String.format("Circular buffer size         : %d bytes\n", CIRCULAR_BUFFER_SIZE));
		s.append(String.format("Intermediate buffer size     : %d bytes\n", UNBOUNDED_BUFFER_SIZE));
		s.append(String.format("Hash table size              : %d bytes\n", HASH_TABLE_SIZE));
		s.append(String.format("Throughput monitor interval  : %d msec\n", THROUGHPUT_MONITOR_INTERVAL));
		s.append(String.format("Performance monitor interval : %d msec\n", PERFORMANCE_MONITOR_INTERVAL));
		s.append(String.format("Number of upstream queries   : %d\n", MOST_UPSTREAM_QUERIES));
		s.append(String.format("GPU pipeline depth           : %d\n", PIPELINE_DEPTH));
		s.append(String.format("Latency measurements         : %s\n", (LATENCY_ON ? "On" : "Off")));
		s.append(String.format("Web server                   : %s\n", (WWW ? "On" : "Off")));
		s.append(String.format("Experiment duration          : %d units (= perf. monitor intervals)\n", DURATION));
		
		s.append("=== [End of system configuration dump] ===");
		
		System.out.println(s.toString());
	}
}
