package uk.ac.imperial.lsds.saber;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.dispatchers.ITaskDispatcher;
import uk.ac.imperial.lsds.saber.processors.TaskProcessorPool;
import uk.ac.imperial.lsds.saber.tasks.TaskQueue;
import uk.ac.imperial.lsds.saber.www.RESTfulHandler;
import uk.ac.imperial.lsds.saber.www.RESTfulServer;

public class QueryApplication {

	private static int threads = SystemConf.THREADS;
	
	private Set<Query> queries;
	
	/*
	 * At the top level, the input stream will be will 
	 * be dispatched to the most upstream queries
	 */
	private int numberOfUpstreamQueries;
	
	private ITaskDispatcher	[] dispatchers;
	
	private TaskQueue queue;
	private TaskProcessorPool workerPool;
	private Executor executor;
	
	private int M = 2; /* CPU and GPGPU */
	private int N; /* Number of queries */
	
	int [][] policy;
	
	private RESTfulHandler handler = null;
	
	public QueryApplication (Set<Query> queries) {
		
		this.queries = queries;
		
		numberOfUpstreamQueries = 0;
		
		dispatchers = new ITaskDispatcher [1];
		
		N = this.queries.size();
	}
	
	public void processData (byte [] values) {
		
		processData (values, values.length);
	}
	
	public void processData (byte [] values, int length) {
		
		for (int i = 0; i < dispatchers.length; ++i) {
			dispatchers[i].dispatch (values, length);
		}
	}
	
	public void processFirstStream (byte [] values) {
		
		processFirstStream (values, values.length);
	}
	
	public void processFirstStream (byte [] values, int length) {
		
		for (int i = 0; i < dispatchers.length; ++i) {
			dispatchers[i].dispatchToFirstStream (values, length);
		}
	}
	
	public void processSecondStream (byte [] values) {
		
		processSecondStream (values, values.length);
	}
	
	public void processSecondStream (byte [] values, int length) {
		
		for (int i = 0; i < dispatchers.length; ++i) {
			dispatchers[i].dispatchToSecondStream (values, length);
		}
	}
	
	public void setup() {
		
		this.policy = new int [M][N];
		
		for (int i = 0; i < M; i++) {
			for (int j = 0; j < N; j++) {
				policy [i][j] = 1;
			}
		}
		// policy[0][0] = 6000;
		// policy[1][0] = 0;
		
		queue = new TaskQueue (N);
		
		/* Bind main thread to CPU core 0 */
		TheCPU.getInstance().bind(0);
		
		if (SystemConf.GPU) {
			TheGPU.getInstance().load ();
			TheGPU.getInstance().init (N, SystemConf.PIPELINE_DEPTH);
		}
		
		workerPool = new TaskProcessorPool(threads, queue, policy, SystemConf.GPU, SystemConf.HYBRID);
		executor = Executors.newCachedThreadPool();
		queue = workerPool.start(executor);
		
		for (Query q: queries) {
			q.setParent(this);
			q.setup();
			if (q.isMostUpstream())
				setDispatcher(q.getTaskDispatcher());
		}
		
		Thread performanceMonitor = new Thread(new PerformanceMonitor(this));
		performanceMonitor.setName("Performance monitor");
		performanceMonitor.start();
		
		Thread throughputMonitor = new Thread(new QueryThroughputMonitor(this));
		throughputMonitor.setName("Throughput monitor");
		throughputMonitor.start();
		
		if (SystemConf.WWW) {
			handler = new RESTfulHandler (this, 100); /* limit = 10 */
			Thread webServer = new Thread(new RESTfulServer(8081, handler));
			webServer.setName("Web server");
			webServer.start();
		}
	}
	
	private void setDispatcher (ITaskDispatcher dispatcher) {
		int idx = numberOfUpstreamQueries++;
		if (numberOfUpstreamQueries > dispatchers.length) {
			/* Resize array */
			ITaskDispatcher [] array = new ITaskDispatcher [numberOfUpstreamQueries];
			for (int i = 0; i < idx; ++i)
				array[i] = dispatchers[i];
			dispatchers = array;
		}
		dispatchers[idx] = dispatcher;
	}
	
	public TaskQueue getExecutorQueue() {
		return queue;
	}
	
	public int getExecutorQueueSize() {
		return queue.size();
	}
	
	public Set<Query> getQueries() {
		return queries;
	}
	
	public TaskProcessorPool getTaskProcessorPool () {
		return workerPool; 
	}

	public void updatePolicy (int [][] policy_) {
		for (int i = 0; i < M; i++)
			for (int j = 0; j < N; j++)
				policy[i][j] = policy_[i][j];
	}
	
	public String policyToString () {
		StringBuilder b = new StringBuilder("[");
		for (int i = 0; i < M; i++) {
			for (int j = 0; j < N; j++) {
				if (i == (M - 1) && j == (N - 1))
					b.append(String.format("[%d][%d]=%5d",  i, j, policy[i][j]));
				else
					b.append(String.format("[%d][%d]=%5d ", i, j, policy[i][j]));
			}
		}
		b.append("]");
		return b.toString();
	}
	
	public float getPolicy (int p, int q) {
		return (float) (policy[p][q]);
	}
	
	public void RESTfulUpdate (long timestamp) {
		if (handler == null)
			return;
		for (int i = 0; i < numberOfQueries(); ++i)
			handler.addMeasurement(i, timestamp, (float) policy[1][i], (float) policy[0][i]);
	}
	
	public String getExecutorQueueCounts () {
		StringBuilder b = new StringBuilder(" ([");
		for (int i = 0; i < M; i++) {
			for (int j = 0; j < N; j++) {
				if (i == (M - 1) && j == (N - 1))
					b.append(String.format("[%d][%d]=%5d",  i, j, queue.getCount(i, j)));
				else
					b.append(String.format("[%d][%d]=%5d ", i, j, queue.getCount(i, j)));
			}
		}
		b.append("])");
		return b.toString();
	}
	
	public int numberOfQueries () {
		return N;
	}
	
	public int numberOfUpstreamQueries () {
		return numberOfUpstreamQueries;
	}
}
