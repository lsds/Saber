package uk.ac.imperial.lsds.saber;

public class QueryThroughputMonitor implements Runnable {
	
	int counter = 0;
	
	private long time, _time = 0L;
	private long dt;
		
	private QueryApplication application;
	private int size;
		
	private long [][] _tasksProcessed;
	private int  [][] policy_; /* New policy */
	
	public QueryThroughputMonitor (QueryApplication application) {
		this.application = application;
			
		size = application.getQueries().size();
		
		_tasksProcessed = new long [SystemConf.THREADS][size];
		for (int i = 0; i < _tasksProcessed.length; i++)
			for (int j = 0; j < size; j++)
				_tasksProcessed[i][j] = 0L;
		
		policy_ = new int [2][size];
		for (int j = 0; j < size; j++) {
			policy_[0][j] = 0;
			policy_[1][j] = 0;
		}
	}
	
	public void run () {
		while (true) {
			
			try { 
				Thread.sleep (SystemConf.THROUGHPUT_MONITOR_INTERVAL);
			} catch (Exception e) 
			{}
			
			time = System.currentTimeMillis();
			
			if (_time > 0) {
				
				dt = time - _time;
			
				/* Reset CPU tasks/sec per query since it is not acumulative */
				for (int j = 0; j < size; j++)
					policy_[1][j] = 0;
			
				/* Iterate over worker threads */
				for (int i = 0; i < _tasksProcessed.length; i++) {
				
					/* Iterate over queries */
					for (int j = 0; j < size; j++) {
					
						long tasksProcessed_ = 
							application.getTaskProcessorPool().getProcessedTasks(i, j);
					
						long delta = tasksProcessed_ - _tasksProcessed[i][j];
					
						double tps = (double) delta / (dt / 1000.);
					
						// System.out.println(String.format("p %d q %d new %6d old %6d delta %6d dt %5.5f tps %5.5f", 
						//	i, j, tasksProcessed_, _tasksProcessed[i][j], delta, (dt / 1000.), tps));
					
						if (SystemConf.HYBRID && i == 0) {
							policy_[0][j] = (int) Math.floor(tps);
						} else {
							policy_[1][j] += (int) Math.floor(tps);
						}
						_tasksProcessed[i][j] = tasksProcessed_;
					}
					application.updatePolicy(policy_);
				}
			}
			_time = time;
		}
	}
}
