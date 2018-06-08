package uk.ac.imperial.lsds.saber.processors;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.tasks.AbstractTask;
import uk.ac.imperial.lsds.saber.tasks.TaskQueue;

public class TaskProcessor implements Runnable {

	TaskQueue queue;
	private int [][] policy;
	private int pid;
	boolean GPU, hybrid;
	
	private int cid = 0; /* Processor class: GPU (0) or CPU (1) */
	
	/* Measurements */
	private AtomicLong [] tasksProcessed;
	
	/* Latency measurements (timing queue's poll method) */
	boolean monitor = false;
	
	private long count = 0L;
	private long start;
	private double  dt;
	double _m, m, _s, s;
	double avg = 0D, std = 0D;
	
	public TaskProcessor (int pid, TaskQueue queue, int [][] policy, boolean GPU, boolean hybrid) {
		
		this.pid = pid;
		this.queue = queue;
		this.policy = policy;
		this.GPU = GPU;
		this.hybrid = hybrid;
		
		if (GPU) 
			cid = 0;
		else 
			cid = 1;
		
		int n = policy[0].length; /* Number of queries */
		tasksProcessed = new AtomicLong [n];
		
		for (int i = 0; i < n; i++)
			this.tasksProcessed[i] = new AtomicLong (0L);
	}
	
	public void enableMonitoring () {
		this.monitor = true;
	}
	
	public void run () {
		
		AbstractTask task = null;
		/* Pin worker to thread */
		int min = (hybrid ? 3 : 1); /* +1 dispatcher, +1 GPU, if available */
		int max = 15;
		int total = max - min + 1;
		if (GPU) {
			System.out.println ("[DBG] GPU thread is " + Thread.currentThread());
			TheCPU.getInstance().bind(1);
		} else {
			int core = ((pid - (min - 1)) % total) + min + 4;
			System.out.println(String.format("[DBG] bind worker %2d to core %2d", pid, core));
			TheCPU.getInstance().bind(core);
		}
		
		ThreadMap.getInstance().register(Thread.currentThread().getId());
		
		while (true) {
			
			try {
				if (monitor) {
					/* Queue poll latency measurements */
					start = System.nanoTime();
				}
				
				while ((task = queue.poll(policy, cid)) == null) {
					LockSupport.parkNanos(1L);
				}
				
				if (monitor) {
					dt = (double) (System.nanoTime() - start);
					count += 1;
					if (count > 1) {
						if (count == 2) {
							_m = m = dt;
							_s = s = 0D;
						} else {
							m = _m + (dt - _m) / (count - 1);
							s = _s + (dt - _m) * (dt - m);
							_m = m;
							_s = s;
						}
					}
				}
				
				// System.out.println(String.format("[DBG] processor %2d task %d.%6d (GPU %5s)", pid, task.queryid, task.taskid, GPU));
				task.setGPU(GPU);
				tasksProcessed[task.queryid].incrementAndGet();
				task.run();
				
			} catch (Exception e) {
				
				e.printStackTrace();
				System.exit(1);
				
			} finally {
				if (task != null) {
					task.free();
				}
			}
		}
	}
	
	public long getProcessedTasks(int qid) {
		return tasksProcessed[qid].get();
	}
	
	public double mean () {
		if (! monitor)
			return 0D;
		avg = (count > 0) ? m : 0D;
		return avg;
	}
	
	public double stdv () {
		if (! monitor)
			return 0D;
		std = (count > 2) ? Math.sqrt(s / (double) (count - 1 - 1)) : 0D;
		return std;
	}
}
