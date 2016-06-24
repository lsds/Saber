package uk.ac.imperial.lsds.saber.processors;

import java.util.concurrent.Executor;

import uk.ac.imperial.lsds.saber.tasks.TaskQueue;

public class TaskProcessorPool {

	private int workers;
	
	private TaskQueue queue;
	private TaskProcessor [] processor;
	
	public TaskProcessorPool (int workers, final TaskQueue queue, int [][] policy, boolean GPU, boolean hybrid) {
		
		this.workers = workers;
		this.queue = queue;
		
		System.out.println(String.format("[DBG] %d threads (hybrid mode %s)", this.workers, hybrid));
		
		this.processor = new TaskProcessor[workers];
		if (hybrid) {
			/* Assign the first processor to be the GPU worker */
			this.processor[0] = new TaskProcessor(0, queue, policy, true, hybrid);
			for (int i = 1; i < workers; i++)
				this.processor[i] = new TaskProcessor(i, queue, policy, false, hybrid);
		} else {
			if (GPU) {
				/* GPGPU-only */
				System.out.println("[DBG] GPGPU-only execution");
				if (workers > 1)
					throw new IllegalArgumentException("error: invalid number of worker threads");
				
				this.processor[0] = new TaskProcessor(0, queue, policy, true, hybrid);
				
			} else {
				/* CPU-only */
				System.out.println("[DBG] CPU-only execution");
				for (int i = 0; i < workers; i++)
					this.processor[i] = new TaskProcessor(i, queue, policy, false, hybrid);
			}
		}
		
		/* Enable monitoring */
		/*
		for (int i = 0; i < workers; i++)
			this.processor[i].enableMonitoring();
		 */
	}
	
	public TaskQueue start (Executor executor) {
		for (int i = 0; i < workers; i++)
			executor.execute(this.processor[i]);
		return queue;
	}
	
	public long getProcessedTasks (int pid, int qid) {
		return processor[pid].getProcessedTasks(qid);
	}
	
	public double mean (int pid) {
		return processor[pid].mean();
	}
	
	public double stdv (int pid) {
		return processor[pid].stdv();
	}
}
