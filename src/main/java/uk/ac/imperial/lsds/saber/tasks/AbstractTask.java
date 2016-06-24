package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.atomic.AtomicMarkableReference;

public abstract class AbstractTask implements IWindowAPI {
	
	public int taskid;
	public int queryid;
	
	public AtomicMarkableReference<AbstractTask> next;
	
	protected boolean GPU = false;
	
	public abstract int run ();
	
	public abstract void free ();
	
	public void setGPU (boolean GPU) {
		this.GPU = GPU;
	}
}
