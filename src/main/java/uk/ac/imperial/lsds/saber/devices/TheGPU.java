package uk.ac.imperial.lsds.saber.devices;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;

public class TheGPU {
	
	private static final String gpuLibrary = SystemConf.SABER_HOME + "/clib/libGPU.so";
	
	private static final int pipelines = SystemConf.PIPELINE_DEPTH;
	
	private static final TheGPU gpuInstance = new TheGPU (5, 10);
	
	public static TheGPU getInstance () { return gpuInstance; }
	
	/* Managing GPU pipelining of multiple queries */
	
	private WindowBatch [] batches;
	
	private HeapMemoryManager memoryManager;
	
	public TheGPU (int q, int b) {
		
		memoryManager = new HeapMemoryManager (q, b);
		
		batches = new WindowBatch [pipelines];
		for (int i = 0; i < pipelines; ++i)
			batches [i] = null;
	}
	
	public void load () {
		
		try {
			
			System.load (gpuLibrary);
		
		} catch (final UnsatisfiedLinkError e) {
			System.err.println("error: failed to load GPU library");
			System.exit(1);
		}
	}
	
	public WindowBatch shiftUp (WindowBatch batch) {
		WindowBatch p = batches[0];
		for (int i = 0; i < pipelines - 1; ++i) {
			batches[i] = batches[i + 1];
		}
		batches[pipelines - 1] = batch;
		return p;
	}
	
	public void setInputBuffer (int qid, int bid, IQueryBuffer buffer) {
		memoryManager.setInputBuffer(qid, bid, buffer, 0, buffer.capacity());
	}
	
	public void setInputBuffer (int qid, int bid, IQueryBuffer buffer, int start, int end) {
		memoryManager.setInputBuffer(qid, bid, buffer, start, end);
	}
	
	public void setOutputBuffer (int qid, int bid, IQueryBuffer buffer) {
		memoryManager.setOutputBuffer(qid, bid, buffer);
	}
	
	public IQueryBuffer getOutputBuffer (int qid, int bid) {
		return memoryManager.getOutputBuffer(qid, bid);
	}
	
	public void inputDataMovementCallback (int qid, int bid, long address, int size) {
		memoryManager.inputDataMovementCallback(qid, bid, address, size);
	}
	
	public void outputDataMovementCallback (int qid, int bid, long address, int size) {
		memoryManager.outputDataMovementCallback(qid, bid, address, size);
	}
	
	public native int init (int N, int D);
	public native int free ();
	
	public native int getQuery (String source, int kernels, int inputs, int outputs);
	
	public native int setInput  (int queryId, int index, int size);
	public native int setOutput (int queryId, int index, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark);
	
	/* Operator-specific configuration */
	public native int setKernelDummy     (int queryId, int [] args1);
	public native int setKernelProject   (int queryId, int [] args1);
	public native int setKernelSelect    (int queryId, int [] args1);
	public native int setKernelThetaJoin (int queryId, int [] args1);
	public native int setKernelReduce    (int queryId, int [] args1, long [] args2);
	public native int setKernelAggregate (int queryId, int [] args1, long [] args2);
	
	public native int execute          (int queryId, int [] threads, int [] threadsPerGroup);
	public native int executeReduce    (int queryId, int [] threads, int [] threadsPerGroup, long [] args);
	public native int executeAggregate (int queryId, int [] threads, int [] threadsPerGroup, long [] args);
}
