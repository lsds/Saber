package uk.ac.imperial.lsds.saber.cql.operators.gpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class NoOpKernel implements IOperatorCode {
	
	private static final int numberOfThreadsPerGroup = 256;
	
	private int qid;
	
	private String filename = SystemConf.SABER_HOME + "/clib/templates/DummyKernel.cl";
	
	private ITupleSchema schema;
	
	private int inputSize;
	
	private int [] threads;
	private int [] threadsPerGroup;
	
	public NoOpKernel (ITupleSchema schema, int inputSize) {
		
		this.schema = schema;
		this.inputSize = inputSize;
		
		int tupleSize = schema.getTupleSize();
		
		if ((inputSize % tupleSize) != 0)
			throw new IllegalArgumentException("error: kernel input size is not a multiple of tuple size");
		
		int tuples = inputSize / tupleSize;
		
		threads = new int [1];
		threadsPerGroup = new int [1];
		
		threads[0] = tuples;
		threadsPerGroup[0] = numberOfThreadsPerGroup;
	}
	
	public void setup() {
		
		String source = KernelGenerator.getDummyOperator (filename, schema, schema);
		
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery(source, 1, 1, 1);
		
		TheGPU.getInstance().setInput (qid, 0, inputSize);
		
		TheGPU.getInstance().setOutput (qid, 0, inputSize, 1, 0, 0, 1, 1);
		
		TheGPU.getInstance().setKernelDummy (qid, null);
	}
	
	public int getQueryId () {
		return qid;
	}
	
	public int getThreads () {
		if (threads != null)
			return threads[0];
		else
			return 0;
	}
	
	public int getThreadsPerGroup () {
		if (threadsPerGroup != null)
			return threadsPerGroup[0];
		else
			return 0;
	}
	
	@Override
	public String toString () {
		StringBuilder sb = new StringBuilder();
		sb.append("DummyKernel ()");
		return sb.toString();
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		/* Set input */
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		int start = batch.getBufferStartPointer();
		int end   = batch.getBufferEndPointer();
		
		TheGPU.getInstance().setInputBuffer(qid, 0, inputBuffer, start, end);
		
		/* Set output for a previously executed operator */
		
		WindowBatch pipelinedBatch = TheGPU.getInstance().shiftUp(batch);
		
		IOperatorCode pipelinedOperator = null; 
		if (pipelinedBatch != null) {
			pipelinedOperator = pipelinedBatch.getQuery().getMostUpstreamOperator().getGpuCode();
			pipelinedOperator.configureOutput (qid);
		}
		
		// System.out.println("[DBG] execute task " + batch.getTaskId());
		
		/* Execute */
		TheGPU.getInstance().execute (qid, threads, threadsPerGroup);
		
		if (pipelinedBatch != null)
			pipelinedOperator.processOutput (qid, pipelinedBatch);
		
		api.outputWindowBatchResult (pipelinedBatch);
	}
	
	public void configureOutput (int queryId) {
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		TheGPU.getInstance().setOutputBuffer (queryId, 0, outputBuffer);
	}
	
	public void processOutput (int queryId, WindowBatch batch) {
		
		// System.out.println("[DBG] process output of task " + batch.getTaskId());
		
		IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer (queryId, 0);
		batch.setBuffer(buffer);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}
}
