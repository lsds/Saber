package uk.ac.imperial.lsds.saber.cql.operators.gpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class ProjectionKernel implements IOperatorCode {
	
	private static final int numberOfThreadsPerGroup = 128;
	
	private int qid;
	
	private static String filename = SystemConf.SABER_HOME + "/clib/templates/Projection.cl";
	
	private ITupleSchema inputSchema, outputSchema;
	
	private Expression [] expressions;
	
	private int inputSize, outputSize;
	
	/* Floating point expression depth. This is tightly coupled with our synthetic benchmark */
	private int depth = -1;
	
	private int [] args;
	
	private int [] threads;
	private int [] threadsPerGroup;
	
	public ProjectionKernel (ITupleSchema inputSchema, Expression[] expressions, int inputSize, int depth) {
		
		this.inputSchema = inputSchema;
		this.expressions = expressions;
		this.inputSize = inputSize;
		this.depth = depth;
		
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		
		int tupleSize = inputSchema.getTupleSize();
		
		if ((inputSize % tupleSize) != 0)
			throw new IllegalArgumentException("error: kernel input size is not a multiple of tuple size");
		
		int tuples = inputSize / tupleSize;
		
		outputSize = tuples * outputSchema.getTupleSize();
		
		threads = new int [1];
		threadsPerGroup = new int [1];
		
		threads[0] = tuples;
		threadsPerGroup[0] = numberOfThreadsPerGroup;
		
		args = new int [4];
		
		args[0] = tuples;
		args[1] = inputSize;
		args[2] = threadsPerGroup[0] * tupleSize;;
		args[3] = threadsPerGroup[0] * outputSchema.getTupleSize();
	}
	
	public void setup () {
		
		String source = KernelGenerator.getProjectionOperator (filename, inputSchema, outputSchema, depth);
		
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery (source, 1, 1, 1);
		
		TheGPU.getInstance().setInput (qid, 0, inputSize);
		
		TheGPU.getInstance().setOutput (qid, 0, outputSize, 1, 0, 0, 1, 1);
		
		TheGPU.getInstance().setKernelProject (qid, args);
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
		StringBuilder s = new StringBuilder();
		s.append("Projection (");
		for (Expression e: expressions)
			s.append(e.toString() + " ");
		s.append(")");
		return s.toString();
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
		
		IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer (queryId, 0);
		batch.setBuffer(buffer);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}
}
