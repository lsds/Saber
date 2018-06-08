package uk.ac.imperial.lsds.saber.cql.operators.gpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class SelectionKernel implements IOperatorCode {
	
	private static final int numberOfThreadsPerGroup = 256;
	private static final int numberOfTuplesPerThread =   2;
	
	private int qid;
	
	private static String filename = SystemConf.SABER_HOME + "/clib/templates/Selection.cl";
	
	private ITupleSchema schema;
	
	private IPredicate predicate;
	private String customPredicate;
	
	private int inputSize, outputSize;
	
	private int [] args; /* Arguments to the selection kernel */
	
	private int [] threads;
	private int [] threadsPerGroup;
	
	int records, numberOfThreadGroups;
	
	private static boolean isPowerOfTwo (int value) {
		if (value == 0)
			return false;
		while (value != 1) {
			if (value % 2 != 0)
				return false;
			value = value /2;
		}
		return true;
    }
	
	public SelectionKernel (ITupleSchema schema, IPredicate predicate, String customPredicate, int inputSize) {
		
		this.schema = schema;
		this.predicate = predicate;
		this.customPredicate = customPredicate;
		this.inputSize = inputSize;
		
		outputSize = inputSize; /* Upper bound, when selectivity is 100% */
		
		int tupleSize = schema.getTupleSize();
		
		if ((inputSize % tupleSize) != 0)
			throw new IllegalArgumentException("error: kernel input size is not a multiple of tuple size");
		
		int tuples = inputSize / tupleSize;
		
		records = tuples;
		while (! isPowerOfTwo(records))
			++records;
		
		if (records < numberOfTuplesPerThread)
			throw new IllegalArgumentException(String.format("error: number of tuples must be greater than %d", 
					numberOfTuplesPerThread));
		
		if ((records % numberOfTuplesPerThread) != 0)
			throw new IllegalArgumentException(String.format("error: number of tuples must be a multiple of %d", 
					numberOfTuplesPerThread));
		
		threads = new int [2];
		threadsPerGroup = new int [2];
		
		threads[0] = threads[1] = records / numberOfTuplesPerThread;
		
		threadsPerGroup[0] = threadsPerGroup[1] = 
				(threads[1] < numberOfThreadsPerGroup) ? threads[1] : numberOfThreadsPerGroup;
		
		numberOfThreadGroups = threads[0] / threadsPerGroup[0];
		
		System.out.println(String.format("[DBG] %d tuples %d threads  %d groups %d threads/group",
				tuples, threads[0], numberOfThreadGroups, threadsPerGroup[0]));
		
		args = new int[3];
		
		args[0] = inputSize;
		args[1] = tuples;
		args[2] = 4 * threadsPerGroup[0] * numberOfTuplesPerThread;
	}
	
	public void setup () {
		
		String source = KernelGenerator.getSelectionOperator (filename, schema, schema, predicate, customPredicate);
		
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery (source, 2, 1, 4);
		
		TheGPU.getInstance().setInput (qid, 0, inputSize);
		
		TheGPU.getInstance().setOutput (qid, 0, 4 * records,              0, 1, 1, 0, 1); /*      Flags */
		TheGPU.getInstance().setOutput (qid, 1, 4 * records,              0, 1, 0, 0, 1); /*    Offsets */
		TheGPU.getInstance().setOutput (qid, 2, 4 * numberOfThreadGroups, 0, 1, 0, 0, 1); /* Partitions */
		TheGPU.getInstance().setOutput (qid, 3, outputSize,               1, 0, 0, 1, 0); /*    Results */
		
		TheGPU.getInstance().setKernelSelect (qid, args);
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
		final StringBuilder s = new StringBuilder();
		s.append("Selection (");
		if (customPredicate != null)
			s.append("\"").append(customPredicate).append("\"");
		else
			s.append(predicate.toString());
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
		TheGPU.getInstance().execute(qid, threads, threadsPerGroup);
		
		if (pipelinedBatch != null)
			pipelinedOperator.processOutput (qid, pipelinedBatch);
		
		api.outputWindowBatchResult (pipelinedBatch);
	}

	public void configureOutput (int queryId) {
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		TheGPU.getInstance().setOutputBuffer(queryId, 3, outputBuffer);
	}
	
	public void processOutput (int queryId, WindowBatch batch) {
		
		IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer(queryId, 3);
		
		// System.out.println(String.format("[DBG] task %10d (\"select\"): output buffer position is %10d", 
		//		batch.getTaskId(), buffer.position()));
		
		batch.setBuffer(buffer);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}
}
