package uk.ac.imperial.lsds.saber.cql.operators.gpu;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class ReductionKernel implements IOperatorCode, IAggregateOperator {
	
	private static final boolean debug = false;
	
	private static final int numberOfThreadsPerGroup = 256;
	
	private int qid;
	
	private static String filename = SystemConf.SABER_HOME + "/clib/templates/Reduction.cl";
	
	WindowDefinition windowDefinition;
	
	private AggregationType [] aggregationTypes;
	
	private FloatColumnReference [] aggregationAttributes;
	
	private LongColumnReference timestampReference;
	
	ITupleSchema inputSchema, outputSchema;
	
	private int keyLength, valueLength;
	
	private int inputSize;
	
	private int  [] args1;
	private long [] args2;
	
	private int [] threads;
	private int [] threadsPerGroup;
	
	/*
	 * The output buffer that holds the number of opening, ..., complete windows
	 * in a batch. This buffer is thread-safe, since only the GPU thread uses it.
	 * 
	 * The buffer is configured during setup().
	 */
	IQueryBuffer windowCounts = null;
	
	public ReductionKernel (WindowDefinition windowDefinition, ITupleSchema inputSchema, int inputSize) {
		
		this.inputSchema = inputSchema;
		this.inputSize = inputSize;
		
		this.windowDefinition = windowDefinition;
		
		aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.CNT;
		
		aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(1);
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[3]; /* +1 for count */
		
		outputAttributes[0] = timestampReference;
		outputAttributes[1] = new FloatColumnReference(1);
		outputAttributes[2] = new IntColumnReference(2); /* count */
		
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		keyLength = 0;
		valueLength = 4;
	}
	
	public ReductionKernel (WindowDefinition windowDefinition, 
		
		AggregationType aggregationType, FloatColumnReference aggregationAttribute, 
		
		ITupleSchema inputSchema, int inputSize) {
		
		this.inputSchema = inputSchema;
		this.inputSize = inputSize;
		
		this.windowDefinition = windowDefinition;
		
		aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = aggregationType;
		
		aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = aggregationAttribute;
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[3]; /* +1 for count */
		
		outputAttributes[0] = timestampReference;
		outputAttributes[1] = new FloatColumnReference(1);
		outputAttributes[2] = new IntColumnReference(2); /* count */
		
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		keyLength = 0;
		valueLength = 4;
	}
	
	public ReductionKernel (WindowDefinition windowDefinition, 

		AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, 
		
		ITupleSchema inputSchema, int inputSize) {
		
		this.inputSchema = inputSchema;
		this.inputSize = inputSize;
		
		this.windowDefinition = windowDefinition;
		this.aggregationTypes = aggregationTypes;
		this.aggregationAttributes = aggregationAttributes;
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[1 + aggregationAttributes.length + 1]; /* +1 for count */

		outputAttributes[0] = timestampReference;
		for (int i = 1; i < outputAttributes.length - 1; ++i)
			outputAttributes[i] = new FloatColumnReference(i);
		outputAttributes[outputAttributes.length - 1] = new IntColumnReference(outputAttributes.length - 1); /* count */

		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		keyLength = 0;
		valueLength = 4 * aggregationAttributes.length;
	}
	
	public boolean hasGroupBy () {
		return false;
	}
	
	public ITupleSchema getOutputSchema () {
		return outputSchema;
	}
	
	public int getKeyLength () {
		return keyLength;
	}
	
	public int getValueLength () {
		return valueLength;
	}

	public int numberOfValues () {
		return aggregationAttributes.length;
	}
	
	public AggregationType getAggregationType () {
		return getAggregationType (0);
	}
	
	public AggregationType getAggregationType (int idx) {
		if (idx < 0 || idx > aggregationTypes.length - 1)
			throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
		return aggregationTypes[idx];
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
		s.append("[Partial window reduction] ");
		for (int i = 0; i < aggregationTypes.length; ++i)
			s.append(aggregationTypes[i].asString(aggregationAttributes[i].toString())).append(" ");
		return s.toString();
	}
	
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
	
	public void setup () {
		
		int tupleSize = inputSchema.getTupleSize();
		
		if ((inputSize % tupleSize) != 0)
			throw new IllegalArgumentException("error: kernel input size is not a multiple of tuple size");
		
		if (! isPowerOfTwo(inputSize))
			throw new IllegalArgumentException("error: kernel input size must be a power of 2");
		
		int tuples = inputSize / tupleSize;
		
		threads = new int [4];
		threadsPerGroup = new int [4];
		
		threads[0] = threads[1] = threads[2] = threads[3] = tuples;
		
		threadsPerGroup[0] = threadsPerGroup[1] = threadsPerGroup[2] = threadsPerGroup[3] = numberOfThreadsPerGroup;
		
		args1 = new int [4];
		
		args1[0] = tuples;
		args1[1] = inputSize;
		args1[2] = SystemConf.PARTIAL_WINDOWS;
		args1[3] = outputSchema.getTupleSize() * numberOfThreadsPerGroup;
		
		args2 = new long [2];
		
		args2[0] = 0; /* Previous pane id   */
		args2[1] = 0; /* Batch start offset */
		
		String source = KernelGenerator.getReductionOperator 
				(filename, inputSchema, outputSchema, windowDefinition, aggregationTypes, aggregationAttributes);
		
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery (source, 4, 1, 5);
		
		TheGPU.getInstance().setInput (qid, 0, inputSize);
		
		int windowPointersSize = 4 * SystemConf.PARTIAL_WINDOWS;
		
		TheGPU.getInstance().setOutput(qid, 0, windowPointersSize, 0, 1, 0, 0, 1);
		TheGPU.getInstance().setOutput(qid, 1, windowPointersSize, 0, 1, 0, 0, 1);
		
		int offsetSize = 16; /* The size of two longs */
		
		TheGPU.getInstance().setOutput(qid, 2, offsetSize, 0, 1, 0, 0, 1);
		
		int windowCountsSize = 20; /* 4 integers, +1 that is the mark */
		windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
		
		TheGPU.getInstance().setOutput(qid, 3, windowCountsSize, 0, 0, 1, 0, 1);
		
		int outputSize = SystemConf.UNBOUNDED_BUFFER_SIZE;
		
		TheGPU.getInstance().setOutput(qid, 4, outputSize, 1, 0, 0, 1, 0);
		
		TheGPU.getInstance().setKernelReduce (qid, args1, args2);
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		/* Set input */
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		int start = batch.getBufferStartPointer();
		int end   = batch.getBufferEndPointer();
		
		TheGPU.getInstance().setInputBuffer(qid, 0, inputBuffer, start, end);
		
		/* Previous pane id, based on stream start pointer, s */
		args2[0] = -1L;
		long s = batch.getStreamStartPointer();
		int  t = inputSchema.getTupleSize();
		long p = batch.getWindowDefinition().getPaneSize();
		if (batch.getStreamStartPointer() > 0) { 
			if (batch.getWindowDefinition().isRangeBased()) {
				int offset = start - t;
				args2[0] = batch.getTimestamp(offset) / p;
			} else {
				args2[0] = ((s / (long) t) / p) - 1;
			}
		}
		args2[1] = s;
		
		/* Set output for a previously executed operator */
		
		WindowBatch pipelinedBatch = TheGPU.getInstance().shiftUp(batch);
		
		IOperatorCode pipelinedOperator = null; 
		if (pipelinedBatch != null) {
			pipelinedOperator = pipelinedBatch.getQuery().getMostUpstreamOperator().getGpuCode();
			pipelinedOperator.configureOutput (qid);
		}
		
		/* Execute */
		TheGPU.getInstance().executeReduce (qid, threads, threadsPerGroup, args2);
		
		if (pipelinedBatch != null)
			pipelinedOperator.processOutput (qid, pipelinedBatch);
		
		api.outputWindowBatchResult (pipelinedBatch);
	}

	public void configureOutput (int queryId) {
		
		TheGPU.getInstance().setOutputBuffer(queryId, 3, windowCounts);
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		TheGPU.getInstance().setOutputBuffer(queryId, 4, outputBuffer);
	}

	public void processOutput (int queryId, WindowBatch batch) {
		
		ByteBuffer b3 = windowCounts.getByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
		b3.clear();
		
		int  numberOfClosingWindows  = b3.getInt();
		int  numberOfPendingWindows  = b3.getInt();
		int  numberOfCompleteWindows = b3.getInt();
		int  numberOfOpeningWindows  = b3.getInt();

		int outputBufferPosition = b3.getInt();
		
		if (debug)
			System.out.println(String.format("[DBG] task %6d window counts %6d/%6d/%6d/%6d (%10d bytes)", 
				batch.getTaskId(), 
				numberOfClosingWindows, numberOfCompleteWindows, numberOfPendingWindows, numberOfOpeningWindows, 
				outputBufferPosition));
		
		IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer(queryId, 4);
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		int length = outputSchema.getTupleSize();
		int offset = 0;
		
		IQueryBuffer result;
		
		result = closingWindows.getBuffer();
		for (int i = 0; i < numberOfClosingWindows; ++i) {
			closingWindows.increment();
			result.put(buffer, offset, length);
			offset += length;
		}
		
		result = pendingWindows.getBuffer();
		for (int i = 0; i < numberOfPendingWindows; ++i) {
			if (i < 1) {
				pendingWindows.increment();
				result.put(buffer, offset, length);
			}
			offset += length;
		}
		
		result = completeWindows.getBuffer();
		for (int i = 0; i < numberOfCompleteWindows; ++i) {
			completeWindows.increment();
			result.put(buffer, offset, length);
			offset += length;
		}
		
		result = openingWindows.getBuffer();
		for (int i = 0; i < numberOfOpeningWindows; ++i) {
			openingWindows.increment();
			result.put(buffer, offset, length);
			offset += length;
		}
		
		batch.setSchema(outputSchema);
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
		
		/* Output buffer contents have been copied to the various partial window result buffers */
		buffer.release();
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}
}
