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
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class AggregationKernel implements IOperatorCode, IAggregateOperator {
	
	private static final boolean debug = false;
	
	private static final int numberOfThreadsPerGroup = 256;
	
	private int qid;
	
	private static String filename = SystemConf.SABER_HOME + "/clib/templates/Aggregation.cl";
	
	WindowDefinition windowDefinition;
	
	private AggregationType [] aggregationTypes;

	private FloatColumnReference [] aggregationAttributes;
	
	private LongColumnReference timestampReference;
	
	private Expression [] groupByAttributes;
	
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
	
	public AggregationKernel (WindowDefinition windowDefinition, 

		AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, Expression [] groupByAttributes,
		
		ITupleSchema inputSchema, int inputSize) {
		
		this.inputSchema = inputSchema;
		this.inputSize = inputSize;
		
		this.windowDefinition = windowDefinition;
		this.aggregationTypes = aggregationTypes;
		this.aggregationAttributes = aggregationAttributes;
		this.groupByAttributes = groupByAttributes;
		
		timestampReference = new LongColumnReference(0);

		/* Create output schema */

		int n = 1 + this.groupByAttributes.length + this.aggregationAttributes.length;

		Expression [] outputAttributes = new Expression[n];

		/* The first attribute is the timestamp */
		outputAttributes[0] = timestampReference;

		keyLength = 0;

		for (int i = 1; i <= this.groupByAttributes.length; ++i) {

			Expression e = this.groupByAttributes[i - 1];
			     if (e instanceof   IntExpression) { outputAttributes[i] = new   IntColumnReference(i); keyLength += 4; }
			else if (e instanceof  LongExpression) { outputAttributes[i] = new  LongColumnReference(i); keyLength += 8; }
			else if (e instanceof FloatExpression) { outputAttributes[i] = new FloatColumnReference(i); keyLength += 4; }
			else
				throw new IllegalArgumentException("error: invalid group-by attribute");
		}

		for (int i = groupByAttributes.length + 1; i < n; ++i)
			outputAttributes[i] = new FloatColumnReference(i);

		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		valueLength = 4 * aggregationTypes.length;
	}

	public boolean hasGroupBy () {
		return true;
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
		s.append("[Partial window u-aggregation] ");
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
		
		threads = new int [9];
		threadsPerGroup = new int [9];
		
		/* TODO 
		 * 
		 * The number of threads should be greater or equal to the number of 
		 * intermediate tuples in each output buffer.
		 */
		
		for (int i = 0; i < threads.length; ++i)
			threads[i] = tuples;
		
		for (int i = 0; i < threadsPerGroup.length; ++i)
			threadsPerGroup[i] = numberOfThreadsPerGroup;
		
		args1 = new int [6];
		
		int outputSize = SystemConf.UNBOUNDED_BUFFER_SIZE;
		int  tableSize = SystemConf.HASH_TABLE_SIZE;
		
		if ((outputSize % tableSize) != 0)
			throw new IllegalArgumentException("error: kernel output size is not a multiple of hash table size");
		
		args1[0] = tuples;
		
		args1[1] = inputSize;
		args1[2] = outputSize;
		
		args1[3] = tableSize;
		args1[4] = SystemConf.PARTIAL_WINDOWS;
		
		args1[5] = keyLength * numberOfThreadsPerGroup;
		
		args2 = new long [2];
		
		args2[0] = 0; /* Previous pane id   */
		args2[1] = 0; /* Start offset */
		
		String source = 
			KernelGenerator.getAggregationOperator
				(filename, inputSchema, outputSchema, 
						windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
			
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery(source, 9, 1, 9);
		
		TheGPU.getInstance().setInput (qid, 0, inputSize);
		
		/* Set outputs */
		
		int windowPointersSize = 4 * SystemConf.PARTIAL_WINDOWS;
		
		TheGPU.getInstance().setOutput(qid, 0, windowPointersSize, 0, 1, 0, 0, 1);
		TheGPU.getInstance().setOutput(qid, 1, windowPointersSize, 0, 1, 0, 0, 1);
		
		int failedFlagsSize = 4 * tuples; /* One int per tuple */
		
		TheGPU.getInstance().setOutput(qid, 2, failedFlagsSize, 1, 1, 0, 0, 1);
		
		int offsetSize = 16; /* The size of two longs */
		
		TheGPU.getInstance().setOutput(qid, 3, offsetSize, 0, 1, 0, 0, 1);
		
		int windowCountsSize = 24; /* 4 integers, +1 that is the complete windows mark, +1 that is the mark */
		windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
		
		TheGPU.getInstance().setOutput(qid, 4, windowCountsSize, 0, 0, 1, 0, 1);
		
		/* Set partial window results */
		TheGPU.getInstance().setOutput(qid, 5, outputSize, 1, 0, 0, 1, 1);
		TheGPU.getInstance().setOutput(qid, 6, outputSize, 1, 0, 0, 1, 1);
		TheGPU.getInstance().setOutput(qid, 7, outputSize, 1, 0, 0, 1, 1);
		TheGPU.getInstance().setOutput(qid, 8, outputSize, 1, 0, 0, 1, 1);
		
		TheGPU.getInstance().setKernelAggregate (qid, args1, args2);
		
		
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
		TheGPU.getInstance().executeAggregate (qid, threads, threadsPerGroup, args2);
		
		if (pipelinedBatch != null)
			pipelinedOperator.processOutput (qid, pipelinedBatch);
		
		api.outputWindowBatchResult (pipelinedBatch);
	}
	
	public void configureOutput (int queryId) {
		
		TheGPU.getInstance().setOutputBuffer(queryId, 4, windowCounts);
		
		/* Closing, pending, complete, and opening windows */
		
		IQueryBuffer outputBuffer5 = UnboundedQueryBufferFactory.newInstance();
		TheGPU.getInstance().setOutputBuffer(queryId, 5, outputBuffer5);
	}
	
	public void processOutput (int queryId, WindowBatch batch) {
		
		ByteBuffer b4 = windowCounts.getByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
		b4.clear();
		
		int  numberOfClosingWindows  = b4.getInt();
		int  numberOfPendingWindows  = b4.getInt();
		int  numberOfCompleteWindows = b4.getInt();
		int  numberOfOpeningWindows  = b4.getInt();
		
		int completeWindowsPosition = b4.getInt();
		completeWindowsPosition = numberOfCompleteWindows * SystemConf.HASH_TABLE_SIZE;
		
		int outputBufferPosition    = b4.getInt();
		
		if (debug)
			System.out.println(
				String.format("[DBG] task %6d window counts %6d/%6d/%6d/%6d (%10d bytes of complete windows, %10d bytes)", 
					batch.getTaskId(), 
					numberOfClosingWindows, numberOfCompleteWindows, numberOfPendingWindows, numberOfOpeningWindows, 
					completeWindowsPosition, outputBufferPosition));
		
		IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer(queryId, 5);
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		int length = SystemConf.HASH_TABLE_SIZE;
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
		
		/* Set complete windows */
		/*
		System.out.println("Complete windows start at " + offset);
		int tupleIndex = 0;
		for (int idx = offset; idx < (offset + SystemConf.HASH_TABLE_SIZE); idx += 32) {
			int mark = buffer.getInt(idx + 0);
			if (mark > 0) {
				long timestamp = buffer.getLong(idx + 8);
				//
				// int key_1
				// float value1
				// float value2
				// int count
				//
				int key = buffer.getInt(idx + 16);
				float val1 = buffer.getFloat(idx + 20);
				float val2 = buffer.getFloat(idx + 24);
				int count = buffer.getInt(idx + 28);
				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d", 
					tupleIndex, 
					Integer.reverseBytes(mark),
					Long.reverseBytes(timestamp),
					key,
					0F,
					0F,
					Integer.reverseBytes(count)
				));
			}
			tupleIndex ++;
		}
		
		System.exit(1);
		*/
		result = completeWindows.getBuffer();
		if (numberOfCompleteWindows > 0) {
			result.put(buffer, offset, completeWindowsPosition);
			completeWindows.setCount (numberOfCompleteWindows, completeWindowsPosition);
			offset += completeWindowsPosition;
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
		
		buffer.release();
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}
}
