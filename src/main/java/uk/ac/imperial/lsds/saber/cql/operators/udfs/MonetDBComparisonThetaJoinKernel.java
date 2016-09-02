package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.nio.ByteOrder;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.devices.TheGPU;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class MonetDBComparisonThetaJoinKernel implements IOperatorCode {
	
	private static boolean debug = false;
	private static boolean print = false;
	
	private static boolean isFirst = true;
	
	private static final int numberOfThreadsPerGroup = 256;
	private static final int numberOfTuplesPerThread =   2;
	
	private int qid;
	
	private static String filename = SystemConf.SABER_HOME + "/saber/clib/templates/ThetaJoin.cl";
	
	private ITupleSchema left, right;
	private ITupleSchema outputSchema;
	
	private IPredicate predicate;
	private String customPredicate;
	
	private String customCopy = null;
	
	private int batchSize, outputSize;
	
	private int [] args;
	
	private int [] threads;
	private int [] threadsPerGroup;
	
	private int records, numberOfThreadGroups;
	
	IQueryBuffer startPointers, endPointers;
	
	private boolean isPowerOfTwo (int n) {
		if (n == 0)
			return false;
		while (n != 1) {
			if (n % 2 != 0)
				return false;
			n = n / 2;
		}
		return true;
	}
	
	public MonetDBComparisonThetaJoinKernel (ITupleSchema left, ITupleSchema right, IPredicate predicate, String customPredicate, 
		
		int batchSize, int outputSize) {
		
		this.left = left;
		this.right = right;
		
		this.predicate = predicate;
		this.customPredicate = customPredicate;
		
		this.batchSize = batchSize;
		this.outputSize = outputSize;
		
//		int [] offsets = new int [2];
//		offsets [0] = 0;
//		offsets [1] = 4;
//		outputSchema = new TupleSchema (offsets, 8);
//		
//		outputSchema.setAttributeType(0, PrimitiveType.INT);
//		outputSchema.setAttributeType(1, PrimitiveType.INT);
//		
//		StringBuilder b = new StringBuilder();
//		b.append("\tq->tuple._1 = p1->tuple._1;\n");
//		b.append("\tq->tuple._2 = p2->tuple._1;\n");
//		customCopy = b.toString();
		
		outputSchema = ExpressionsUtil.mergeTupleSchemas(left, right);
		
		int tupleSize = left.getTupleSize();
		
		if ((batchSize % tupleSize) != 0)
			throw new IllegalArgumentException("error: kernel input size is not a multiple of tuple size");
		
		int tuples = batchSize / tupleSize;
		
		records = tuples;
		while (! isPowerOfTwo(records))
			++records;
		
		if (records < numberOfTuplesPerThread)
			throw new IllegalArgumentException(String.format("error: number of tuples must be greater than %d", 
					numberOfTuplesPerThread));
		
		if ((records % numberOfTuplesPerThread) != 0)
			throw new IllegalArgumentException(String.format("error: number of tuples must be a multiple of %d", 
					numberOfTuplesPerThread));
		
		threads = new int [4];
		threadsPerGroup = new int [4];
		
		threads[0] = records;
		threads[1] = threads[2] = records / numberOfTuplesPerThread; /* Scan and compact kernels */
		threads[3] = records;
		
		for (int i = 0; i < 4; ++i)
			threadsPerGroup[i] = numberOfThreadsPerGroup;
		
		numberOfThreadGroups = threads[1] / threadsPerGroup[1];
		
		System.out.println(String.format("[DBG] %d tuples %d threads  %d groups %d threads/group",
				tuples, threads[0], numberOfThreadGroups, threadsPerGroup[0]));
		
		args = new int [4];
		
		args[0] = batchSize /  left.getTupleSize();
		args[1] = batchSize / right.getTupleSize();
		args[2] = outputSize;
		args[3] = 4 * threadsPerGroup[0] * numberOfTuplesPerThread;
		
		startPointers = new UnboundedQueryBuffer (-1, 4 * tuples, false); 
		  endPointers = new UnboundedQueryBuffer (-1, 4 * tuples, false);
		
		startPointers.getByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
		  endPointers.getByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
	}
	
	public void setup () {
		
		String source = KernelGenerator.getThetaJoinOperator (filename, left, right, outputSchema, predicate, 
				customPredicate, customCopy);
		
		System.out.println(source);
		
		qid = TheGPU.getInstance().getQuery(source, 4, 4, 4);
		
		TheGPU.getInstance().setInput(qid, 0, batchSize);
		TheGPU.getInstance().setInput(qid, 1, batchSize);
		
		/* Start and end pointers */
		TheGPU.getInstance().setInput(qid, 2, startPointers.capacity());
		TheGPU.getInstance().setInput(qid, 3,   endPointers.capacity());
		
		TheGPU.getInstance().setOutput(qid, 0,              records * 4, 0, 1, 1, 0, 1); /* counts */
		TheGPU.getInstance().setOutput(qid, 1,              records * 4, 0, 1, 0, 0, 1); /* offsets */
		TheGPU.getInstance().setOutput(qid, 2, numberOfThreadGroups * 4, 0, 1, 0, 0, 1); /* partitions */
		TheGPU.getInstance().setOutput(qid, 3,           outputSize    , 1, 0, 0, 1, 0);
		
		TheGPU.getInstance().setKernelThetaJoin(qid, args);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		
		IQueryBuffer inputBuffer1 = first.getBuffer();
		int start1 = first.getBufferStartPointer();
		int end1 = first.getBufferEndPointer() + left.getTupleSize();
		
		IQueryBuffer inputBuffer2 = second.getBuffer();
		int start2 = second.getBufferStartPointer();
		int end2 = second.getBufferEndPointer() + right.getTupleSize();
		
		if (debug) {
			System.out.println(String.format("[DBG] task %6d 1st batch [%10d, %10d] %10d tuples / 2nd batch [%10d, %10d] %10d tuples", 
				first.getTaskId(), 
				start1, 
				end1,
				(end1 - start1) / left.getTupleSize(),
				start2, 
				end2,
				(end2 - start2)/ right.getTupleSize()
				));
		}
		
		TheGPU.getInstance().setInputBuffer(qid, 0, inputBuffer1, start1, end1);
		TheGPU.getInstance().setInputBuffer(qid, 1, inputBuffer2, start2, end2);
		
		if (isFirst) {
			clearPointers ();
			computePointers (first, second);
			normalisePointers (start2);
			if (print)
				printPointers ();
			isFirst = false;
		}
		
		TheGPU.getInstance().setInputBuffer (qid, 2, startPointers);
		TheGPU.getInstance().setInputBuffer (qid, 3,   endPointers);
		
		/* Set output for a previously executed operator */
		
		WindowBatch pipelinedBatch = TheGPU.getInstance().shiftUp(first);
		
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
		
		if (debug)
			System.out.println(String.format("[DBG] task %10d (\"join\"): output buffer position is %10d", 
					batch.getTaskId(), buffer.position()));
		
		batch.setBuffer(buffer);
	}
	
	public void processData(WindowBatch batch, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operator on a single stream");
	}
	
	private void clearPointers () {
		
		startPointers.clear();
		  endPointers.clear();
		
		while (startPointers.hasRemaining() && endPointers.hasRemaining()) {
			startPointers.putInt(-1);
			  endPointers.putInt(-1);
		}
	}
	
	private void normalisePointers (int norm) {
		if (debug)
			System.out.println(String.format("[DBG] offset is %d", norm));
		int tuples = batchSize / left.getTupleSize();
		int offset;
		for (int tid = 0; tid < tuples; ++tid) {
			offset = 4 * tid;
			startPointers.putInt(offset, startPointers.getInt(offset) - norm);
			  endPointers.putInt(offset,   endPointers.getInt(offset) - norm);
		}
	}
	
	private void printPointers () {
		int tuples = batchSize / left.getTupleSize();
		int offset = 0;
		for (int tid = 0; tid < tuples; ++tid) {
			offset = 4 * tid;
			System.out.println(String.format("batch-1 tuple %6d batch-2 window [%10d, %10d]",
				tid, startPointers.getInt(offset), endPointers.getInt(offset)));
		}
	}
	
	private void computePointers (WindowBatch batch1, WindowBatch batch2) {
		
		startPointers.clear();
		endPointers.clear();
		
		int currentIndex1 = batch1.getBufferStartPointer();
		int currentIndex2 = batch2.getBufferStartPointer();

		int endIndex1 = batch1.getBufferEndPointer() + 32;
		int endIndex2 = batch2.getBufferEndPointer() + 32;
		
		int currentWindowStart1 = currentIndex1;
		int currentWindowStart2 = currentIndex2;
		
		int currentWindowEnd1 = currentIndex1;
		int currentWindowEnd2 = currentIndex2;

		ITupleSchema schema1 = batch1.getSchema();
		ITupleSchema schema2 = batch2.getSchema();

		int tupleSize1 = schema1.getTupleSize();
		int tupleSize2 = schema2.getTupleSize();

		WindowDefinition windowDef1 = batch1.getWindowDefinition();
		WindowDefinition windowDef2 = batch2.getWindowDefinition();

		long currentTimestamp1, startTimestamp1;
		long currentTimestamp2, startTimestamp2;
		
		/* Is one of the windows empty? */
		if (currentIndex1 == endIndex1 || currentIndex2 == endIndex2) {
			System.err.println("warning: empty window");
			return;
		}
		
		int __firstTupleIndex = 0;
		
		while (currentIndex1 < endIndex1 && currentIndex2 <= endIndex2) {
			/*
			 * Get time stamps of currently processed tuples in either batch
			 */
			currentTimestamp1 = getTimestamp(batch1, currentIndex1, 0);
			currentTimestamp2 = getTimestamp(batch2, currentIndex2, 0);
			/*
			 * Move in first batch?
			 */
			if (currentTimestamp1 < currentTimestamp2 
					|| (currentTimestamp1 == currentTimestamp2 && currentIndex2 >= endIndex2)) {
					
				startPointers.putInt(__firstTupleIndex * 4, currentWindowStart2);
				endPointers.putInt(__firstTupleIndex * 4,   currentWindowEnd2);
				  
				__firstTupleIndex++;
				
				/* Add current tuple to window over first batch */
				currentWindowEnd1 = currentIndex1;
	
				/* Remove old tuples in window over first batch */
				if (windowDef1.isRowBased()) {
					
					if ((currentWindowEnd1 - currentWindowStart1) / tupleSize1 > windowDef1.getSize()) 
						currentWindowStart1 += windowDef1.getSlide() * tupleSize1;
					
				} else 
				if (windowDef1.isRangeBased()) {
					
					startTimestamp1 = getTimestamp(batch1, currentWindowStart1, 0);
					
					while (startTimestamp1 < currentTimestamp1 - windowDef1.getSize()) {
						
						currentWindowStart1 += tupleSize1;
						startTimestamp1 = getTimestamp(batch1, currentWindowStart1, 0);
					}
				}
				
				/* Remove old tuples in window over second batch (only for range windows) */
				if (windowDef2.isRangeBased()) {
					
					startTimestamp2 = getTimestamp(batch2, currentWindowStart2, 0);
					
					while (startTimestamp2 < currentTimestamp1 - windowDef2.getSize()) {
						
						currentWindowStart2 += tupleSize2;
						startTimestamp2 = getTimestamp(batch2, currentWindowStart2, 0);
					}
				}
					
				/* Do the actual move in first window batch */
				currentIndex1 += tupleSize1;
				
			} else { /* Move in second batch! */
				
				for (int i = currentWindowStart1; i < currentWindowEnd1; i += tupleSize1) {
					
					int __tmpIndex = (i - batch1.getBufferStartPointer()) / tupleSize1;
					endPointers.putInt(__tmpIndex * 4, currentIndex2);
				}
				
				/* Add current tuple to window over second batch */
				currentWindowEnd2 = currentIndex2;
				
				/* Remove old tuples in window over second batch */
				if (windowDef2.isRowBased()) {
					
					if ((currentWindowEnd2 - currentWindowStart2) / tupleSize2 > windowDef2.getSize()) 
						currentWindowStart2 += windowDef2.getSlide() * tupleSize2;
					
				} else 
				if (windowDef2.isRangeBased()) {
					
					startTimestamp2 = getTimestamp(batch2, currentWindowStart2, 0);
					
					while (startTimestamp2 < currentTimestamp2 - windowDef2.getSize()) {
						
						currentWindowStart2 += tupleSize2;
						startTimestamp2 = getTimestamp(batch2, currentWindowStart2, 0);
					}
				}
				
				/* Remove old tuples in window over first batch (only for range windows) */
				if (windowDef1.isRangeBased()) {
					
					startTimestamp1 = getTimestamp(batch1, currentWindowStart1, 0);
					
					while (startTimestamp1 < currentTimestamp2 - windowDef1.getSize()) {
						
						currentWindowStart1 += tupleSize1;
						startTimestamp1 = getTimestamp(batch1, currentWindowStart1, 0);
					}
				}
					
				/* Do the actual move in second window batch */
				currentIndex2 += tupleSize2;
			}
		}
	}
	
	private long getTimestamp (WindowBatch batch, int index, int attribute) {
		long value = batch.getLong(index, attribute);
		if (SystemConf.LATENCY_ON)
			value = (long) Utils.getTupleTimestamp(value);
		return value;
	}
}
