package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class ThetaJoin implements IOperatorCode {
	
	private static boolean debug = false;
	private static boolean monitorSelectivity = false;
	
	private long invoked = 0L;
	private long matched = 0L;
	
	private IPredicate predicate;

	private ITupleSchema outputSchema = null;
	
	public ThetaJoin(ITupleSchema schema1, ITupleSchema schema2, IPredicate predicate) {
		
		this.predicate = predicate;
		
		outputSchema = ExpressionsUtil.mergeTupleSchemas(schema1, schema2);
	}
	
	public void processData (WindowBatch batch1, WindowBatch batch2, IWindowAPI api) {

		int currentIndex1 = batch1.getBufferStartPointer();
		int currentIndex2 = batch2.getBufferStartPointer();

		int endIndex1 = batch1.getBufferEndPointer() + 32;
		int endIndex2 = batch2.getBufferEndPointer() + 32;
		
		int currentWindowStart1 = currentIndex1;
		int currentWindowEnd1   = currentIndex1;
		
		int currentWindowStart2 = currentIndex2;
		int currentWindowEnd2   = currentIndex2;
		
		IQueryBuffer buffer1 = batch1.getBuffer();
		IQueryBuffer buffer2 = batch2.getBuffer();
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema1 = batch1.getSchema();
		ITupleSchema schema2 = batch2.getSchema();

		int tupleSize1 = schema1.getTupleSize();
		int tupleSize2 = schema2.getTupleSize();
		
		/* Actual Tuple Size without padding*/
		int pointerOffset1 = tupleSize1 - schema1.getPadLength();
		int pointerOffset2 = tupleSize2 - schema2.getPadLength();

		WindowDefinition windowDef1 = batch1.getWindowDefinition();
		WindowDefinition windowDef2 = batch2.getWindowDefinition();
		
		if (debug) {
			System.out.println(
				String.format("[DBG] t %6d batch-1 [%10d, %10d] %10d tuples [f %10d] / batch-2 [%10d, %10d] %10d tuples [f %10d]", 
					batch1.getTaskId(), 
					currentIndex1, 
					endIndex1, 
					(endIndex1 + tupleSize1 - currentIndex1)/tupleSize1,
					batch1.getFreePointer(),
					currentIndex2, 
					endIndex2,
					(endIndex2 + tupleSize2 - currentIndex2)/tupleSize2,
					batch2.getFreePointer()
				)
			);
		}
		
		long currentTimestamp1, startTimestamp1;
		long currentTimestamp2, startTimestamp2;
		
		if (monitorSelectivity)
			invoked = matched = 0L;
		
		// if (currentIndex2 == 0 && endIndex2 == 0)
		//	endIndex2 = tupleSize2;
		
		/* Is one of the windows empty? */
		if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) {
			
			int prevCurrentIndex1 = -1;
			int countMatchPositions = 0;
		
			// Changed <=, <=, || to &&
			// while (currentIndex1 < endIndex1 && currentIndex2 <= endIndex2) {
			// OLD
			while (currentIndex1 < endIndex1 || currentIndex2 < endIndex2) {
				
				// System.out.println(String.format("[DBG] batch-1 index %10d end %10d batch-2 index %10d end %10d",
				//		currentIndex1, endIndex1, currentIndex2, endIndex2));
				
				/* Get timestamps of currently processed tuples in either batch */
				currentTimestamp1 = getTimestamp( batch1,  currentIndex1, 0);
				currentTimestamp2 = getTimestamp(batch2, currentIndex2, 0);
				
				/* Move in first batch? */
				if (
					(currentTimestamp1 < currentTimestamp2) || 
					(currentTimestamp1 == currentTimestamp2 && currentIndex2 >= endIndex2)) {
					
					/* Scan second window */
					
					// Changed here: <=
					// for (int i = currentWindowStart2; i <= currentWindowEnd2; i += tupleSize2) {
					// OLD
					for (int i = currentWindowStart2; i < currentWindowEnd2; i += tupleSize2) {
						
						// System.out.println(String.format("[DBG] 1st window index %10d 2nd window index %10d", 
						//		currentIndex1, i));
						
//						System.out.println(String.format("tuple %6d %010d: %2d,%2d,%2d,%2d,%2d,%2d,%2d | %010d: %2d,%2d,%2d,%2d,%2d,%2d,%2d | %5s", 
//								currentIndex1 / 32,
//								currentIndex1,
//								buffer1.getByteBuffer().getLong(currentIndex1 +  0),
//								buffer1.getByteBuffer().getInt (currentIndex1 +  8),
//								buffer1.getByteBuffer().getInt (currentIndex1 + 12),
//								buffer1.getByteBuffer().getInt (currentIndex1 + 16),
//								buffer1.getByteBuffer().getInt (currentIndex1 + 20),
//								buffer1.getByteBuffer().getInt (currentIndex1 + 24),
//								buffer1.getByteBuffer().getInt (currentIndex1 + 28),
//								i,
//								buffer2.getByteBuffer().getLong(i +  0),
//								buffer2.getByteBuffer().getInt (i +  8),
//								buffer2.getByteBuffer().getInt (i + 12),
//								buffer2.getByteBuffer().getInt (i + 16),
//								buffer2.getByteBuffer().getInt (i + 20),
//								buffer2.getByteBuffer().getInt (i + 24),
//								buffer2.getByteBuffer().getInt (i + 28),
//								predicate.satisfied (buffer1, schema1, currentIndex1, buffer2, schema2, i)
//								));
						
						if (monitorSelectivity)
							invoked ++;
						
						if (
							predicate == null || 
							predicate.satisfied (buffer1, schema1, currentIndex1, buffer2, schema2, i)
						) {
							
							if (prevCurrentIndex1 != currentIndex1) {
								prevCurrentIndex1 = currentIndex1;
								countMatchPositions ++;
							}
							
							// System.out.println(String.format("[DBG] match at currentIndex1 = %10d (count = %6d)", 
							//		currentIndex1, countMatchPositions));
							
							buffer1.appendBytesTo(currentIndex1, pointerOffset1, outputBuffer);
							buffer2.appendBytesTo(            i, pointerOffset2, outputBuffer);
							/* Write dummy content, if needed */
							outputBuffer.put(outputSchema.getPad());
							
							if (monitorSelectivity)
								matched ++;
						}
					}
					
					/* Add current tuple to window over first batch */
					currentWindowEnd1 = currentIndex1;
					
					/* Remove old tuples in window over first batch */
					if (windowDef1.isRowBased()) {
						
						if ((currentWindowEnd1 - currentWindowStart1) / tupleSize1 > windowDef1.getSize()) 
							currentWindowStart1 += windowDef1.getSlide() * tupleSize1;
						
					} else 
					if (windowDef1.isRangeBased()) {
						
						startTimestamp1 = getTimestamp (batch1, currentWindowStart1, 0);
						
						while (startTimestamp1 < currentTimestamp1 - windowDef1.getSize()) {
							currentWindowStart1 += tupleSize1;
							startTimestamp1 = getTimestamp (batch1, currentWindowStart1, 0);
						}
					}
					
					/* Remove old tuples in window over second batch (only for range windows) */
					if (windowDef2.isRangeBased()) {
						
						startTimestamp2 = getTimestamp (batch2, currentWindowStart2, 0);
						
						while (startTimestamp2 < currentTimestamp1 - windowDef2.getSize()) {
							currentWindowStart2 += tupleSize2;
							startTimestamp2 = getTimestamp (batch2, currentWindowStart2, 0);
						}
					}
					
					/* Do the actual move in first window batch */
					currentIndex1 += tupleSize1;
				}
				else /* Move in second batch */ 
				{
					/* Scan first window */
					
					// System.out.println("[DBG] move in second window...");
					// System.out.println(String.format("[DBG] scan first window: start %10d end %10d", 
					//		currentWindowStart1, currentWindowEnd1));
					
					// Changed here: <=
					// for (int i = currentWindowStart1; i <= currentWindowEnd1; i += tupleSize1) {
					for (int i = currentWindowStart1; i < currentWindowEnd1; i += tupleSize1) {
						
						if (monitorSelectivity)
							invoked ++;
						
						if (
							predicate == null || 
							predicate.satisfied (buffer1, schema1, i, buffer2, schema2, currentIndex2)
						) {
							
							// System.out.println("[DBG] Match in first window...");
							
							buffer1.appendBytesTo(            i, tupleSize1, outputBuffer);
							buffer2.appendBytesTo(currentIndex2, tupleSize2, outputBuffer);
							/* Write dummy content if needed */
							outputBuffer.put(outputSchema.getPad());
							
							if (monitorSelectivity)
								matched ++;
						}
					}
					
					/* Add current tuple to window over second batch */
					currentWindowEnd2 = currentIndex2;
					
					// System.out.println("[DBG] currentWindowStart2 = " + currentWindowStart2);
					// System.out.println("[DBG] currentWindowEnd2   = " + currentWindowEnd2   );
	
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
		
		buffer1.release();
		buffer2.release();

		batch1.setBuffer(outputBuffer);
		batch1.setSchema(outputSchema);
		
		if (debug) 
			System.out.println("[DBG] output buffer position is " + outputBuffer.position());
		
		if (monitorSelectivity) {
			double selectivity = 0D;
			if (invoked > 0)
				selectivity = ((double) matched / (double) invoked) * 100D;
			System.out.println(String.format("[DBG] task %6d %2d out of %2d tuples selected (%4.1f)", 
					batch1.getTaskId(), matched, invoked, selectivity));
		}
		
		/* Print tuples
		outBuffer.close();
		int tid = 1;
		while (outBuffer.hasRemaining()) {
		
			System.out.println(String.format("%03d: %2d,%2d,%2d,%2d,%2d,%2d,%2d | %2d,%2d,%2d,%2d,%2d,%2d,%2d", 
			tid++,
			outBuffer.getByteBuffer().getLong(),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getLong(),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt (),
			outBuffer.getByteBuffer().getInt ()
			));
		}
		*/
		api.outputWindowBatchResult(batch1);
		/*
		System.err.println("Disrupted");
		System.exit(-1);
		*/
	}
	
	private long getTimestamp (WindowBatch batch, int index, int attribute) {
		long value = batch.getLong (index, attribute);
		if (SystemConf.LATENCY_ON)
			return (long) Utils.getTupleTimestamp(value);
		return value;
	}
	
	public void processData(WindowBatch batch, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operator on a single stream");
	}

	public void configureOutput (int queryId) {
		
		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	public void processOutput (int queryId, WindowBatch batch) {
		
		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}
	
	public void setup() {
		
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}
}
