package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.nio.ByteBuffer;
//import java.util.List;
import java.util.Random;

//import com.google.common.collect.Multimap;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.processors.HashMap;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class YahooBenchmarkOp implements IOperatorCode, IAggregateOperator {
	
	private static final boolean debug = false;
	
	private static boolean monitorSelectivity = false;
	
	private long invoked = 0L;
	private long matched = 0L;
	
	private IQueryBuffer relationBuffer;
	
	private WindowDefinition windowDefinition;
	
	private IPredicate selectPredicate = null;
	
	private Expression [] expressions;
	
	private ITupleSchema projectedSchema;
	
	private IPredicate joinPredicate = null;
	
	private ITupleSchema joinedSchema;
	
	private ITupleSchema relationSchema;
	
	private ITupleSchema outputSchema;
	
	private boolean incrementalProcessing;
	
	private AggregationType [] aggregationTypes;

	private FloatColumnReference [] aggregationAttributes;
	
	private LongColumnReference timestampReference;
	
	private Expression [] groupByAttributes;
	private boolean groupBy = false;
	
	private int keyLength, valueLength;
	
	private boolean isV2 = true;
	
	/* Thread local variables */
	private ThreadLocal<float   []> tl_values;
	private ThreadLocal<int     []> tl_counts;
	private ThreadLocal<byte    []> tl_tuplekey;
	private ThreadLocal<boolean []> tl_found;
		
	//private Multimap<Integer,Integer> multimap;
	private HashMap hashMap;
	
	public YahooBenchmarkOp (
			ITupleSchema inputSchema,
			IPredicate selectPredicate, 
			Expression [] expressions, 
			IPredicate joinPredicate,
			ITupleSchema relationSchema,
			IQueryBuffer relationBuffer,
			// Multimap<Integer,Integer> multimap,
			HashMap hashMap,
			WindowDefinition windowDefinition,
			AggregationType [] aggregationTypes, 
			FloatColumnReference [] aggregationAttributes, 
			Expression [] groupByAttributes
			) { 
		this(inputSchema, selectPredicate, expressions, joinPredicate, relationSchema,
				relationBuffer, hashMap, windowDefinition, aggregationTypes, 
				aggregationAttributes, groupByAttributes, false);
	}
	public YahooBenchmarkOp (
			ITupleSchema inputSchema,
			IPredicate selectPredicate, 
			Expression [] expressions, 
			IPredicate joinPredicate,
			ITupleSchema relationSchema,
			IQueryBuffer relationBuffer,
			// Multimap<Integer,Integer> multimap,
			HashMap hashMap,
			WindowDefinition windowDefinition,
			AggregationType [] aggregationTypes, 
			FloatColumnReference [] aggregationAttributes, 
			Expression [] groupByAttributes,
			boolean isV2
			) {
		
		this.windowDefinition = windowDefinition;
		this.selectPredicate = selectPredicate;
		this.expressions = expressions;
		
		/* This is the output of projection */
		this.projectedSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		
		this.joinPredicate = joinPredicate;
		this.relationSchema = relationSchema;
		this.relationBuffer = relationBuffer;		
		// this.multimap = multimap;
		this.hashMap = hashMap;
		
		/* This is the output of join */
		this.joinedSchema = ExpressionsUtil.mergeTupleSchemas(projectedSchema, relationSchema);
	    //================================================================================

		this.outputSchema = this.joinedSchema;
		
		this.isV2 = isV2;
		
	    //================================================================================
		/* Initialize Aggregation variables*/
		this.aggregationTypes = aggregationTypes;
		this.aggregationAttributes = aggregationAttributes;
		this.groupByAttributes = groupByAttributes;
		
		if (groupByAttributes != null)
			groupBy = true;
		else 
			groupBy = false;
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
/*		int numberOfKeyAttributes;
		if (groupByAttributes != null)
			numberOfKeyAttributes = groupByAttributes.length;
		else
			numberOfKeyAttributes = 0;
		
		int n = 1 + numberOfKeyAttributes + aggregationAttributes.length;
		
		int numberOfOutputAttributes = n;
		if (groupByAttributes == null)
			numberOfOutputAttributes += 1;*/ /* +1 for count */
		
/*		Expression [] outputAttributes = new Expression[numberOfOutputAttributes];
*/		
		/* The first attribute is the timestamp */
/*		outputAttributes[0] = timestampReference;
		
		keyLength = 0;
		
		if (numberOfKeyAttributes > 0) {
			
			for (int i = 1; i <= numberOfKeyAttributes; ++i) {
				
				Expression e = groupByAttributes[i - 1];
				     if (e instanceof      IntExpression) { outputAttributes[i] = new   IntColumnReference(i); keyLength += 4; }
				else if (e instanceof     LongExpression) { outputAttributes[i] = new  LongColumnReference(i); keyLength += 8; }
				else if (e instanceof    FloatExpression) { outputAttributes[i] = new FloatColumnReference(i); keyLength += 4; }
				else if (e instanceof LongLongExpression) { outputAttributes[i] = new  LongColumnReference(i); keyLength += 16; }
				else
					throw new IllegalArgumentException("error: invalid group-by attribute");
			}
		}
		
		for (int i = numberOfKeyAttributes + 1; i < n; ++i)
			outputAttributes[i] = new FloatColumnReference(i);*/
		
		/* Set count attribute */
		/*if (groupByAttributes == null)
			outputAttributes[numberOfOutputAttributes - 1] = new IntColumnReference(numberOfOutputAttributes - 1);
		*/
		/* Compute output schema */
		/*this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		boolean containsIncrementalAggregationType = true;
		for (int i = 0; i < aggregationTypes.length; ++i) {
			if (
				aggregationTypes[i] != AggregationType.CNT && 
				aggregationTypes[i] != AggregationType.SUM && 
				aggregationTypes[i] != AggregationType.AVG) {
				
				containsIncrementalAggregationType = false;
				break;
			}
		}*/
		
		/* Compute windows incrementally? */
		/*if (containsIncrementalAggregationType) {
			System.out.println("[DBG] operator contains incremental aggregation type");
			this.incrementalProcessing = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
		} else {
			this.incrementalProcessing = false;
		}
		
		valueLength = 4 * aggregationTypes.length;
				
		tl_values = new ThreadLocal<float []> () {
			@Override protected float [] initialValue () {
				return new float [numberOfValues()];
		    }
		};
		
		tl_counts = new ThreadLocal<int []> () {
			@Override protected int [] initialValue () {
				return new int [numberOfValues()];
		    }
		};
		
		if (groupByAttributes != null) {
			tl_tuplekey = new ThreadLocal <byte []> () {
				@Override protected byte [] initialValue () {
					return new byte [keyLength];
				}
			};
		
			tl_found = new ThreadLocal<boolean []> () {
				@Override protected boolean [] initialValue () {
					return new boolean [1];
				}
			};
		}	*/
	}
	
	@Override
	public boolean hasGroupBy () {
		return groupBy;
	}

	@Override
	public ITupleSchema getOutputSchema () {
		return outputSchema;
	}

	@Override
	public int getKeyLength() {
		return keyLength;
	}

	@Override
	public int getValueLength() {
		return valueLength;
	}

	@Override
	public int numberOfValues() {
		return aggregationAttributes.length;
	}

	@Override
	public AggregationType getAggregationType() {
		return getAggregationType (0);
	}

	@Override
	public AggregationType getAggregationType(int idx) {
		if (idx < 0 || idx > aggregationTypes.length - 1)
			throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
		return aggregationTypes[idx];
	}

	@Override
	public void processData(WindowBatch batch, IWindowAPI api) {
		
		boolean useCalc = true;
		if (useCalc && selectPredicate != null && expressions != null)
			calc (batch, api);
		else {
			/* Select */
			if (selectPredicate != null)
				select (batch, api);
		
		
			/* Project */
			if (expressions != null)
				project (batch, api);
		
		}
		/* Hash Join */
		if (joinPredicate != null)
			hashJoin (batch, api);
		
		
		/* Aggregate */
/*		batch.initPartialWindowPointers();
		
		if (debug) {
			System.out.println(
				String.format("[DBG] hash-join task %6d: batch starts at %10d (%10d) ends at %10d (%10d)", batch.getTaskId(),
				batch.getBufferStartPointer(),
				batch.getStreamStartPointer(),
				batch.getBufferEndPointer(),
				batch.getStreamEndPointer()
				)
			);
		}
		
		if (groupBy == false) {
			System.err.println("You should group by campaign_id and ad_id!");
			System.exit(-1);
		} else {
			if (incrementalProcessing) { 
				System.err.println("You should use a tumbling Window!");
				System.exit(-1);
				processDataPerWindowIncrementallyWithGroupBy (batch, api);
			} else {
				processDataPerWindowWithGroupBy (batch, api);
			}
		}
		
		if (debug) {
			System.out.println(
				String.format("[DBG] aggregation task %6d: %4d closing %4d pending %4d complete and %4d opening windows]", batch.getTaskId(),
					batch.getClosingWindows ().numberOfWindows(),
					batch.getPendingWindows ().numberOfWindows(),
					batch.getCompleteWindows().numberOfWindows(),
					batch.getOpeningWindows ().numberOfWindows()
				)
			);
		}*/
		
		/*batch.getBuffer().release();
		batch.setSchema(outputSchema);
		
		api.outputWindowBatchResult(batch);*/
	}
	
	private void calc(WindowBatch batch, IWindowAPI api) {
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();
		
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			if (selectPredicate.satisfied (inputBuffer, schema, pointer)) {
				
				/* Write tuple to result buffer */
				for (int i = 0; i < expressions.length; ++i) {
					
					expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
				}
				outputBuffer.put(projectedSchema.getPad());			}
		}
		
		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();
		
		/* Reset position for output buffer */
		outputBuffer.close();
		
		/* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
		batch.setBuffer(outputBuffer);
		batch.setSchema(projectedSchema);

		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());
		
		api.outputWindowBatchResult (batch);		
	}

	private void select(WindowBatch batch, IWindowAPI api) {
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();
		
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			if (selectPredicate.satisfied (inputBuffer, schema, pointer)) {
				
				/* Write tuple to result buffer */
				inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
			}
		}
		
		inputBuffer.release();
		
		/* Reset position for output buffer */
		outputBuffer.close();
		
		batch.setBuffer(outputBuffer);
		
		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());
		
		api.outputWindowBatchResult (batch);
	}

	private void project (WindowBatch batch, IWindowAPI api) {
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();
		
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			for (int i = 0; i < expressions.length; ++i) {
				
				expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
			}
			outputBuffer.put(projectedSchema.getPad());
		}					
		
		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();
		
		/* Reset position for output buffer */
		outputBuffer.close();
		
		/* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
		batch.setBuffer(outputBuffer);
		batch.setSchema(projectedSchema);

		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());
		
		api.outputWindowBatchResult (batch);
	}
	
	private void hashJoin (WindowBatch batch, IWindowAPI api) {
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		byte[] bInputBuffer = inputBuffer.getByteBuffer().array();	
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();				
				
		int column1 = isV2? ((LongColumnReference)joinPredicate.getFirstExpression()).getColumn() : 
								((LongLongColumnReference)joinPredicate.getFirstExpression()).getColumn();
		int offset1 = projectedSchema.getAttributeOffset(column1);
		int currentIndex1 =  batch.getBufferStartPointer();
		int currentIndex2 =  0;// relationBuffer.getBufferStartPointer();

		int endIndex1 = batch.getBufferEndPointer() + 32;
		int endIndex2 = relationBuffer.limit();//relationBatch.getBufferEndPointer() + 32;				
		
		int tupleSize1 = projectedSchema.getTupleSize();
		int tupleSize2 = relationSchema.getTupleSize();
		
		/* Actual Tuple Size without padding*/
		int pointerOffset1 = tupleSize1 - projectedSchema.getPadLength();
		int pointerOffset2 = tupleSize2 - relationSchema.getPadLength();
		
		if (monitorSelectivity)
			invoked = matched = 0L;
		
		/* Is one of the windows empty? */
		if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) { 
					
			byte [] key = isV2? new byte[8] : new byte[16];
			ByteBuffer b = ByteBuffer.wrap(key);
			int value;
			int j = 0;

			for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize1) {
				
				if (monitorSelectivity)
					invoked ++;
						
				/*while (j < key.length) {
					key[j] = bInputBuffer[pointer + offset1 + j];
					j += 1;
				}
				j = 0;*/
				
				System.arraycopy(inputBuffer.array(), pointer + offset1 + j, b.array(), 0,key.length);


				
				value = hashMap.get(key);
				if (value != -1) {
					// Write tuple to result buffer 
					inputBuffer.appendBytesTo(pointer, pointerOffset1, outputBuffer);
					relationBuffer.appendBytesTo(value, pointerOffset2, outputBuffer);
					
					/* Write dummy content, if needed */
					outputBuffer.put(this.joinedSchema.getPad());
					
					if (monitorSelectivity)
						matched ++;
				}
			}
		}
		
		if (debug) 
			System.out.println("[DBG] output buffer position is " + outputBuffer.position());
		
		if (monitorSelectivity) {
			double selectivity = 0D;
			if (invoked > 0)
				selectivity = ((double) matched / (double) invoked) * 100D;
			System.out.println(String.format("[DBG] task %6d %2d out of %2d tuples selected (%4.1f)", 
					batch.getTaskId(), matched, invoked, selectivity));
		}
		
		/*		Print tuples */
/*		outputBuffer.close();
		int tid = 1;
		while (outputBuffer.hasRemaining()) {
		
			System.out.println(String.format("%03d: %2d,%2d,%2d | %2d,%2d,%2d", 
			tid++,
		    outputBuffer.getByteBuffer().getLong(),
			outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getLong(),
			outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getInt ()
			));
		}
		System.err.println("Disrupted");
		System.exit(-1);*/
		
		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();
		
		/* Reset position for output buffer */
		//outputBuffer.close();
		
		batch.setBuffer(outputBuffer);
		batch.setSchema(joinedSchema);

		/* Important to set start and end buffer pointers */
		//batch.setBufferPointers(0, outputBuffer.limit());
		
		api.outputWindowBatchResult (batch);
	}

	private void setGroupByKey (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		int pivot = 0;
		for (int i = 0; i < groupByAttributes.length; i++) {
			pivot = groupByAttributes[i].evalAsByteArray (buffer, schema, offset, bytes, pivot);
		}
	}
	
	private void processDataPerWindowWithGroupBy (WindowBatch batch, IWindowAPI api) {
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();
		
		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;
		
		/* Current window start and end pointers */
		int start, end;
		
		WindowHashTable windowHashTable;
		byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
		boolean [] found = (boolean []) tl_found.get(); // new boolean[1];
		boolean pack = false;
		
		float [] values = tl_values.get(); 
		
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;
			
			pack = false;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
				pack = true;
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			windowHashTable = WindowHashTableFactory.newInstance(workerId);
			windowHashTable.setTupleLength(keyLength, valueLength);
			
			while (start < end) {
				/* Get the group-by key */
				setGroupByKey (inputBuffer, inputSchema, start, tupleKey);
				/* Get values */
				for (int i = 0; i < numberOfValues(); ++i) {
					if (aggregationTypes[i] == AggregationType.CNT)
						values[i] = 1;
					else {
						if(inputBuffer == null)
							System.err.println("Input is iull?");
						if(inputSchema == null)							
							System.err.println("Schema is null?");
						if(values == null)
							System.err.println("Values is null?");
						if(aggregationAttributes[i] == null)
							System.err.println("Attributes is null?");
						values[i] = aggregationAttributes[i].eval (inputBuffer, inputSchema, start);
						
					}
				}
				
				/* Check whether there is already an entry in the hash table for this key. 
				 * If not, create a new entry */
				found[0] = false;
				int idx = windowHashTable.getIndex (tupleKey, found);
				if (idx < 0) {
					System.out.println("error: open-adress hash table is full");
					System.exit(1);
				}
				
				ByteBuffer theTable = windowHashTable.getBuffer();
				if (! found[0]) {
					theTable.put (idx, (byte) 1);
					int timestampOffset = windowHashTable.getTimestampOffset (idx);
					theTable.position (timestampOffset);
					/* Store timestamp */
					theTable.putLong (inputBuffer.getLong(start));
					/* Store key and value(s) */
					theTable.put (tupleKey);
					for (int i = 0; i < numberOfValues(); ++i)
						theTable.putFloat(values[i]);
					/* Store count */
					theTable.putInt(1);
				} else {
					/* Update existing entry */
					int valueOffset = windowHashTable.getValueOffset (idx);
					int countOffset = windowHashTable.getCountOffset (idx);
					/* Store value(s) */
					float v;
					int p;
					for (int i = 0; i < numberOfValues(); ++i) {
						p = valueOffset + i * 4;
						switch (aggregationTypes[i]) {
						case CNT:
							theTable.putFloat(p, (theTable.getFloat(p) + 1));
							break;
						case SUM:
						case AVG:
							theTable.putFloat(p, (theTable.getFloat(p) + values[i]));
						case MIN:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v > values[i]) ? values[i] : v));
							break;
						case MAX:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v < values[i]) ? values[i] : v));
							break;
						default:
							throw new IllegalArgumentException ("error: invalid aggregation type");
						}
					}
					/* Increment tuple count */
					theTable.putInt(countOffset, theTable.getInt(countOffset) + 1);
				}
				/* Move to next tuple in window */
				start += inputTupleSize;
			}
			/* Store window result and move to next window */
			evaluateWindow (windowHashTable, outputBuffer, pack);
			/* Release hash maps */
			windowHashTable.release();
		}
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}
	
	private void evaluateWindow (WindowHashTable windowHashTable, IQueryBuffer buffer, boolean pack) {
		
		/* Write current window results to output buffer; copy the entire hash table */
		if (! pack) {
			buffer.put(windowHashTable.getBuffer().array());
			return;
		}
		
		/* Set complete windows */
/*		System.out.println("Complete windows start at " + buffer.position());
*/		
		ByteBuffer theTable = windowHashTable.getBuffer();
		int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
		/* Pack the elements of the table */
		
/*		int tupleIndex = 0;
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
			if (theTable.get(idx) == 1) {
				int mark = theTable.get(idx);
				long timestamp = theTable.getLong(idx + 8);
				long fKey = theTable.getLong(idx + 16);
				long key = theTable.getLong(idx + 24);				
				float val1 = theTable.getFloat(idx + 32);
				float val2 = theTable.getFloat(idx + 36);
				int count = theTable.getInt(idx + 40);
				System.out.println(String.format("%5d: %10d, %10d, %10d, %10d, %5.3f, %5.3f, %10d", 
						tupleIndex, 
						mark,
						timestamp,
						fKey,
						key,
						val1,
						val2,
						count
						));
			}
			tupleIndex ++;
		}*/
		
		//System.exit(1);
//		int tupleIndex = 0;
//		for (int idx = offset; idx < (offset + SystemConf.HASH_TABLE_SIZE); idx += 32) {
//			int mark = buffer.getInt(idx + 0);
//			if (mark > 0) {
//				long timestamp = buffer.getLong(idx + 8);
//				//
//				// int key_1
//				// float value1
//				// float value2
//				// int count
//				//
//				int key = buffer.getInt(idx + 16);
//				float val1 = buffer.getFloat(idx + 20);
//				float val2 = buffer.getFloat(idx + 24);
//				int count = buffer.getInt(idx + 28);
//				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d", 
//					tupleIndex, 
//					Integer.reverseBytes(mark),
//					Long.reverseBytes(timestamp),
//					Integer.reverseBytes(key),
//					0F,
//					0F,
//					Integer.reverseBytes(count)
//				));
//			}
//			tupleIndex ++;
//		}
		
		// ByteBuffer theTable = windowHashTable.getBuffer();
		// int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
		
		/* Pack the elements of the table */
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
			
			if (theTable.get(idx) != 1) /* Skip empty slots */
				continue;
			
			/* Store timestamp, and key */
			int timestampOffset = windowHashTable.getTimestampOffset (idx);
			buffer.put(theTable.array(), timestampOffset, (8 + keyLength));
			
			int valueOffset = windowHashTable.getValueOffset (idx);
			int countOffset = windowHashTable.getCountOffset (idx);
			
			int count = theTable.getInt(countOffset);
			int p;
			for (int i = 0; i < numberOfValues(); ++i) {
				p = valueOffset + i * 4;
				if (aggregationTypes[i] == AggregationType.AVG) {
					buffer.putFloat(theTable.getFloat(p) / (float) count);
				} else {
					buffer.putFloat(theTable.getFloat(p));
				}
			}
			buffer.put(outputSchema.getPad());
		}
	}
	
	private void processDataPerWindowIncrementallyWithGroupBy (WindowBatch batch, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not support incremental computation yet");
	}

	@Override
	public void processData(WindowBatch first, WindowBatch second, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}

	@Override
	public void configureOutput (int queryId) {
		
		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	@Override
	public void processOutput(int queryId, WindowBatch batch) {
		
		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}

	@Override
	public void setup() {
	
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}

}
