package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
//import java.util.List;
import java.util.Random;

//import com.google.common.collect.Multimap;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
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
import uk.ac.imperial.lsds.saber.devices.TheCPU;
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

    private Expression [] expressions2;
	
	private ITupleSchema projectedSchema;

    private ITupleSchema projectedSchema2;

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
            Expression [] expressions2,
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
		this.expressions2 = expressions2;
		
		/* This is the output of projection */
		this.projectedSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
        this.projectedSchema2 = ExpressionsUtil.getTupleSchemaFromExpressions(expressions2);

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
		
		boolean useCalc = false; // enable this boolean to merge select and project operators
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

        /* Project to drop unwanted columns */
        //if (expressions2 != null)
        //    project2 (batch, api);

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
				outputBuffer.put(projectedSchema.getPad());
			}
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
				// Write tuple to result buffer
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
		//byte[] bInputBuffer = inputBuffer.getByteBuffer().array();
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
			int value;
			int j = 0;

			for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize1) {
				
				if (monitorSelectivity)
					invoked ++;
				
				//System.arraycopy(inputBuffer.array(), pointer + offset1 + j, b.array(), 0, key.length);
                for (int i = 0; i < key.length; i++) {
                    key[i] = inputBuffer.getByteBuffer().get(pointer + offset1 + j + i);
                }

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

    private void project2 (WindowBatch batch, IWindowAPI api) {

        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

            for (int i = 0; i < expressions2.length; ++i) {

                expressions2[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
            }
            outputBuffer.put(projectedSchema2.getPad());
        }

        /* Return any (unbounded) buffers to the pool */
        inputBuffer.release();

        /* Reset position for output buffer */
        outputBuffer.close();

        /* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
        batch.setBuffer(outputBuffer);
        batch.setSchema(projectedSchema2);

        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult (batch);
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
