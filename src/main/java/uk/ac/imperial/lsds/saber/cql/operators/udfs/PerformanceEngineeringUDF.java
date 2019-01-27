package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongConstant;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.processors.HashMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

import java.util.Arrays;
import java.util.Random;


public class PerformanceEngineeringUDF implements IOperatorCode, IAggregateOperator {

    private static final boolean debug = false;

    private static boolean monitorSelectivity = false;

    private long invoked = 0L;
    private long matched = 0L;

    private IQueryBuffer relationBuffer;

    private WindowDefinition windowDefinition;

    private IPredicate selectPredicate;

    private IPredicate selectPredicate2;

    private Expression[] expressions;

    private Expression[] expressions2;

    private ITupleSchema projectedSchema;

    private ITupleSchema projectedSchema2;

    private IPredicate joinPredicate = null;

    private ITupleSchema joinedSchema;

    private ITupleSchema relationSchema;

    private ITupleSchema outputSchema;

    private boolean incrementalProcessing;

    private AggregationType[] aggregationTypes;

    private FloatColumnReference[] aggregationAttributes;

    private LongColumnReference timestampReference;

    private Expression[] groupByAttributes;
    private boolean groupBy = false;

    private int keyLength, valueLength;

    private boolean isV2 = true;

    private int queryNum;

    private Random randGen;

    /* Thread local variables */
    private ThreadLocal<float[]> tl_values;
    private ThreadLocal<int[]> tl_counts;
    private ThreadLocal<byte[]> tl_tuplekey;
    private ThreadLocal<boolean[]> tl_found;

    //private Multimap<Integer,Integer> multimap;
    private HashMap hashTable;

    private byte [] testTupleBefore = new byte[128];
    private byte [] testTupleAfter = new byte[128];

    public PerformanceEngineeringUDF(
            ITupleSchema inputSchema,
            int select1Column,
            int select1Constant,
            ComparisonPredicate select1ComparisonPredicate,
            int select2Column,
            int select2Constant,
            ComparisonPredicate select2ComparisonPredicate,
            int project1Column1,
            int project1Column2,
            int project2Column1,
            int project2Column2,
            int joinColumnFromLeftSide,
            int joinColumnFromRightSide,
            ComparisonPredicate joinComparisonPredicate,
            ITupleSchema relationSchema,
            IQueryBuffer relationBuffer,
            //HashMap hashMap,
            boolean isV2,
            int queryNum
    ) {

        /* FILTER (event_type == "view") */
        /* Create the predicates required for the filter operator */
        this.selectPredicate = new IntComparisonPredicate
                (select1ComparisonPredicate.ordinal(), new IntColumnReference(select1Column), new IntConstant(select1Constant));

        /* PROJECT (ad_id, event_time) */
        /* Define which fields are going to be projected */
        this.expressions = new Expression[2];
        expressions[0] = new LongColumnReference(project1Column1);      // event_time
        expressions[1] = new LongLongColumnReference(project1Column2);  // ad_id


        /* FILTER (timestamp > 98) for query 0 and 1*/
        /* FILTER (ad_type == "mail" or "mobile") for query 3*/
        this.selectPredicate2 = (this.queryNum == 3) ?
                new IntComparisonPredicate(select2ComparisonPredicate.ordinal(), new IntColumnReference(select2Column), new IntConstant(select2Constant))
                : new LongComparisonPredicate(select2ComparisonPredicate.ordinal(), new LongColumnReference(select2Column), new LongConstant(select2Constant));


        /* JOIN (ad_id, ad_id) */
        /* Define which fields are going be used for the Join operator */
        this.joinPredicate = new LongLongComparisonPredicate
                (joinComparisonPredicate.ordinal(), new LongLongColumnReference(joinColumnFromLeftSide), new LongLongColumnReference(joinColumnFromRightSide));

        /* Create HashTable for join */
        this.relationSchema = relationSchema;
        this.relationBuffer = relationBuffer;
        buildHashTable(); // Build hashtable from the static relation

        this.expressions2 = new Expression[2];
        this.expressions2[0] = new LongColumnReference(project2Column1);        // event_time
        this.expressions2[1] = new LongLongColumnReference(project2Column2);    // campaign_id


        /* This is the output of projection */
        this.projectedSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
        this.projectedSchema2 = ExpressionsUtil.getTupleSchemaFromExpressions(expressions2);

        /* This is the output of join */
        this.joinedSchema = ExpressionsUtil.mergeTupleSchemas(projectedSchema, relationSchema);
        this.outputSchema = this.joinedSchema;
        this.isV2 = isV2;
        this.timestampReference = new LongColumnReference(0);
        this.randGen = new Random();
        this.queryNum = queryNum;
    }

    void buildHashTable() {
        this.hashTable = new HashMap();
        byte[] buffer = new byte[this.relationBuffer.getByteBuffer().position()];
        for (int i = 0; i < this.relationBuffer.getByteBuffer().position(); i++)
            buffer[i] = this.relationBuffer.getByteBuffer().get(i);

        int tupleSize = this.relationSchema.getTupleSize();

        int column = ((LongLongColumnReference) this.joinPredicate.getSecondExpression()).getColumn();
        int offset = this.relationSchema.getAttributeOffset(column);

        byte[] b = new byte[16];
        int endIndex = SystemConf.RELATIONAL_TABLE_BUFFER_SIZE; //batch1.getBufferEndPointer();
        int i = 0;
        int j = 0;
        while (i < endIndex) {
            while (j < b.length) {
                b[j] = buffer[i + offset + j];
                j += 1;
            }
            j = 0;
            this.hashTable.register(Arrays.copyOf(b, b.length), i);
            i += tupleSize;
        }
    }

    @Override
    public boolean hasGroupBy() {
        return groupBy;
    }

    @Override
    public ITupleSchema getOutputSchema() {
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
        return getAggregationType(0);
    }

    @Override
    public AggregationType getAggregationType(int idx) {
        if (idx < 0 || idx > aggregationTypes.length - 1)
            throw new ArrayIndexOutOfBoundsException("error: invalid aggregation type index");
        return aggregationTypes[idx];
    }

    @Override
    public void processData(WindowBatch batch, IWindowAPI api) {
        switch (this.queryNum) {
            case 0:
            case 1:
                query0_1(batch, api);
                break;
            case 2:
                query2(batch, api);
                break;
            case 3:
                query3(batch, api);
                break;
            default:
                System.out.println("Wrong query number!");
                System.exit(1);
        }
    }

    public void query0_1(WindowBatch batch, IWindowAPI api) {
        // variables testing for correctness
        int inputPointer, randPos, offset;
        long tempLongVar1, tempLongVar2, tempLongVar3;

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer());
        offset = batch.getBufferStartPointer() + 1024;
        for (int i = 0; i < 128; i++)
            testTupleBefore[i] = batch.getBuffer().getByteBuffer().get(offset+i);

        /* Select */
        if (selectPredicate != null)
            select(batch, api);

        offset = batch.getBufferStartPointer();
        offset += (this.queryNum == 0 || this.queryNum == 2) ? 2*batch.getSchema().getTupleSize() : 4*batch.getSchema().getTupleSize();
        for (int i = 0; i < 128; i++)
            testTupleAfter[i] = batch.getBuffer().getByteBuffer().get(offset+i);
        assert (batch.getBuffer().limit() == inputPointer * SystemConf.FIRST_FILTER_SELECTIVITY) : "Broken Selection: The pointer of the outputBuffer hasn't moved properly!";
        assert (Arrays.equals(testTupleBefore, testTupleAfter)) : "Broken Selection: Wrong tuples passed!";


        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1) / batch.getSchema().getTupleSize();
        randPos = this.randGen.nextInt(inputPointer);
        tempLongVar1 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize());
        /* Project */
        if (expressions != null)
            project(batch, api);

        assert (batch.getBuffer().limit() == (inputPointer * projectedSchema.getTupleSize())) : "Broken project: wrong number of columns copied!";
        assert (tempLongVar1 == batch.getBuffer().getByteBuffer().getLong(randPos * projectedSchema.getTupleSize())) : "Broken project: Wrong tuples passed!";

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer());
        randPos = this.randGen.nextInt(inputPointer/batch.getSchema().getTupleSize());
        tempLongVar1 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize());
        tempLongVar2 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+8);
        tempLongVar3 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+16);

        /* Hash Join */
        if (joinPredicate != null)
            hashJoin(batch, api);

        assert (batch.getBuffer().limit() == inputPointer * 4 / SystemConf.FIRST_FILTER_SELECTIVITY) : "Broken HashJoin: The pointer of the outputBuffer hasn't moved properly!";
        assert (tempLongVar1 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()) &&
                tempLongVar2 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+8) &&
                tempLongVar3 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+16)) : "Broken HashJoin: Wrong tuples passed!";

        /* Select */
        if (selectPredicate2 != null)
            select2(batch, api);
        assert (batch.getBuffer().limit() % batch.getSchema().getTupleSize() == 0) : "Broken second Selection: The pointer of the outputBuffer hasn't moved properly!";
    }

    public void query2(WindowBatch batch, IWindowAPI api) {
        // variables testing for correctness
        int inputPointer, randPos, offset;
        long tempLongVar1, tempLongVar2, tempLongVar3;

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer());
        offset = batch.getBufferStartPointer() + 1024;
        for (int i = 0; i < 128; i++)
            testTupleBefore[i] = batch.getBuffer().getByteBuffer().get(offset+i);

        /* Select */
        if (selectPredicate != null)
            select(batch, api);

        offset = batch.getBufferStartPointer();
        offset += (this.queryNum == 0 || this.queryNum == 2) ? 2*batch.getSchema().getTupleSize() : 4*batch.getSchema().getTupleSize();
        for (int i = 0; i < 128; i++)
            testTupleAfter[i] = batch.getBuffer().getByteBuffer().get(offset+i);
        assert (batch.getBuffer().limit() == inputPointer * SystemConf.FIRST_FILTER_SELECTIVITY) : "Broken Selection: The pointer of the outputBuffer hasn't moved properly!";
        assert (Arrays.equals(testTupleBefore, testTupleAfter)) : "Broken Selection: Wrong tuples passed!";

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1)/batch.getSchema().getTupleSize();
        randPos = this.randGen.nextInt(inputPointer);
        tempLongVar1 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize());
        /* Project */
        if (expressions != null)
            project(batch, api);

        assert (batch.getBuffer().limit() == (inputPointer * projectedSchema.getTupleSize())) : "Broken project: wrong number of columns copied";
        assert (tempLongVar1 == batch.getBuffer().getByteBuffer().getLong(randPos * projectedSchema.getTupleSize())) : "Broken project: wrong tuple passed";

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer());
        randPos = this.randGen.nextInt(inputPointer/batch.getSchema().getTupleSize());
        tempLongVar1 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize());
        tempLongVar2 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+8);
        tempLongVar3 = batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+16);

        /* Hash Join */
        if (joinPredicate != null)
            hashJoin(batch, api);

        assert (batch.getBuffer().limit() == inputPointer * 4 / SystemConf.FIRST_FILTER_SELECTIVITY) : "Broken HashJoin: The pointer of the outputBuffer hasn't moved properly!";
        assert (tempLongVar1 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()) &&
                tempLongVar2 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+8) &&
                tempLongVar3 == batch.getBuffer().getByteBuffer().getLong(randPos * batch.getSchema().getTupleSize()+16)) : "Broken HashJoin: Wrong tuples passed!";
    }

    public void query3(WindowBatch batch, IWindowAPI api) {
        // variables testing for correctness
        int inputPointer, offset;

        inputPointer = (batch.getBufferEndPointer() - batch.getBufferStartPointer());
        offset = batch.getBufferStartPointer() + 1024;
        for (int i = 0; i < 128; i++)
            testTupleBefore[i] = batch.getBuffer().getByteBuffer().get(offset+i);

        /* Select */
        if (selectPredicate != null)
            select(batch, api);

        offset = batch.getBufferStartPointer();
        offset += (this.queryNum == 0 || this.queryNum == 2) ? 2*batch.getSchema().getTupleSize() : 4*batch.getSchema().getTupleSize();
        for (int i = 0; i < 128; i++)
            testTupleAfter[i] = batch.getBuffer().getByteBuffer().get(offset+i);
        assert (batch.getBuffer().limit() == inputPointer * SystemConf.FIRST_FILTER_SELECTIVITY) : "Broken Selection: The pointer of the outputBuffer hasn't moved properly!";
        assert (Arrays.equals(testTupleBefore, testTupleAfter)) : "Broken Selection: Wrong tuples passed!";

        /* Select */
        if (selectPredicate2 != null)
            select2(batch, api);
        assert (batch.getBuffer().limit() % batch.getSchema().getTupleSize() == 0) : "Broken second Selection: The pointer of the outputBuffer hasn't moved properly!";
    }

    public void select(WindowBatch batch, IWindowAPI api) {

        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();

		int selectivity = 0;
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			if (selectPredicate.satisfied (inputBuffer, schema, pointer)) {
				// Write tuple to result buffer
				inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
				selectivity++;
			}
		}

        // assume selectivity is 25% in our case
        assert (selectivity / SystemConf.FIRST_FILTER_SELECTIVITY == ((batch.getBufferEndPointer() - batch.getBufferStartPointer()) / tupleSize)) : "Broken Selection: Selectivity is 25%!";
        // check that the pointer of the outputBuffer has moved accordingly
        assert (outputBuffer.position() == selectivity * tupleSize) : "Broken Selection: The pointer of the outputBuffer hasn't moved properly!";

        inputBuffer.release();

        /* Reset position for output buffer */
        outputBuffer.close();

        batch.setBuffer(outputBuffer);

        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult(batch);
    }

    public void project(WindowBatch batch, IWindowAPI api) {

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

        assert (outputBuffer.position() == (((batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1) / tupleSize) * projectedSchema.getTupleSize())) : "Broken project: wrong number of columns copied";
        int randPos = this.randGen.nextInt((batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1) / tupleSize);
        assert (inputBuffer.getByteBuffer().getLong(randPos * tupleSize) == outputBuffer.getByteBuffer().getLong(randPos * projectedSchema.getTupleSize())) : "Broken project: wrong tuple passed";


        /* Return any (unbounded) buffers to the pool */
        inputBuffer.release();

        /* Reset position for output buffer */
        outputBuffer.close();

        /* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
        batch.setBuffer(outputBuffer);
        batch.setSchema(projectedSchema);

        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult(batch);
    }

    public void hashJoin(WindowBatch batch, IWindowAPI api) {

        IQueryBuffer inputBuffer = batch.getBuffer();
        //byte[] bInputBuffer = inputBuffer.getByteBuffer().array();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        int column1 = ((LongLongColumnReference) joinPredicate.getFirstExpression()).getColumn();
        int offset1 = projectedSchema.getAttributeOffset(column1);
        int currentIndex1 = batch.getBufferStartPointer();
        int currentIndex2 = 0;// relationBuffer.getBufferStartPointer();

        int endIndex1 = batch.getBufferEndPointer() + 32;
        int endIndex2 = relationBuffer.limit();//relationBatch.getBufferEndPointer() + 32;

        int tupleSize1 = projectedSchema.getTupleSize();
        int tupleSize2 = relationSchema.getTupleSize();

        /* Actual Tuple Size without padding*/
        int pointerOffset1 = tupleSize1 - projectedSchema.getPadLength();
        int pointerOffset2 = tupleSize2 - relationSchema.getPadLength();

        //if (monitorSelectivity)
        invoked = matched = 0L;

        /* Is one of the windows empty? */
        if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) {

            byte[] key = new byte[16];
            int value;
            int j = 0;

            for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize1) {

                if (monitorSelectivity)
                    invoked++;

                //System.arraycopy(inputBuffer.array(), pointer + offset1 + j, b.array(), 0, key.length);
                for (int i = 0; i < key.length; i++) {
                    key[i] = inputBuffer.getByteBuffer().get(pointer + offset1 + j + i);
                }
                // hashMap is a helper data structure that stores the key (ad_id) of the HashJoin operation from the static table
                // and has their actual positions in the off-heap buffer (relationBuffer) as a value
                value = hashTable.get(key); // here we retrieve the wanted position
                if (value != -1) {
                    // Write tuple to result buffer

                    // the first half is the tuple from the input stream
                    inputBuffer.appendBytesTo(pointer, pointerOffset1, outputBuffer);
                    // now we copy the second half from the relationalBuffer,
                    // instead of the hashMap which just contains the mapping and not the actual tuples!
                    relationBuffer.appendBytesTo(value, pointerOffset2, outputBuffer);

                    /* Write dummy content, if needed */
                    outputBuffer.put(this.joinedSchema.getPad());

                    //if (monitorSelectivity)
                    matched++;
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

        // assume selectivity is 100% in this case
        assert (matched == ((batch.getBufferEndPointer() - batch.getBufferStartPointer()) / tupleSize1)) : "Broken HashJoin: Selectivity is 100%!";
        // check that the pointer of the outputBuffer has moved accordingly
        assert (outputBuffer.position() == matched * this.joinedSchema.getTupleSize()) : "Broken HashJoin: The pointer of the outputBuffer hasn't moved properly!";


        /* Return any (unbounded) buffers to the pool */
        inputBuffer.release();

        /* Reset position for output buffer */
        //outputBuffer.close();

        batch.setBuffer(outputBuffer);
        batch.setSchema(joinedSchema);

        /* Important to set start and end buffer pointers */
        //batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult(batch);
    }

    private void project2(WindowBatch batch, IWindowAPI api) {

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

        assert (outputBuffer.position() == (((batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1) / tupleSize) * projectedSchema2.getTupleSize())) : "Broken project: wrong number of columns copied";
        int randPos = this.randGen.nextInt((batch.getBufferEndPointer() - batch.getBufferStartPointer() + 1) / tupleSize);
        assert (inputBuffer.getByteBuffer().getLong(randPos * tupleSize) == outputBuffer.getByteBuffer().getLong(randPos * projectedSchema2.getTupleSize())) : "Broken project: wrong tuple passed";

        /* Return any (unbounded) buffers to the pool */
        inputBuffer.release();

        /* Reset position for output buffer */
        outputBuffer.close();

        /* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
        batch.setBuffer(outputBuffer);
        batch.setSchema(projectedSchema2);

        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult(batch);
    }

    public void select2(WindowBatch batch, IWindowAPI api) {

        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();

        int selectivity = 0;
        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

            if (selectPredicate2.satisfied(inputBuffer, schema, pointer)) {
                // Write tuple to result buffer
                inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
                selectivity++;
            }
        }

        // assume selectivity is 25% in our case
        // check that the pointer of the outputBuffer has moved accordingly
        assert (outputBuffer.position() == selectivity * tupleSize) : "Broken Second Selection: The pointer of the outputBuffer hasn't moved properly!";

        inputBuffer.release();

        /* Reset position for output buffer */
        outputBuffer.close();

        batch.setBuffer(outputBuffer);

        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());

        api.outputWindowBatchResult(batch);
    }

    public void calc(WindowBatch batch, IWindowAPI api) {
        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

            if (selectPredicate.satisfied(inputBuffer, schema, pointer)) {

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

        api.outputWindowBatchResult(batch);
    }

    @Override
    public void processData(WindowBatch first, WindowBatch second, IWindowAPI api) {

        throw new UnsupportedOperationException("error: operator does not operate on two streams");
    }

    @Override
    public void configureOutput(int queryId) {

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
