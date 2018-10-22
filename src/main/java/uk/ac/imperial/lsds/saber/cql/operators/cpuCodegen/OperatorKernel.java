package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators.CpuKernelGenerator;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OperatorKernel implements IOperatorCode, IAggregateOperator {

    private static final boolean debug = false;
    WindowDefinition windowDefinition;
    private ITupleSchema inputSchema;

    // Projection
    private Expression [] expressions;

    // Selection
    private IPredicate predicate;

    // Aggregation
    private AggregationType[] aggregationTypes;
    private FloatColumnReference[] aggregationAttributes;
    private AggregationType[] aggregationGroupByTypes;
    private FloatColumnReference[] aggregationGroupByAttributes;
    private LongColumnReference timestampReference;
    private Expression[] groupByAttributes;
    private boolean groupBy = false;

    private boolean processIncremental;

    ITupleSchema outputSchema;

    private int keyLength, valueLength;

    // Projection
    public OperatorKernel (Expression [] expressions, ITupleSchema inputSchema) {
        this.expressions = expressions;
        this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
        this.inputSchema = inputSchema;
    }

    public OperatorKernel (Expression expression, ITupleSchema inputSchema) {
        this.expressions = new Expression [] { expression };
        this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
        this.inputSchema = inputSchema;
    }

    // Selection
    public OperatorKernel (IPredicate predicate, ITupleSchema inputSchema) {
        this.predicate = predicate;
        this.inputSchema = inputSchema;
    }

    public OperatorKernel (Expression [] expressions, IPredicate predicate, ITupleSchema inputSchema) {
        this.expressions = expressions;
        this.predicate = predicate;
        this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
        this.inputSchema = inputSchema;
    }

    // Aggregation
    public OperatorKernel (WindowDefinition windowDefinition,
                           AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes,
                           AggregationType [] aggregationGroupByTypes, FloatColumnReference [] aggregationGroupByAttributes, Expression [] groupByAttributes,
                           Expression [] expressions, IPredicate predicate, ITupleSchema inputSchema) {

        this.windowDefinition = windowDefinition;
        this.aggregationTypes = aggregationTypes;
        this.aggregationAttributes = aggregationAttributes;
        this.aggregationGroupByTypes = aggregationGroupByTypes;
        this.aggregationGroupByAttributes = aggregationGroupByAttributes;
        this.groupByAttributes = groupByAttributes;

        this.expressions = expressions;
        this.predicate = predicate;

        this.inputSchema = inputSchema;

        if (groupByAttributes != null)
            groupBy = true;
        else
            groupBy = false;

        timestampReference = new LongColumnReference(0);

        /* Create output schema */

        int numberOfKeyAttributes;
        if (groupByAttributes != null)
            numberOfKeyAttributes = groupByAttributes.length;
        else
            numberOfKeyAttributes = 0;

        int n = 1 + numberOfKeyAttributes + aggregationGroupByAttributes.length;

        int numberOfOutputAttributes = n;
        if (groupByAttributes == null)
            numberOfOutputAttributes += 1; /* +1 for count */

        Expression [] outputAttributes = new Expression[numberOfOutputAttributes];

        /* The first attribute is the timestamp */
        outputAttributes[0] = timestampReference;

        keyLength = 0;

        if (numberOfKeyAttributes > 0) {
            for (int i = 1; i <= numberOfKeyAttributes; ++i) {
                Expression e = groupByAttributes[i - 1];
                if (e instanceof IntExpression) { outputAttributes[i] = new IntColumnReference(i); keyLength += 4; }
                else if (e instanceof LongExpression) { outputAttributes[i] = new     LongColumnReference(i); keyLength += 8; }
                else if (e instanceof FloatExpression) { outputAttributes[i] = new    FloatColumnReference(i); keyLength += 4; }
                else if (e instanceof LongLongExpression) { outputAttributes[i] = new LongLongColumnReference(i); keyLength += 16; }
                else
                    throw new IllegalArgumentException("error: invalid group-by attribute");
            }
        }

        for (int i = numberOfKeyAttributes + 1; i < n; ++i)
            outputAttributes[i] = new FloatColumnReference(i);

        /* Set count attribute */
        if (groupByAttributes == null)
            outputAttributes[numberOfOutputAttributes - 1] = new IntColumnReference(numberOfOutputAttributes - 1);

        this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);

        boolean containsIncrementalAggregationType = true;
        for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
            if (
                    aggregationGroupByTypes[i] != AggregationType.CNT &&
                            aggregationGroupByTypes[i] != AggregationType.SUM &&
                            aggregationGroupByTypes[i] != AggregationType.AVG) {

                containsIncrementalAggregationType = false;
                break;
            }
        }

        if (containsIncrementalAggregationType) {
            System.out.println("[DBG] operator contains incremental aggregation type");
            processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
        } else {
            processIncremental = false;
        }

        valueLength = 4 * aggregationGroupByTypes.length;
    }

    public boolean hasGroupBy () {
        return groupBy;
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
        return aggregationGroupByAttributes.length;
    }

    public AggregationType getAggregationType () {
        return getAggregationType (0);
    }

    public AggregationType getAggregationType (int idx) {
        if (idx < 0 || idx > aggregationTypes.length - 1)
            throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
        return aggregationTypes[idx];
    }

    public AggregationType getAggregationGroupByType () {
        return getAggregationGroupByType (0);
    }

    public AggregationType getAggregationGroupByType (int idx) {
        if (idx < 0 || idx > aggregationGroupByTypes.length - 1)
            throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
        return aggregationGroupByTypes[idx];
    }

    public void processData (WindowBatch batch, IWindowAPI api) {
        int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());

        int [] startP = batch.getWindowStartPointers();
        int []   endP = batch.getWindowEndPointers();

        ITupleSchema inputSchema = batch.getSchema();
        int inputTupleSize = inputSchema.getTupleSize();

        PartialWindowResults closingWindows = PartialWindowResultsFactory.newInstance (workerId);
        PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
        PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
        PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);

        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer openingWindowsBuffer = openingWindows.getBuffer();
        IQueryBuffer closingWindowsBuffer = closingWindows.getBuffer();
        IQueryBuffer pendingWindowsBuffer = pendingWindows.getBuffer();
        IQueryBuffer completeWindowsBuffer = completeWindows.getBuffer();


        ByteBuffer arrayHelper = ByteBuffer.allocateDirect(4 * 8);
        arrayHelper.order(ByteOrder.LITTLE_ENDIAN);

        /*System.out.println("-----");
        System.out.println("StartTimeStamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferStartPointer()));
        System.out.println("EndTimeStamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferEndPointer()-32));
        System.out.println("StartPointer: " + batch.getBufferStartPointer());
        System.out.println("EndPointer: " + batch.getBufferEndPointer());*/
        // REPLACE THE POSITIONS WITH THE CORRECT INDEX BY DIVIDING WITH 4!!!

        TheCPU.getInstance().singleOperatorComputation(inputBuffer.getByteBuffer(),
                batch.getBufferStartPointer()/inputSchema.getTupleSize(),
                batch.getBufferEndPointer()/inputSchema.getTupleSize(),
                openingWindowsBuffer.getByteBuffer(), closingWindowsBuffer.getByteBuffer(),
                pendingWindowsBuffer.getByteBuffer(), completeWindowsBuffer.getByteBuffer(),
                openingWindows.getStartPointers(), closingWindows.getStartPointers(),
                pendingWindows.getStartPointers(), completeWindows.getStartPointers(),
                batch.getStreamStartPointer(),
                0,0,0,0,
                //openingWindowsBuffer.position()/tupleLength, closingWindowsBuffer.position()/tupleLength,
                //pendingWindowsBuffer.position()/tupleLength, completeWindowsBuffer.position()/outputSchema.getTupleSize(),
                arrayHelper);

        // FIX positions and Counters!
        // todo: check again this numbers
        openingWindowsBuffer.position(arrayHelper.getInt(0));
        closingWindowsBuffer.position(arrayHelper.getInt(4));
        pendingWindowsBuffer.position(arrayHelper.getInt(8));
        completeWindowsBuffer.position(arrayHelper.getInt(12));

        openingWindows.setCount(arrayHelper.getInt(16));
        closingWindows.setCount(arrayHelper.getInt(20));
        pendingWindows.setCount(arrayHelper.getInt(24));
        completeWindows.setCount(arrayHelper.getInt(28));


        /*System.out.println("first timestamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferStartPointer()));
        System.out.println("last timestamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferEndPointer() - inputTupleSize));
        System.out.println("streamStartPointer: " + batch.getStreamStartPointer());
        System.out.println("opening windows "+ openingWindows.numberOfWindows());
        if (openingWindows.numberOfWindows() > 0) {
            for (int i = 0; i < openingWindows.numberOfWindows(); i++) {
                System.out.println("occupancy, timestamp, key, value");
                int base = i * SystemConf.HASH_TABLE_SIZE;
                for (int j = 0; j < SystemConf.HASH_TABLE_SIZE/32; j++) {
                    int offset = j * 32;
                    System.out.println(openingWindows.getBuffer().getByteBuffer().getLong(base + offset) + ", "  +
                            openingWindows.getBuffer().getByteBuffer().getLong(base + offset + 8) + ", "  +
                            openingWindows.getBuffer().getByteBuffer().getInt(base + offset + 16) + ", "  +
                            //openingWindows.getBuffer().getByteBuffer().getFloat(base + offset + 20) + ", "  +
                            openingWindows.getBuffer().getByteBuffer().getInt(base + offset + 24));
                }
            }
            System.out.println("-------------");
        }

        System.out.println("closing windows "+ closingWindows.numberOfWindows());
        if (closingWindows.numberOfWindows() > 0) {
            for (int i = 0; i < closingWindows.numberOfWindows(); i++) {
                System.out.println("occupancy, timestamp, key, value");
                int base = i * SystemConf.HASH_TABLE_SIZE;
                for (int j = 0; j < SystemConf.HASH_TABLE_SIZE/32; j++) {
                    int offset = j * 32;
                    System.out.println(closingWindows.getBuffer().getByteBuffer().getLong(base + offset) + ", "  +
                            closingWindows.getBuffer().getByteBuffer().getLong(base + offset + 8) + ", "  +
                            closingWindows.getBuffer().getByteBuffer().getInt(base + offset + 16) + ", "  +
                            //closingWindows.getBuffer().getByteBuffer().getFloat(base + offset + 20) + ", "  +
                            closingWindows.getBuffer().getByteBuffer().getInt(base + offset + 24));
                }
            }
            System.out.println("-------------");
        }

        System.out.println("pending windows "+ pendingWindows.numberOfWindows());
        if (pendingWindows.numberOfWindows() > 0) {
            for (int i = 0; i < pendingWindows.numberOfWindows(); i++) {
                System.out.println("occupancy, timestamp, key, value");
                int base = i * SystemConf.HASH_TABLE_SIZE;
                for (int j = 0; j < SystemConf.HASH_TABLE_SIZE/32; j++) {
                    int offset = j * 32;
                    System.out.println(pendingWindows.getBuffer().getByteBuffer().getLong(base + offset) + ", "  +
                            pendingWindows.getBuffer().getByteBuffer().getLong(base + offset + 8) + ", " +
                            pendingWindows.getBuffer().getByteBuffer().getInt(base + offset + 16) + ", "  +
                            //pendingWindows.getBuffer().getByteBuffer().getFloat(base + offset + 20) + ", "  +
                            pendingWindows.getBuffer().getByteBuffer().getInt(base + offset + 24));
                }
            }
            System.out.println("-------------");
        }
        System.out.println("complete windows "+ completeWindows.numberOfWindows());
        if (completeWindows.numberOfWindows() > 0) {
            for (int i = 0; i < completeWindows.numberOfWindows(); i++) {
                System.out.println("timestamp, key, value");
                int base = i * SystemConf.HASH_TABLE_SIZE/32 * 16;
                for (int j = 0; j < SystemConf.HASH_TABLE_SIZE/32; j++) {
                    int offset = j * 16;
                    System.out.println(completeWindows.getBuffer().getByteBuffer().getLong(base + offset) + ", "  +
                            completeWindows.getBuffer().getByteBuffer().getInt(base + offset + 8) + ", " +
                            (int) completeWindows.getBuffer().getByteBuffer().getFloat(base + offset + 12));
                }
            }
            System.out.println("-------------");
        }
        System.out.println("--------xxxxx---------");

        System.out.println("first timestamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferStartPointer()));
        System.out.println("last timestamp: " + inputBuffer.getByteBuffer().getLong(batch.getBufferEndPointer() - inputTupleSize));
        System.out.println("streamStartPointer: " + batch.getStreamStartPointer());
        System.out.println("opening windows "+ openingWindows.numberOfWindows());
        System.out.println("closing windows "+ closingWindows.numberOfWindows());
        System.out.println("pending windows "+ pendingWindows.numberOfWindows());
        System.out.println("complete windows "+ completeWindows.numberOfWindows());
        System.out.println("--------");*/

        /* At the end of processing, set window batch accordingly */
        batch.setClosingWindows  ( closingWindows);
        batch.setPendingWindows  ( pendingWindows);
        batch.setCompleteWindows (completeWindows);
        batch.setOpeningWindows  ( openingWindows);
    }

    public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {

        throw new UnsupportedOperationException("error: operator does not operate on two streams");
    }

    public void configureOutput (int queryId) {

        throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
    }

    public void processOutput (int queryId, WindowBatch batch) {

        throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
    }

    public void setup() {


        String headers_and_structs = CpuKernelGenerator.getHeader(inputSchema, null, outputSchema);
        String windowDef = CpuKernelGenerator.getWindowDefinition(windowDefinition);
        String hashtable = CpuKernelGenerator.getHashTableDefinition(SystemConf.SABER_HOME + "/clib/cpu_templates/hashtable_tmpl",
                aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, keyLength, valueLength);

        String templateTypes = CpuKernelGenerator.getTemplateTypes (aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes);
        String signature = CpuKernelGenerator.getSignature (templateTypes);
        String initializeAggregationVariables = CpuKernelGenerator.getVariables (aggregationTypes, aggregationAttributes,
                aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, templateTypes);

        String sw_p1 = CpuKernelGenerator.sw_p1;
        String computationBlockForInsert_t3 = CpuKernelGenerator.getComputationBlockForInsert (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 3);
        String sw_p2 = CpuKernelGenerator.sw_p2;
        //String computationBlockForInsert = CpuKernelGenerator.getComputationBlock ();
        String sw_p3 = CpuKernelGenerator.sw_p3;
        //String computationBlockForInsert = CpuKernelGenerator.getComputationBlock ();
        String sw_p4 = CpuKernelGenerator.sw_p4;
        String computationBlockForEvict_t4 = CpuKernelGenerator.getComputationBlockForEvict (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 4);
        String sw_p5 = CpuKernelGenerator.sw_p5;

        // todo: this is where having or self join should happen
        String writeCompleteWindows_t3 = CpuKernelGenerator.getWriteCompleteWindowsBlock (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 3);

        String sw_p6 = CpuKernelGenerator.sw_p6;
        String computationBlockForInsert_t2 = CpuKernelGenerator.getComputationBlockForInsert (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 2);
        String sw_p7 = CpuKernelGenerator.sw_p7;
        // String computationBlockForEvict_t4

        String sw_p8 = CpuKernelGenerator.sw_p8;
        // todo: this is where having or self join should happen
        String writeCompleteWindows_t6 = CpuKernelGenerator.getWriteCompleteWindowsBlock (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 6);
        String sw_p9 = CpuKernelGenerator.sw_p9;
        String computationBlockForInsert_t5 = CpuKernelGenerator.getComputationBlockForInsert (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 5);
        String sw_p10 = CpuKernelGenerator.sw_p10;
        String computationBlockForInsert_t4 = CpuKernelGenerator.getComputationBlockForInsert (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 4);
        String sw_p11 = CpuKernelGenerator.sw_p11;
        String computationBlockForEvict_t3 = CpuKernelGenerator.getComputationBlockForEvict (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,
                expressions, predicate, inputSchema, 3);
        String sw_p12 = CpuKernelGenerator.sw_p12;

        // merge code
        String mergeFunction_p1 = CpuKernelGenerator.getMergeFunction_p1 (templateTypes);
        String writeCompleteWindowsForMerge_b1_t4_not_found = CpuKernelGenerator.getWriteCompleteWindowsBlockForMerge (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 1,4, false);
        String mergeFunction_p2 = CpuKernelGenerator.mergeFunction_p2;
        String writeCompleteWindowsForMerge_b1_t4_found = CpuKernelGenerator.getWriteCompleteWindowsBlockForMerge (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 1,4, true);
        String mergeFunction_p3 = CpuKernelGenerator.mergeFunction_p3;
        String mergeOpeningWindowsBlock_b1_t4 = CpuKernelGenerator.getMergeOpeningWindowsBlock (windowDefinition, aggregationTypes,
                aggregationAttributes, aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes,4);
        String mergeFunction_p4 = CpuKernelGenerator.mergeFunction_p4;
        String writeCompleteWindowsForMerge_b2_t3 = CpuKernelGenerator.getWriteCompleteWindowsBlockForMerge (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 2,3, false);
        String mergeFunction_p5 = CpuKernelGenerator.mergeFunction_p5;
        String mergeOpeningWindowsBlock_b2_t3 = CpuKernelGenerator.getWriteOpeningWindowsBlockForMerge (aggregationGroupByTypes, aggregationGroupByAttributes,
                groupByAttributes, outputSchema, 1,3);
        String mergeFunction_p6 = CpuKernelGenerator.getMergeFunction_p6 (templateTypes);

        // helper function for merging
        String mergeHelperFunction = CpuKernelGenerator.getMergeHelperFunction (templateTypes);
        // change timestamps
        String changeTimestampsFunction = CpuKernelGenerator.getChangeTimestampsFunction ();


        String source = headers_and_structs + windowDef + hashtable + signature + initializeAggregationVariables +
                sw_p1 + computationBlockForInsert_t3 + sw_p2 + computationBlockForInsert_t3 + sw_p3 + computationBlockForInsert_t3 +
                sw_p4 + computationBlockForEvict_t4 + sw_p5 + writeCompleteWindows_t3 + sw_p6 +
                computationBlockForInsert_t2 + sw_p7 + computationBlockForEvict_t4 + sw_p8 + writeCompleteWindows_t6 + sw_p9 +
                computationBlockForInsert_t5 +sw_p10 + computationBlockForInsert_t4 + sw_p11 + computationBlockForEvict_t3 + sw_p12 +

                mergeFunction_p1 + writeCompleteWindowsForMerge_b1_t4_not_found + mergeFunction_p2 + writeCompleteWindowsForMerge_b1_t4_found + mergeFunction_p3 + mergeOpeningWindowsBlock_b1_t4 +
                mergeFunction_p4 + writeCompleteWindowsForMerge_b2_t3 + mergeFunction_p5 + mergeOpeningWindowsBlock_b2_t3 + mergeFunction_p6 +
                mergeHelperFunction + changeTimestampsFunction;

        //System.out.println(source);

        Process p;
        try {
            // remove the previous file
            p = Runtime.getRuntime().exec("rm "+ SystemConf.SABER_HOME + "/clib/new_generated.c");
            p = Runtime.getRuntime().exec("rm "+ SystemConf.SABER_HOME + "/clib/new_generated.o");
            Writer fileWriter = new FileWriter(SystemConf.SABER_HOME + "/clib/new_generated.c");
            fileWriter.write(source);
            fileWriter.close();

            p = Runtime.getRuntime().exec("rm "+ SystemConf.SABER_HOME + "/clib/libCPUGen.so");

            System.out.println("Compiling CPU library...");
            p = Runtime.getRuntime().exec(new String[] {"/usr/bin/make", "-C", SystemConf.SABER_HOME+"/clib/"}, null);

            //p = Runtime.getRuntime().exec(SystemConf.SABER_HOME+"/clib/build_cpu.sh -C " + SystemConf.SABER_HOME+"/clib/");

            //p = Runtime.getRuntime().exec("/usr/bin/make -C " + SystemConf.SABER_HOME + "/clib/");
            /*String command = "/usr/bin/make -C " ;
            String [] envp = { } ;
            File dir = new File ( SystemConf.SABER_HOME + "/clib/") ;
            Process proc = Runtime.getRuntime().exec(command,envp,dir);
            proc.waitFor ( ) ;*/

            /*ProcessBuilder pb = new ProcessBuilder("build_cpu.sh", SystemConf.SABER_HOME+"/clib/");
            pb.directory(new File(SystemConf.SABER_HOME+"/clib/"));
            p = pb.start();*/

            Thread.sleep(2000);

            //System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
