package uk.ac.imperial.lsds.saber.microbenchmarks;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

public class TestAggregation {

	public static final String usage = "usage: TestAggregation";

	public static void main(String [] args) {
	
//		String executionMode = "gpu";
//		int numberOfThreads = 1;
//		int batchSize = 1048576;
//		WindowType windowType = WindowType.ROW_BASED;
//		int windowRange = 1024;
//		int windowSlide = 1024;
//		int numberOfAttributes = 6;
//		String aggregateExpression = "cnt,sum";
//		int numberOfGroups = 8;
//		int tuplesPerInsert = 32768;
//		
//		/* Parse command line arguments */
//		int i, j;
//		for (i = 0; i < args.length; ) {
//			if ((j = i + 1) == args.length) {
//				System.err.println(usage);
//				System.exit(1);
//			}
//			if (args[i].equals("--mode")) { 
//				executionMode = args[j];
//			} else
//			if (args[i].equals("--threads")) {
//				numberOfThreads = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--batch-size")) { 
//				batchSize = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-type")) { 
//				windowType = WindowType.fromString(args[j]);
//			} else
//			if (args[i].equals("--window-range")) { 
//				windowRange = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-slide")) { 
//				windowSlide = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--input-attributes")) { 
//				numberOfAttributes = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--aggregate-expression")) { 
//				aggregateExpression = args[j];
//			} else
//			if (args[i].equals("--groups")) { 
//				numberOfGroups = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--tuples-per-insert")) { 
//				tuplesPerInsert = Integer.parseInt(args[j]);
//			} else {
//				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
//				System.exit(1);
//			}
//			i = j + 1;
//		}
//		
//		SystemConf.CIRCULAR_BUFFER_SIZE = 256 * 1048576;
//		SystemConf.LATENCY_ON = false;
//		
//		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
//		SystemConf.SWITCH_THRESHOLD = 10;
//		
//		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;
//		
//		SystemConf.PARTIAL_WINDOWS = 64; // 32768;
//		SystemConf.HASH_TABLE_SIZE = 32768;
//		
//		SystemConf.UNBOUNDED_BUFFER_SIZE = 2 * 1048576;
//		
//		SystemConf.CPU = false;
//		SystemConf.GPU = false;
//		
//		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
//			SystemConf.CPU = true;
//		
//		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
//			SystemConf.GPU = true;
//		
//		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
//		
//		SystemConf.THREADS = numberOfThreads;
//		
//		QueryConf queryConf = new QueryConf (batchSize);
//		
//		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);
//		
//		int [] offsets = new int [numberOfAttributes + 1];
//		offsets[0] = 0;
//		int tupleSize = 8;
//		for (i = 1; i < numberOfAttributes + 1; i++) {
//			offsets[i] = tupleSize;
//			tupleSize += 4;
//		}
//		
//		ITupleSchema schema = new TupleSchema (offsets, tupleSize);
//		schema.setAttributeType(0,  PrimitiveType.LONG);
//		schema.setAttributeType(1, PrimitiveType.FLOAT);
//		for (i = 2; i < numberOfAttributes + 1; i++) {
//			schema.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize = schema.getTupleSize();
//		
//		String [] aggregates = aggregateExpression.split(",");
//		if (aggregates.length < 1) {
//			System.err.println("error: invalid aggregate expression");
//			System.exit(1);
//		}
//		AggregationType [] aggregationTypes = new AggregationType [aggregates.length];
//		for (i = 0; i < aggregates.length; ++i) {
//			System.out.println("[DBG] aggregation type string is " + aggregates[i]);
//			aggregationTypes[i] = AggregationType.fromString(aggregates[i]);
//		}
//		
//		FloatColumnReference [] aggregationAttributes = new FloatColumnReference [aggregates.length];
//		for (i = 0; i < aggregates.length; ++i)
//			aggregationAttributes[i] = new FloatColumnReference(1);
//		
//		Expression [] groupByAttributes = null;
//		if (numberOfGroups > 0) {
//			groupByAttributes = new Expression [] {
//				new IntColumnReference(2)
//			};
//		}
//		
//		IOperatorCode cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes);
//		System.out.println(cpuCode);
//		IOperatorCode gpuCode;
//		if (numberOfGroups == 0)
//			gpuCode = new ReductionKernel (window, aggregationTypes, aggregationAttributes, schema, batchSize);
//		else
//			gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, groupByAttributes, schema, batchSize);
//		
//		QueryOperator operator;
//		operator = new QueryOperator (cpuCode, gpuCode);
//		
//		Set<QueryOperator> operators = new HashSet<QueryOperator>();
//		operators.add(operator);
//		
//		long timestampReference = System.nanoTime();
//		
//		Query query = new Query (0, operators, schema, window, null, null, queryConf, timestampReference);
//		
//		Set<Query> queries = new HashSet<Query>();
//		queries.add(query);
//		
//		QueryApplication application = new QueryApplication(queries);
//		
//		application.setup();
//		
//		/* The path is query -> dispatcher -> handler -> aggregator */
//		if (SystemConf.CPU)
//			query.setAggregateOperator((IAggregateOperator) cpuCode);
//		else
//			query.setAggregateOperator((IAggregateOperator) gpuCode);
//		
//		/* Set up the input stream */
//		
//		byte [] data = new byte [tupleSize * tuplesPerInsert];
//		Random random = new Random();
//		
//		ByteBuffer b = ByteBuffer.wrap(data);
//		/* Fill the buffer */
//		int groupId = 1;
//		while (b.hasRemaining()) {
//			b.putLong (1);
//			b.putFloat (random.nextFloat());
//			b.putInt(groupId);
//			if (numberOfGroups > 0) {
//				groupId = (groupId + 1) % numberOfGroups;
//				if (groupId < 1)
//					groupId = 1;
//			}
//			for (i = 2; i < numberOfAttributes; i++)
//				b.putInt(1);
//		}
//		/* Reset timestamp */
//		if (SystemConf.LATENCY_ON) {
//			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
//			long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
//			b.putLong(0, packedTimestamp);
//		}
//		
//		try {
//			while (true) {	
//				application.processData (data);
//				if (SystemConf.LATENCY_ON)
//					b.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
//			}
//		} catch (Exception e) { 
//			e.printStackTrace(); 
//			System.exit(1);
//		}
	}
}
