package uk.ac.imperial.lsds.saber.microbenchmarks;

import java.nio.ByteBuffer;
import java.util.HashSet;
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
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.ORPredicate;

public class TestAdaptivity {
	
	public static final String usage = "usage: TestAdaptivity";

	public static void main (String [] args) {
	
//		String executionMode = "hybrid";
//		int numberOfThreads = 15;
//		int batchSize = 1048576;
//		WindowType windowType = WindowType.ROW_BASED;
//		int windowRange = 1;
//		int windowSlide = 1;
//		int numberOfAttributes = 6;
//		int minSelectivity = 1;
//		int maxSelectivity = 50;
//		int comparisons = 20;
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
//			if (args[i].equals("--min-selectivity")) { 
//				minSelectivity = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--max-selectivity")) { 
//				maxSelectivity = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--comparisons")) { 
//				comparisons = Integer.parseInt(args[j]);
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
//		SystemConf.CIRCULAR_BUFFER_SIZE = 1024 * 1048576;
//		SystemConf.LATENCY_ON = false;
//		
//		SystemConf.PARTIAL_WINDOWS = 0;
//		
//		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
//		SystemConf.SWITCH_THRESHOLD = 20;
//		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 500L;
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
//		for (i = 1; i < numberOfAttributes + 1; i++) {
//			schema.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize = schema.getTupleSize();
//		
//		/* Phase change(s)
//		 * 
//		 * Query selectivity changes from 1% to 50%.
//		 * 
//		 * The predicate is:
//		 * 
//		 * ("1" >= 0) & (("1" < 1) | ("1" < 2) | ("1" < 3) | ... | ("1" < N - 1))
//		 * 
//		 * where:
//		 * 		"1" is attribute (or column) "1";
//		 * 		1,2,...,N are constants
//		 * 		N is the number of predicates
//		 * 
//		 * Column "1" values are 0, -1,-1,-1,...,-1, 0, -1,-1,-1,...,-1, 0,...
//		 *                          <---99 times--->
//		 * 
//		 * Query selectivity is 1%. The first predicate is true.
//		 * 
//		 * Column "1" values change to N-2,N-2,N-2,..., N,N,N,N,...,N, ...
//		 *                             <---50 times---> <--50 times-->
//		 * 
//		 * Query selectivity is 50%. The last predicate is true.
//		 * 
//		 */
//		if (comparisons < 2) {
//			System.err.println("error: number of comparisons must be greater than 1");
//			System.exit(1);
//		}
//		
//		IPredicate [] andPredicates = new IPredicate [2];
//		IPredicate []  orPredicates = new IPredicate [comparisons - 1];
//		
//		andPredicates[0] = new IntComparisonPredicate
//				(IntComparisonPredicate.NONLESS_OP, new IntColumnReference(1), new IntConstant(0));
//		
//		for (i = 0; i < comparisons - 1; i++) {
//			orPredicates[i] = new IntComparisonPredicate
//					(IntComparisonPredicate.LESS_OP, new IntColumnReference(1), new IntConstant(i + 1));
//		}
//		andPredicates[1] = new ORPredicate (orPredicates);
//		
//		IPredicate predicate = new ANDPredicate (andPredicates);
//		
//		StringBuilder customPredicate = new StringBuilder ();
//		customPredicate.append("int value = 1;\n");
//		customPredicate.append("int attribute_value = __bswap32(p->tuple._1);\n");
//		customPredicate.append("value = value & (attribute_value >= 0) & ");
//		customPredicate.append("(");
//		for (i = 0; i < comparisons - 1; i++) {
//			customPredicate.append(String.format("(attribute_value < %d)", (i + 1)));
//			if (i != (comparisons - 2))
//				customPredicate.append(" | ");
//		}
//		customPredicate.append(");\n");
//		customPredicate.append("return value;\n");
//		
//		IOperatorCode cpuCode = new Selection (predicate);
//		IOperatorCode gpuCode = new SelectionKernel (schema, predicate, customPredicate.toString(), batchSize);
//		
//		System.out.println(cpuCode);
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
//		query.setName("Query");
//		
//		Set<Query> queries = new HashSet<Query>();
//		queries.add(query);
//		
//		QueryApplication application = new QueryApplication(queries);
//		
//		application.setup();
//		
//		/* Set up the input stream */
//		
//		byte [] data1 = new byte [tupleSize * tuplesPerInsert];
//		byte [] data2 = new byte [tupleSize * tuplesPerInsert];
//		
//		ByteBuffer b1 = ByteBuffer.wrap(data1);
//		ByteBuffer b2 = ByteBuffer.wrap(data2);
//		
//		int value, count;
//		/* Fill the first buffer */
//		value = 0;
//		count = 0;
//		while (b1.hasRemaining()) {
//			b1.putLong (1);
//			b1.putInt(value);
//			for (i = 1; i < numberOfAttributes; i++)
//				b1.putInt(1);
//			count ++;
//			if (count >= minSelectivity && count < 100) {
//				value = -1;
//			} else if (count >= 100) { /* Reset */
//				count = 0;
//				value = 0;
//			}
//		}
//		/* Reset timestamp */
//		if (SystemConf.LATENCY_ON) {
//			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
//			long packedTimestamp = Utils.pack(systemTimestamp, b1.getLong(0));
//			b1.putLong(0, packedTimestamp);
//		}
//		
//		/* Fill the second buffer */
//		value = (comparisons - 2);
//		count = 0;
//		while (b2.hasRemaining()) {
//			b2.putLong (1);
//			b2.putInt(value);
//			for (i = 1; i < numberOfAttributes; i++)
//				b2.putInt(1);
//			count ++;
//			if (count >= maxSelectivity && count < 100) {
//				value = comparisons;
//				// System.out.println("[DBG] value is " + value);
//			} else if (count >= 100) { /* Reset */
//				count = 0;
//				value = (comparisons - 2);
//			}
//		}
//		/* Reset timestamp */
//		if (SystemConf.LATENCY_ON) {
//			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
//			long packedTimestamp = Utils.pack(systemTimestamp, b2.getLong(0));
//			b2.putLong(0, packedTimestamp);
//		}
//		
//		/* Populate stream */
//		boolean phaseChange = false;
//		long numberOfInserts = 0L;
//		long switchThreshold = 50000 * 30;
//		try {
//			while (true) {
//				if (phaseChange) {
//					application.processData (data2);
//					if (SystemConf.LATENCY_ON)
//						b2.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
//				} else {
//					application.processData (data1);
//					if (SystemConf.LATENCY_ON)
//						b1.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
//				}
//				if ((++numberOfInserts) == switchThreshold) {
//					System.out.println("========== Phase change ==========");
//					phaseChange = true;
//					// application.phaseChange();
//				}
//			}
//		} catch (Exception e) { 
//			e.printStackTrace(); 
//			System.exit(1);
//		}
	}
}
