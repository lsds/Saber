package uk.ac.imperial.lsds.saber.experiments.microbenchmarks;

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
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class TestThetaJoin {
	
	public static final String usage = "usage: TestThetaJoin"
	+ " [ --batch-size ]"
	+ " [ --window-type-of-first-stream ]" 
	+ " [ --window-size-of-first-stream ]" 
	+ " [ --window-slide-of-first-stream ]"
	+ " [ --input-attributes-of-first-stream ]"
	+ " [ --window-type-of-second-stream ]" 
	+ " [ --window-size-of-second-stream ]" 
	+ " [ --window-slide-of-second-stream ]"
	+ " [ --input-attributes-of-second-stream ]"
	+ " [ --comparisons ]"
	+ " [ --selectivity ]"
	+ " [ --tuples-per-insert ]";
	
	public static void main (String [] args) {
		
		try {
		
		/* Application-specific arguments */
		
		int batchSize = 1048576;
		
		/* First stream */
		WindowType windowType1 = WindowType.ROW_BASED;
		
		int windowSize1 = 1024;
		int windowSlide1 = 1024;
		
		int numberOfAttributes1 = 6;
		
		/* Second stream */
		WindowType windowType2 = WindowType.ROW_BASED;
		
		int windowSize2 = 1024;
		int windowSlide2 = 1024;
		
		int numberOfAttributes2 = 6;
		
		int selectivity = 1;
		int numberOfComparisons = 0;
		
		int tuplesPerInsert = 128;
		
		/* Parse application-specific command-line arguments */
		
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("--batch-size")) { 
				batchSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-type-of-first-stream")) { 
				windowType1 = WindowType.fromString(args[j]);
			} else
			if (args[i].equals("--window-size-of-first-stream")) { 
				windowSize1 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-slide-of-first-stream")) { 
				windowSlide1 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--input-attributes-of-first-stream")) { 
				numberOfAttributes1 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-type-of-second-stream")) { 
				windowType2 = WindowType.fromString(args[j]);
			} else
			if (args[i].equals("--window-size-of-second-stream")) { 
				windowSize2 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-slide-of-second-stream")) { 
				windowSlide2 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--input-attributes-of-second-stream")) { 
				numberOfAttributes2 = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--comparisons")) { 
				numberOfComparisons = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--selectivity")) { 
				selectivity = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--tuples-per-insert")) { 
				tuplesPerInsert = Integer.parseInt(args[j]);
			} else 
			if (! SystemConf.parse(args[i], args[j])) {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window1 = new WindowDefinition (windowType1, windowSize1, windowSlide1);
		
		/*
		 * Setup first input stream
		 */
		
		int [] offsets1 = new int [numberOfAttributes1 + 1];
		
		offsets1[0] = 0;
		int tupleSize1 = 8;
		
		for (i = 1; i < numberOfAttributes1 + 1; i++) {
			offsets1[i] = tupleSize1;
			tupleSize1 += 4;
		}
		
		ITupleSchema schema1 = new TupleSchema (offsets1, tupleSize1);
		
		schema1.setAttributeType(0,  PrimitiveType.LONG);
		
		for (i = 1; i < numberOfAttributes1 + 1; i++) {
			schema1.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* tupleSize1 equals schema1.getTupleSize() */
		tupleSize1 = schema1.getTupleSize();
		
		WindowDefinition window2 = new WindowDefinition (windowType2, windowSize2, windowSlide2);
		
		/*
		 * Setup second input stream
		 */
		
		int [] offsets2 = new int [numberOfAttributes2 + 1];
		
		offsets2[0] = 0;
		int tupleSize2 = 8;
		
		for (i = 1; i < numberOfAttributes2 + 1; i++) {
			offsets2[i] = tupleSize2;
			tupleSize2 += 4;
		}
		
		ITupleSchema schema2 = new TupleSchema (offsets2, tupleSize2);
		
		schema2.setAttributeType(0,  PrimitiveType.LONG);
		
		for (i = 1; i < numberOfAttributes2 + 1; i++) {
			schema2.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* tupleSize2 equals schema2.getTupleSize() */
		tupleSize2 = schema2.getTupleSize();
		
		/* Build the selection query */
		
		IPredicate predicate = null;
		IPredicate [] predicates = null;
		
		if (selectivity > 0 && numberOfComparisons == 0) {
			
			predicate =  
				new IntComparisonPredicate(IntComparisonPredicate.LESS_OP, new IntColumnReference(1), new IntColumnReference(1));
			
		} else 
		if (selectivity == 0 && numberOfComparisons > 0) {
			
			predicates = new IPredicate [numberOfComparisons];
			for (i = 0; i < numberOfComparisons; i++) {
				predicates[i] = 
					new IntComparisonPredicate(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(1), new IntColumnReference(1));
			}
			predicate = new ANDPredicate (predicates);
			
		} else {
			System.err.println(String.format("error: invalid configuration: %d%% selectivity and %d comparisons", 
					selectivity, numberOfComparisons));
			System.exit(1);
		}
		
		IOperatorCode cpuCode = new ThetaJoin (schema1, schema2, predicate);
		IOperatorCode gpuCode = new ThetaJoinKernel (schema1, schema2, predicate, null, batchSize, SystemConf.UNBOUNDED_BUFFER_SIZE);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		long timestampReference = System.nanoTime();
		
		Query query = new Query (0, operators, schema1, window1, schema2, window2, queryConf, timestampReference);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query);
		
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		/* Set up the input streams */
		
		byte [] data1 = new byte [tupleSize1 * tuplesPerInsert];
		byte [] data2 = new byte [tupleSize2 * tuplesPerInsert];
		
		ByteBuffer b1 = ByteBuffer.wrap (data1);
		ByteBuffer b2 = ByteBuffer.wrap (data2);
		
		/* Fill the first buffer buffer */
		int value = 0;
		while (b1.hasRemaining()) {
			b1.putLong (1);
			if (selectivity > 0) {
				b1.putInt(value);
				value = (value + 1) % 100;
				for (i = 1; i < numberOfAttributes1; i ++)
					b1.putInt(1);
			} else {
				for (i = 0; i < numberOfAttributes1; i ++)
					b1.putInt(1);
			}
		}
		
		/* Fill the second buffer */
		while (b2.hasRemaining()) {
			b2.putLong (1);
			if (selectivity > 0) {
				b2.putInt(selectivity);
				for (i = 1; i < numberOfAttributes2; i ++)
					b2.putInt(1);
			} else {
				for (i = 0; i < numberOfAttributes2; i ++)
					b2.putInt(1);
			}
		}
		
		if (SystemConf.LATENCY_ON) {
			/* Reset timestamp */
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* us */
			long packedTimestamp = Utils.pack(systemTimestamp, b1.getLong(0));
			b1.putLong(0, packedTimestamp);
		}
		
		while (true) {
			
			application.processFirstStream  (data1);
			application.processSecondStream (data2);
			
			if (SystemConf.LATENCY_ON)
				b1.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), b1.getLong(0)));
		}
		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
