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
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class TestThetaJoinSelectivity {
	
	public static final String usage = "usage: TestThetaJoinSelectivity";

	public static void main(String [] args) {
	
//		String executionMode = "cpu";
//		int numberOfThreads = 1;
//		int batchSize = 1048576;
//		WindowType windowType1 = WindowType.ROW_BASED;
//		int windowRange1 = 1024;
//		int windowSlide1 = 1024;
//		int numberOfAttributes1 = 6;
//		WindowType windowType2 = WindowType.ROW_BASED;
//		int windowRange2 = 1024;
//		int windowSlide2 = 1024;
//		int numberOfAttributes2 = 6;
//		int selectivity = 1;
//		int tuplesPerInsert = 128;
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
//			if (args[i].equals("--window-type-of-first-stream")) { 
//				windowType1 = WindowType.fromString(args[j]);
//			} else
//			if (args[i].equals("--window-range-of-first-stream")) { 
//				windowRange1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-slide-of-first-stream")) { 
//				windowSlide1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--input-attributes-of-first-stream")) { 
//				numberOfAttributes1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-type-of-second-stream")) { 
//				windowType2 = WindowType.fromString(args[j]);
//			} else
//			if (args[i].equals("--window-range-of-second-stream")) { 
//				windowRange2 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-slide-of-second-stream")) { 
//				windowSlide2 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--input-attributes-of-second-stream")) { 
//				numberOfAttributes2 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--selectivity")) { 
//				selectivity = Integer.parseInt(args[j]);
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
//		SystemConf.CIRCULAR_BUFFER_SIZE = 32 * 1048576;
//		
//		SystemConf.UNBOUNDED_BUFFER_SIZE = 16 * 1048576;
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
//		WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
//		
//		int [] offsets1 = new int [numberOfAttributes1 + 1];
//		offsets1[0] = 0;
//		int tupleSize1 = 8;
//		for (i = 1; i < numberOfAttributes1 + 1; i++) {
//			offsets1[i] = tupleSize1;
//			tupleSize1 += 4;
//		}
//		
//		ITupleSchema schema1 = new TupleSchema (offsets1, tupleSize1);
//		schema1.setAttributeType(0,  PrimitiveType.LONG);
//		for (i = 1; i < numberOfAttributes1 + 1; i++) {
//			schema1.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize1 = schema1.getTupleSize();
//		
//		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
//		
//		int [] offsets2 = new int [numberOfAttributes2 + 1];
//		offsets2[0] = 0;
//		int tupleSize2 = 8;
//		for (i = 1; i < numberOfAttributes2 + 1; i++) {
//			offsets2[i] = tupleSize2;
//			tupleSize2 += 4;
//		}
//		
//		ITupleSchema schema2 = new TupleSchema (offsets2, tupleSize2);
//		schema2.setAttributeType(0,  PrimitiveType.LONG);
//		for (i = 1; i < numberOfAttributes2 + 1; i++) {
//			schema2.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize2 = schema2.getTupleSize();
//		
//		IPredicate predicate =  new IntComparisonPredicate
//				(IntComparisonPredicate.LESS_OP, new IntColumnReference(1), new IntColumnReference(1));
//		
//		IOperatorCode cpuCode = new ThetaJoin (schema1, schema2, predicate);
//		IOperatorCode gpuCode = new ThetaJoinKernel (schema1, schema2, predicate, null, batchSize, 1048576);
//		
//		QueryOperator operator;
//		operator = new QueryOperator (cpuCode, gpuCode);
//		
//		Set<QueryOperator> operators = new HashSet<QueryOperator>();
//		operators.add(operator);
//		
//		long timestampReference = System.nanoTime();
//		
//		Query query = new Query (0, operators, schema1, window1, schema2, window2, queryConf, timestampReference);
//		
//		Set<Query> queries = new HashSet<Query>();
//		queries.add(query);
//		
//		QueryApplication application = new QueryApplication(queries);
//		
//		application.setup();
//		
//		/* Set up the input streams */
//		
//		byte [] data1 = new byte [tupleSize1 * tuplesPerInsert];
//		byte [] data2 = new byte [tupleSize2 * tuplesPerInsert];
//		
//		ByteBuffer b1 = ByteBuffer.wrap(data1);
//		ByteBuffer b2 = ByteBuffer.wrap(data2);
//		
//		/* Fill the buffers */
//		
//		int value = 0;
//		while (b1.hasRemaining()) {
//			b1.putLong (1);
//			b1.putInt(value);
//			value = (value + 1) % 100; 
//			for (i = 1; i < numberOfAttributes1; i++)
//				b1.putInt(1);
//		}
//		
//		while (b2.hasRemaining()) {
//			b2.putLong (1);
//			b2.putInt(selectivity);
//			for (i = 1; i < numberOfAttributes2; i++)
//				b2.putInt(1);
//		}
//		
//		/* Reset timestamp */
//		if (SystemConf.LATENCY_ON) {
//			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
//			long packedTimestamp = Utils.pack(systemTimestamp, b1.getLong(0));
//			b1.putLong(0, packedTimestamp);
//		}
//		
//		try {
//			while (true) {	
//				application.processFirstStream  (data1);
//				application.processSecondStream (data2);
//				if (SystemConf.LATENCY_ON)
//					b1.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), b1.getLong(0)));
//			}
//		} catch (Exception e) { 
//			e.printStackTrace(); 
//			System.exit(1);
//		}
	}
}
