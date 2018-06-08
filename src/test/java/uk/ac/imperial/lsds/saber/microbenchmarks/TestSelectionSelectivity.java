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
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class TestSelectionSelectivity {
	
	public static final String usage = "usage: TestSelectionSelectivity";

	public static void main(String [] args) {
	
//		String executionMode = "gpu";
//		int numberOfThreads = 1;
//		int batchSize = 1048576;
//		WindowType windowType = WindowType.ROW_BASED;
//		int windowRange = 1;
//		int windowSlide = 1;
//		int numberOfAttributes = 6;
//		int selectivity = 1;
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
//		SystemConf.CIRCULAR_BUFFER_SIZE = 64 * 1048576;
//		SystemConf.LATENCY_ON = false;
//		
//		SystemConf.PARTIAL_WINDOWS = 0;
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
//		IPredicate predicate =  new IntComparisonPredicate
//				(IntComparisonPredicate.LESS_OP, new IntColumnReference(1), new IntConstant(selectivity));
//		
//		IOperatorCode cpuCode = new Selection (predicate);
//		IOperatorCode gpuCode = new SelectionKernel (schema, predicate, null, batchSize);
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
//		/* Set up the input stream */
//		
//		byte [] data = new byte [tupleSize * tuplesPerInsert];
//		
//		ByteBuffer b = ByteBuffer.wrap(data);
//		/* Fill the buffer */
//		int value = 0;
//		while (b.hasRemaining()) {
//			b.putLong (1);
//			b.putInt(value);
//			value = (value + 1) % 100; 
//			for (i = 1; i < numberOfAttributes; i++)
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
