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
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.NoOp;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.NoOpKernel;

public class TestNoop {
	
	public static final String usage = "usage: TestNoop"
	+ " [ --batch-size ]"
	+ " [ --window-type ]" 
	+ " [ --window-size ]" 
	+ " [ --window-slide ]"
	+ " [ --input-attributes ]"
	+ " [ --tuples-per-insert ]";
	
	public static void main (String [] args) {
		
		try {
		
		/* Application-specific arguments */
		
		int batchSize = 1048576 / 4;
		
		WindowType windowType = WindowType.ROW_BASED;
		
		int windowSize = 1;
		int windowSlide = 1;
		
		int numberOfAttributes = 6;
		
		int tuplesPerInsert = 32768 / 32;
		
		SystemConf.CIRCULAR_BUFFER_SIZE = 4 * 1048576;
		
		SystemConf.UNBOUNDED_BUFFER_SIZE = batchSize;
		
		SystemConf.THREADS = 1;
		
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
			if (args[i].equals("--window-type")) { 
				windowType = WindowType.fromString(args[j]);
			} else
			if (args[i].equals("--window-size")) { 
				windowSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-slide")) { 
				windowSlide = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--input-attributes")) { 
				numberOfAttributes = Integer.parseInt(args[j]);
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
		
		WindowDefinition window = new WindowDefinition (windowType, windowSize, windowSlide);
		
		/* Setup input stream schema:
		 * 
		 * The first attribute is the timestamp, followed 
		 * by `numberOfAttributes` integer attributes.
		 */
		
		int [] offsets = new int [numberOfAttributes + 1];
		
		offsets[0] = 0;
		int tupleSize = 8;
		
		for (i = 1; i < numberOfAttributes + 1; i++) {
			offsets[i] = tupleSize;
			tupleSize += 4;
		}
		
		ITupleSchema schema = new TupleSchema (offsets, tupleSize);
		
		schema.setAttributeType(0, PrimitiveType.LONG);
		
		for (i = 1; i < numberOfAttributes + 1; i++) {
			schema.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* tupleSize equals schema.getTupleSize() */
		tupleSize = schema.getTupleSize();
		
		/* Setup the query */
		
		IOperatorCode cpuCode = new NoOp ();
		IOperatorCode gpuCode = new NoOpKernel (schema, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		long timestampReference = System.nanoTime();
		
		Query query = new Query (0, operators, schema, window, null, null, queryConf, timestampReference);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query);
		
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		/* Set up the input stream */
		
		byte [] data = new byte [tupleSize * tuplesPerInsert];
		
		ByteBuffer b = ByteBuffer.wrap (data);
		
		/* Fill the buffer */
		while (b.hasRemaining()) {
			b.putLong (1);
			for (i = 0; i < numberOfAttributes; i ++)
				b.putInt(1);
		}
		
		if (SystemConf.LATENCY_ON) {
			/* Reset timestamp */
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* us */
			long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
			b.putLong(0, packedTimestamp);
		}
		
		while (true) {
				
			application.processData (data);
				
			if (SystemConf.LATENCY_ON)
				b.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
		}
		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
