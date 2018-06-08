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
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatMultiplication;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;

public class TestProjection {
	
	public static final String usage = "usage: TestProjection"
	+ " [ --batch-size ]"
	+ " [ --window-type ]" 
	+ " [ --window-size ]" 
	+ " [ --window-slide ]"
	+ " [ --input-attributes ]"
	+ " [ --projected-attributes ]"
	+ " [ --expression-depth ]"
	+ " [ --tuples-per-insert ]";
	
	public static void main (String [] args) {
		
		try {
		
		/* Application-specific arguments */
		
		int batchSize = 1048576;
		
		WindowType windowType = WindowType.ROW_BASED;
		
		int windowSize = 1;
		int windowSlide = 1;
		
		int numberOfInputAttributes = 6;
		int numberOfProjectedAttributes = 1;
		
		int projectionExpressionDepth = 0;
		
		int tuplesPerInsert = 32768;
		
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
				numberOfInputAttributes = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--projected-attributes")) { 
				numberOfProjectedAttributes = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--expression-depth")) { 
				projectionExpressionDepth = Integer.parseInt(args[j]);
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
		 * The first attribute is the timestamp, the second attribute is 
		 * a float on which we perform a the aggregation operations, 
		 * followed by `numberOfAttributes - 1` integer attributes.
		 */
		
		int [] offsets = new int [numberOfInputAttributes + 1];
		
		offsets[0] = 0;
		int tupleSize = 8;
		
		for (i = 1; i < numberOfInputAttributes + 1; i++) {
			offsets[i] = tupleSize;
			tupleSize += 4;
		}
		
		ITupleSchema schema = new TupleSchema (offsets, tupleSize);

		schema.setAttributeType(0, PrimitiveType.LONG );
		schema.setAttributeType(1, PrimitiveType.FLOAT);
		
		for (i = 2; i < numberOfInputAttributes + 1; i++) {
			schema.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* tupleSize equals schema.getTupleSize() */
		tupleSize = schema.getTupleSize();
		
		/* Build the projection query */
		
		Expression [] expressions = new Expression [numberOfProjectedAttributes + 1];
		/* Always project the timestamp */
		expressions[0] = new LongColumnReference(0);
		
		for (i = 0; i < numberOfProjectedAttributes; ++i)
			expressions[i + 1] = new IntColumnReference ((i % (numberOfInputAttributes)) + 1);
		
		/* Introduce 0 or more floating-point arithmetic expressions */
		FloatExpression f = new FloatColumnReference(1);
		for (i = 0; i < projectionExpressionDepth; i++)
			f = new FloatDivision (new FloatMultiplication (new FloatConstant(3), f), new FloatConstant(2));
		
		expressions[1] = f;
		
		IOperatorCode cpuCode = new Projection (expressions);
		IOperatorCode gpuCode = new ProjectionKernel (schema, expressions, batchSize, projectionExpressionDepth);
		
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
			for (i = 0; i < numberOfInputAttributes; i ++)
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
