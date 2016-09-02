package uk.ac.imperial.lsds.saber.experiments.microbenchmarks.scheduling;

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
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;

public class W1 {
	
	public static final String usage = "usage: W1";
	
	public static void main (String [] args) {
		
		/* Application-specific arguments */
		
		int batchSize = 1048576;
		
		/* First query */
		WindowType windowType1 = WindowType.ROW_BASED;
		
		int windowRange1 = 1024;
		int windowSlide1 = 1024;
		
		int numberOfAttributes1 = 6;
		
		/* Second query */
		WindowType windowType2 = WindowType.ROW_BASED;
		
		int windowRange2 = 1024;
		int windowSlide2 = 512;
		
		int tuplesPerInsert = 32768;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (! SystemConf.parse(args[i], args[j])) {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
		SystemConf.dump();
		
		QueryConf queryConf1 = new QueryConf (batchSize);
		
		WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
		
		/* Setup first input stream schema:
		 * 
		 * The first attribute is the timestamp, the second a float, followed
		 * by `numberOfAttributes - 1` integer attributes.
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
		schema1.setAttributeType(1, PrimitiveType.FLOAT);
		
		for (i = 2; i < numberOfAttributes1 + 1; i++) {
			schema1.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* Reset tuple size */
		tupleSize1 = schema1.getTupleSize();
		
		QueryConf queryConf2 = new QueryConf (batchSize);
		
		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
		
		int [] offsets2 = new int [numberOfAttributes1 + 1];
		
		offsets2[0] = 0;
		int tupleSize2 = 8;
		
		for (i = 1; i < offsets2.length; i++) {
			offsets2[i] = tupleSize2;
			tupleSize2 += 4;
		}
		
		ITupleSchema schema2 = new TupleSchema (offsets2, tupleSize2);
		
		schema2.setAttributeType(0,  PrimitiveType.LONG);
		schema2.setAttributeType(1, PrimitiveType.FLOAT);
		
		for (i = 2; i < offsets2.length; i++) {
			schema2.setAttributeType(i, PrimitiveType.INT);
		}
		
		/* Reset tuple size */
		tupleSize2 = schema2.getTupleSize();

		long timestampReference = System.nanoTime();
		
		/* Setup the first query */
		
		Expression [] expressions = new Expression [2];
		
		/* Always project the timestamp */
		expressions[0] = new  LongColumnReference(0);
		
		/* Introduce 100 floating-point arithmetic expressions */
		FloatExpression f = new FloatColumnReference(1);
		for (i = 0; i < 100; i++)
			f = new FloatDivision (new FloatMultiplication (new FloatConstant(3), f), new FloatConstant(2));
		
		expressions[1] = f;
		
		IOperatorCode cpuCode1 = new Projection (expressions);
		IOperatorCode gpuCode1 = new ProjectionKernel (schema1, expressions, batchSize, 100);
		
		QueryOperator operator1;
		operator1 = new QueryOperator (cpuCode1, gpuCode1);
		
		Set<QueryOperator> operators1 = new HashSet<QueryOperator>();
		operators1.add(operator1);
		
		Query query1 = new Query (0, operators1, schema1, window1, null, null, queryConf1, timestampReference);
		
		/* Setup the second query */
		
		AggregationType [] aggregationTypes = new AggregationType [1];
		
		for (i = 0; i < aggregationTypes.length; ++i) {
			aggregationTypes[i] = AggregationType.fromString("cnt");
		}
		
		FloatColumnReference [] aggregationAttributes = new FloatColumnReference [1];
		
		for (i = 0; i < aggregationAttributes.length; ++i) {
			aggregationAttributes[i] = new FloatColumnReference(1);
		}
		
		Expression [] groupByAttributes = new Expression [] {
			new IntColumnReference(2)
		};
		
		IOperatorCode cpuCode2 = new Aggregation (window2, aggregationTypes, aggregationAttributes, groupByAttributes);
		IOperatorCode gpuCode2 = new AggregationKernel (window2, aggregationTypes, aggregationAttributes, groupByAttributes, schema2, batchSize);
		
		QueryOperator operator2;
		operator2 = new QueryOperator (cpuCode2, gpuCode2);
		
		Set<QueryOperator> operators2 = new HashSet<QueryOperator>();
		operators2.add(operator2);
		
		Query query2 = new Query (1, operators2, schema2, window2, null, null, queryConf2, timestampReference);
		
		/* Put queries together */
		
		query1.connectTo(query2);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query1);
		queries.add(query2);
		
		QueryApplication application = new QueryApplication (queries);

		application.setup();

		/* The path is query -> dispatcher -> handler -> aggregator */
		if (SystemConf.CPU)
			query2.setAggregateOperator((IAggregateOperator) cpuCode2);
		else
			query2.setAggregateOperator((IAggregateOperator) gpuCode2);

		/* Set up the input stream */

		byte [] data = new byte [tupleSize1 * tuplesPerInsert];

		ByteBuffer b = ByteBuffer.wrap(data);
		
		/* Fill the buffer */
		while (b.hasRemaining()) {
			b.putLong (1);
			for (i = 0; i < numberOfAttributes1; i ++)
				b.putInt(1);
		}
		
		/* Reset timestamp */
		
		if (SystemConf.LATENCY_ON) {
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* us */
			long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
			b.putLong(0, packedTimestamp);
		}

		try {
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
