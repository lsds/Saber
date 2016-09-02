package uk.ac.imperial.lsds.saber.microbenchmarks.hls;

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
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatMultiplication;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.NoOp;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.NoOpKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

public class Test1 {
	
	public static final String usage = "usage: TestNoOp";

	public static void main(String [] args) {
		
//		String executionMode = "hybrid";
//		int numberOfThreads = 15;
//		int batchSize = 1048576;
//		WindowType windowType1 = WindowType.ROW_BASED;
//		int windowRange1 = 1024;
//		int windowSlide1 = 1024;
//		int numberOfAttributes1 = 6;
//		WindowType windowType2 = WindowType.ROW_BASED;
//		int windowRange2 = 1024;
//		int windowSlide2 = 1024;
//		int numberOfAttributes2 = 6;
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
//			if (args[i].equals("--window-type-of-first-query")) { 
//				windowType1 = WindowType.fromString(args[j]);
//			} else
//			if (args[i].equals("--window-range-of-first-query")) { 
//				windowRange1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-slide-of-first-query")) { 
//				windowSlide1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--input-attributes-of-first-query")) { 
//				numberOfAttributes1 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-type-of-second-query")) { 
//				windowType2 = WindowType.fromString(args[j]);
//			} else
//			if (args[i].equals("--window-range-of-second-query")) { 
//				windowRange2 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--window-slide-of-second-query")) { 
//				windowSlide2 = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--input-attributes-of-second-query")) { 
//				numberOfAttributes2 = Integer.parseInt(args[j]);
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
//		SystemConf.UNBOUNDED_BUFFER_SIZE = 1 * 1048576;
//		
//		SystemConf.PARTIAL_WINDOWS = 128;
//		
//		SystemConf.LATENCY_ON = false;
//		
//		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
//		SystemConf.SWITCH_THRESHOLD = 5;
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
//		QueryConf queryConf1 = new QueryConf (batchSize);
//		WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
//		int [] offsets1 = new int [numberOfAttributes1 + 1];
//		offsets1[0] = 0;
//		int tupleSize1 = 8;
//		for (i = 1; i < numberOfAttributes1 + 1; i++) {
//			offsets1[i] = tupleSize1;
//			tupleSize1 += 4;
//		}
//		ITupleSchema schema1 = new TupleSchema (offsets1, tupleSize1);
//		schema1.setAttributeType(0,  PrimitiveType.LONG);
//		schema1.setAttributeType(1, PrimitiveType.FLOAT);
//		for (i = 2; i < numberOfAttributes1 + 1; i++) {
//			schema1.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize1 = schema1.getTupleSize();
//		
//		QueryConf queryConf2 = new QueryConf (batchSize);
//		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
//		int [] offsets2 = new int [2];
//		offsets2[0] = 0;
//		int tupleSize2 = 8;
//		for (i = 1; i < offsets2.length; i++) {
//			offsets2[i] = tupleSize2;
//			tupleSize2 += 4;
//		}
//		ITupleSchema schema2 = new TupleSchema (offsets2, tupleSize2);
//		schema2.setAttributeType(0,  PrimitiveType.LONG);
//		schema2.setAttributeType(1, PrimitiveType.FLOAT);
//		for (i = 2; i < offsets2.length; i++) {
//			schema2.setAttributeType(i, PrimitiveType.INT);
//		}
//		/* Reset tuple size */
//		tupleSize2 = schema1.getTupleSize();
//		
//		long timestampReference = System.nanoTime();
//		
//		Expression [] expressions = new Expression [2];
//		/* Always project the timestamp */
//		expressions[0] = new  LongColumnReference(0);
//		/* Introduce a floating-point arithmetic expression */
//		FloatExpression f = new FloatColumnReference(1);
//		for (i = 0; i < 1; i++)
//			f = new FloatDivision (new FloatMultiplication (new FloatConstant(3), f), new FloatConstant(2));
//		expressions[1] = f;
//		
//		IOperatorCode cpuCode1 = new Projection (expressions);
//		IOperatorCode gpuCode1 = new ProjectionKernel (schema1, expressions, batchSize, 1);
//		QueryOperator operator1;
//		operator1 = new QueryOperator (cpuCode1, gpuCode1);
//		Set<QueryOperator> operators1 = new HashSet<QueryOperator>();
//		operators1.add(operator1);
//		Query query1 = new Query (0, operators1, schema1, window1, null, null, queryConf1, timestampReference);
//		
//		AggregationType [] aggregationTypes = new AggregationType [1];
//		for (i = 0; i < aggregationTypes.length; ++i) {
//			aggregationTypes[i] = AggregationType.fromString("sum");
//		}
//		
//		FloatColumnReference [] aggregationAttributes = new FloatColumnReference [1];
//		for (i = 0; i < aggregationAttributes.length; ++i) {
//			aggregationAttributes[i] = new FloatColumnReference(1);
//		}
//		
//		IOperatorCode cpuCode2 = new Aggregation (window2, aggregationTypes, aggregationAttributes);
//		IOperatorCode gpuCode2 = new ReductionKernel (window2, aggregationTypes, aggregationAttributes, schema2, batchSize);
//		QueryOperator operator2;
//		operator2 = new QueryOperator (cpuCode2, gpuCode2);
//		Set<QueryOperator> operators2 = new HashSet<QueryOperator>();
//		operators2.add(operator2);
//		Query query2 = new Query (1, operators2, schema2, window2, null, null, queryConf2, timestampReference);
//		
//		query1.connectTo(query2);
//		
//		Set<Query> queries = new HashSet<Query>();
//		queries.add(query1);
//		queries.add(query2);
//		
//		QueryApplication application = new QueryApplication(queries);
//		
//		application.setup();
//		
//		/* The path is query -> dispatcher -> handler -> aggregator */
//		if (SystemConf.CPU)
//			query2.setAggregateOperator((IAggregateOperator) cpuCode2);
//		else
//			query2.setAggregateOperator((IAggregateOperator) gpuCode2);
//		
//		/* Set up the input stream */
//		
//		byte [] data = new byte [tupleSize1 * tuplesPerInsert];
//		
//		ByteBuffer b = ByteBuffer.wrap(data);
//		/* Fill the buffer */
//		while (b.hasRemaining()) {
//			b.putLong (1);
//			for (i = 0; i < numberOfAttributes1; i ++)
//				b.putInt(1);
//		}
//		/* Reset timestamp */
//		if (SystemConf.LATENCY_ON) {
//			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* us */
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
