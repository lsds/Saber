package uk.ac.imperial.lsds.saber.experiments.microbenchmarks.scheduling;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.FloatComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.ORPredicate;

public class W3 {
	
	public static final String usage = "usage: W3";
	
	public static void main (String [] args) throws Exception {
		
		int batchSize = 1048576;
		
		WindowType windowType = WindowType.ROW_BASED;
		
		int windowRange = 1;
		int windowSlide = 1;
		
		int tuplesPerInsert = 16384; /* x64 = 1MB */
		
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
		
		/*
		SystemConf.CIRCULAR_BUFFER_SIZE = 512 * 1048576;
		SystemConf.LATENCY_ON = false;
		
		SystemConf.PARTIAL_WINDOWS = 0;
		
		SystemConf.THROUGHPUT_MONITOR_INTERVAL  = 200L;
		SystemConf.PERFORMANCE_MONITOR_INTERVAL = 500L;
		
		SystemConf.SCHEDULING_POLICY = SchedulingPolicy.HLS;
		
		SystemConf.SWITCH_THRESHOLD = 10;
		
		SystemConf.CPU = false;
		SystemConf.GPU = false;
		*/
		
		/*
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;
		*/
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);
		
		int [] offsets = new int [12];
		
		offsets[ 0] =  0; /*   timestamp:  long */
		offsets[ 1] =  8; /*       jobId:  long */
		offsets[ 2] = 16; /*      taskId:  long */
		offsets[ 3] = 24; /*   machineId:  long */
		offsets[ 4] = 32; /*      userId:   int */
		offsets[ 5] = 36; /*   eventType:   int */
		offsets[ 6] = 40; /*    category:   int */
		offsets[ 7] = 44; /*    priority:   int */
		offsets[ 8] = 48; /*         cpu: float */
		offsets[ 9] = 52; /*         ram: float */
		offsets[10] = 56; /*        disk: float */
		offsets[11] = 60; /* constraints:   int */
		
		ITupleSchema schema = new TupleSchema (offsets, 64);
		
		schema.setAttributeType ( 0, PrimitiveType.LONG );
		schema.setAttributeType ( 1, PrimitiveType.LONG );
		schema.setAttributeType ( 2, PrimitiveType.LONG );
		schema.setAttributeType ( 3, PrimitiveType.LONG );
		schema.setAttributeType ( 4, PrimitiveType.INT  );
		schema.setAttributeType ( 5, PrimitiveType.INT  );
		schema.setAttributeType ( 6, PrimitiveType.INT  );
		schema.setAttributeType ( 7, PrimitiveType.INT  );
		schema.setAttributeType ( 8, PrimitiveType.FLOAT);
		schema.setAttributeType ( 9, PrimitiveType.FLOAT);
		schema.setAttributeType (10, PrimitiveType.FLOAT);
		schema.setAttributeType (11, PrimitiveType.INT  );
		
		/* Load attributes of interest */
		
		int ntasks = 8812;
		
		int extraBytes = 5120 * schema.getTupleSize();
		
		ByteBuffer [] data = new ByteBuffer [ntasks];
		for (i = 0; i < ntasks; i++)
			data[i] = ByteBuffer.allocate(schema.getTupleSize() * tuplesPerInsert);
		
		ByteBuffer buffer;
		
		for (i = 0; i < ntasks; i++) {
			buffer = data[i];
			buffer.clear();
			while (buffer.hasRemaining()) {
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putLong  (1);
				buffer.putInt   (1);
				buffer.putInt   (1); // Event type
				buffer.putInt   (1); // Category
				buffer.putInt   (1); // Priority
				buffer.putFloat (1); // CPU
				buffer.putFloat (1);
				buffer.putFloat (1);
				buffer.putInt   (1);
			}
		}
		
		String dataDir = SystemConf.SABER_HOME + "/datasets/google-cluster-data/";
		
		String [] filenames = {
			dataDir +"norm-event-types.txt",
			dataDir +      "categories.txt",
			dataDir +      "priorities.txt",
			dataDir + "cpu-utilisation.txt",
		};
		
		boolean [] containsInts = { true, true, true, false };
		
		FileInputStream f;
		DataInputStream d;
		BufferedReader  b;
		
		String line = null;
		int lines = 0;
		
		int bufferIndex, tupleIndex, attributeIndex;
			
		for (i = 0; i < 4; i++) {

			lines = 0;

			bufferIndex = tupleIndex = 0;
			
			buffer = data[bufferIndex];

			/* Load file into memory */

			System.out.println(String.format("# loading file %s", filenames[i]));
			f = new FileInputStream(filenames[i]);
			d = new DataInputStream(f);
			b = new BufferedReader(new InputStreamReader(d));

			while ((line = b.readLine()) != null) {

				if (tupleIndex >= tuplesPerInsert) {
					tupleIndex = 0;
					bufferIndex ++;
				}

				lines += 1;

				buffer = data[bufferIndex];

				attributeIndex = tupleIndex * schema.getTupleSize() + 36 + (i * 4);
				
				if (containsInts[i])
					buffer.putInt(attributeIndex, Integer.parseInt(line));
				else
					buffer.putFloat(attributeIndex, Float.parseFloat(line));
				
				tupleIndex ++;
			}

			b.close();

			System.out.println(String.format("# %d lines last buffer position at %d has remaining ? %5s (%d bytes)", 
					lines, buffer.position(), buffer.hasRemaining(), buffer.remaining()));

			/* Fill in the extra lines */
			if (i != 0) {
				int destPos = buffer.capacity() - extraBytes;
				System.arraycopy(buffer.array(), 0, buffer.array(), destPos, extraBytes);
			}
		}
		
		IPredicate [] andPredicates = new IPredicate [2];
		
		andPredicates[0] = new IntComparisonPredicate 
				(IntComparisonPredicate.EQUAL_OP,   new IntColumnReference(5), new IntConstant(3));
		
		int comparisons = 500;
		
		IPredicate [] orPredicates = new IPredicate [comparisons];
		int a = 8;
		for (i = 0; i < comparisons - 1; i++) {
			orPredicates[i] = new FloatComparisonPredicate 
					(FloatComparisonPredicate.LESS_OP, new FloatColumnReference(a), new FloatConstant(0));
			a ++;
			if (a == 11) a = 8;
		}
		
		orPredicates[comparisons - 1] = new FloatComparisonPredicate 
				(FloatComparisonPredicate.NONLESS_OP, new FloatColumnReference(8), new FloatConstant(0));
		
		andPredicates[1] = new ORPredicate (orPredicates);
		
		IPredicate predicate = new ANDPredicate (andPredicates);
		
		StringBuilder customPredicate = new StringBuilder ();
		customPredicate.append("int value = 1;\n");
		customPredicate.append("int attribute_value1 = __bswap32(p->tuple._5);\n");
		customPredicate.append("float attribute_value2 = __bswapfp(p->tuple._8);\n");
		customPredicate.append("value = value & (attribute_value1 == 3) & ");
		customPredicate.append("(");
		for (i = 0; i < comparisons; i++) {
			if (i < 499)
				customPredicate.append(String.format("(attribute_value2 < 0) | "));
			else
				customPredicate.append(String.format("(attribute_value2 >= 0)"));
		}
		customPredicate.append(");\n");
		customPredicate.append("return value;\n");
		
		IOperatorCode cpuCode = new Selection (predicate);
		IOperatorCode gpuCode = new SelectionKernel (schema, predicate, customPredicate.toString(), batchSize);
		
		System.out.println(cpuCode);
		
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
		
		/* Push the input stream */
		
		i = 0;
		while (true) {
			application.processData (data[i].array());
			i ++;
			i = i % ntasks;
		}
	}
}
