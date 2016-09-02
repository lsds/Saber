package uk.ac.imperial.lsds.saber.microbenchmarks;

import java.io.File;
import java.io.FileInputStream;
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
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
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

public class DemoWithGoogleClusterData {
	
	public static final String usage = "usage: DemoWithGoogleClusterData";

	public static void main (String [] args) throws Exception {
		
//		String dataDir = SystemConf.SABER_HOME + "/datasets/google-cluster-data/";
//		
//		String inputFile = "saber-debs-demo.data";
//		
//		int stretch = 20;
//		
//		String executionMode = "hybrid";
//		
//		int numberOfThreads = 15;
//		
//		int batchSize = 1048576;
//		
//		WindowType windowType = WindowType.ROW_BASED;
//		
//		int windowRange = 1024;
//		int windowSlide = 1024;
//		
//		/* Parse command line arguments */
//		
//		int i, j;
//		
//		for (i = 0; i < args.length; ) {
//			
//			if ((j = i + 1) == args.length) {
//				System.err.println(usage);
//				System.exit(1);
//			}
//			
//			if (args[i].equals("--mode")) { 
//				
//				executionMode = args[j];
//			} else
//			if (args[i].equals("--data-dir")) {
//				
//				dataDir = args[j];
//			} else
//			if (args[i].equals("--input-file")) {
//					
//				inputFile = args[j];
//			} else
//			if (args[i].equals("--stretch")) {
//				
//				stretch = Integer.parseInt(args[j]);
//			} else
//			if (args[i].equals("--threads")) {
//				
//				numberOfThreads = Integer.parseInt(args[j]);
//			} else {
//				
//				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
//				System.exit(1);
//			}
//			i = j + 1;
//		}
//		
//		/* System configuration */
//		
//		SystemConf.WWW = true;
//		
//		SystemConf.CIRCULAR_BUFFER_SIZE = 512 * 1048576;
//		SystemConf.LATENCY_ON = false;
//		
//		SystemConf.PARTIAL_WINDOWS = 0;
//		
//		SystemConf.THROUGHPUT_MONITOR_INTERVAL  = 100L;
//		SystemConf.PERFORMANCE_MONITOR_INTERVAL = 500L;
//		
//		SystemConf.SCHEDULING_POLICY = SchedulingPolicy.HLS;
//		
//		SystemConf.SWITCH_THRESHOLD = 5;
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
//		/* Setup Google cluster data schema */
//		
//		int [] offsets = new int [12];
//		
//		offsets[ 0] =  0; /*   timestamp:  long */
//		offsets[ 1] =  8; /*       jobId:  long */
//		offsets[ 2] = 16; /*      taskId:  long */
//		offsets[ 3] = 24; /*   machineId:  long */
//		offsets[ 4] = 32; /*      userId:   int */
//		offsets[ 5] = 36; /*   eventType:   int */
//		offsets[ 6] = 40; /*    category:   int */
//		offsets[ 7] = 44; /*    priority:   int */
//		offsets[ 8] = 48; /*         cpu: float */
//		offsets[ 9] = 52; /*         ram: float */
//		offsets[10] = 56; /*        disk: float */
//		offsets[11] = 60; /* constraints:   int */
//		
//		ITupleSchema schema = new TupleSchema (offsets, 64);
//		
//		schema.setAttributeType ( 0, PrimitiveType.LONG );
//		schema.setAttributeType ( 1, PrimitiveType.LONG );
//		schema.setAttributeType ( 2, PrimitiveType.LONG );
//		schema.setAttributeType ( 3, PrimitiveType.LONG );
//		schema.setAttributeType ( 4, PrimitiveType.INT  );
//		schema.setAttributeType ( 5, PrimitiveType.INT  );
//		schema.setAttributeType ( 6, PrimitiveType.INT  );
//		schema.setAttributeType ( 7, PrimitiveType.INT  );
//		schema.setAttributeType ( 8, PrimitiveType.FLOAT);
//		schema.setAttributeType ( 9, PrimitiveType.FLOAT);
//		schema.setAttributeType (10, PrimitiveType.FLOAT);
//		schema.setAttributeType (11, PrimitiveType.INT  );
//		
//		int numberOfTasks = 8812;
//		int taskSize = 1048576;
//		
//		ByteBuffer [] data = new ByteBuffer [numberOfTasks];
//		for (i = 0; i < numberOfTasks; i++)
//			data[i] = ByteBuffer.allocate(taskSize);
//		
//		ByteBuffer buffer; /* Pointer */
//		
//		/* Load file into memory */
//		
//		File file = new File (dataDir + inputFile);
//		FileInputStream f = new FileInputStream(file);
//		
//		long length = file.length();
//		long bytes = 0L;
//		/* Bundle offsets */
//		byte [] L = new byte [4];
//		
//		long percent_ = 0L, _percent = 0L;
//		
//		int idx = 0;
//		int r;
//		
//		while (bytes < length) {
//			
//			/* Read offset */
//			f.read (L, 0, 4);
//			int _bundle_ = ByteBuffer.wrap(L).getInt();
//			
//			if (_bundle_ != taskSize) {
//				System.err.println(String.format("error: invalid offset no.%d in %s", (idx + 1), inputFile));
//				System.exit(1);
//			}
//			
//			buffer = data[idx++];
//			
//			r = f.read (buffer.array(), 0, _bundle_);
//			if (r != _bundle_) {
//				System.err.println(String.format("error: read() of bundle no.%d returned less than %d bytes", (idx + 1), _bundle_));
//				System.exit(1);
//			}
//			
//			bytes += 4; /* length */
//			bytes += _bundle_; /* bundle */
//			
//			percent_ = (bytes * 100) / length;
//			if (percent_ == (_percent + 1)) {
//				System.out.print(String.format("Loading data file...%3d%%\r", percent_));
//				_percent = percent_;
//			}
//		}
//		f.close();
//		
//		System.out.println(String.format("%d bytes read (last buffer index is %d)", bytes, idx));
//		
//		/* Create query */
//		IPredicate [] andPredicates = new IPredicate [2];
//		
//		andPredicates[0] = new IntComparisonPredicate 
//				(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(5), new IntConstant(3));
//		
//		int comparisons = 500;
//		
//		IPredicate [] orPredicates = new IPredicate [comparisons];
//		int column = 8;
//		for (i = 0; i < comparisons - 1; i++) {
//			orPredicates[i] = new FloatComparisonPredicate 
//					(FloatComparisonPredicate.LESS_OP, new FloatColumnReference(column), new FloatConstant(0));
//			column ++;
//			if (column == 11) 
//				column = 8;
//		}
//		
//		orPredicates[comparisons - 1] = new FloatComparisonPredicate 
//				(FloatComparisonPredicate.NONLESS_OP, new FloatColumnReference(8), new FloatConstant(0));
//		
//		andPredicates[1] = new ORPredicate (orPredicates);
//		
//		IPredicate predicate = new ANDPredicate (andPredicates);
//		
//		/* GPU operator predicate */
//		StringBuilder customPredicate = new StringBuilder ();
//		
//		customPredicate.append("int value = 1;\n");
//		customPredicate.append("int attribute_value1 = __bswap32(p->tuple._5);\n");
//		customPredicate.append("float attribute_value2 = __bswapfp(p->tuple._8);\n");
//		customPredicate.append("value = value & (attribute_value1 == 3) & ");
//		customPredicate.append("(");
//		for (i = 0; i < comparisons; i++) {
//			if (i < 499)
//				customPredicate.append(String.format("(attribute_value2 < 0) | "));
//			else
//				customPredicate.append(String.format("(attribute_value2 >= 0)"));
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
//		query.setName("Query");
//		
//		Set<Query> queries = new HashSet<Query>();
//		queries.add(query);
//		
//		QueryApplication application = new QueryApplication(queries);
//		
//		application.setup();
//		
//		/* Push the input stream */
//		int ndx = 0;
//		int str = 0;
//		while (true) {
//			application.processData (data[ndx].array());
//			if ((str++ % stretch) == 0) {
//				ndx = ++ndx % numberOfTasks;
//			}
//			if (str == Integer.MAX_VALUE) {
//				break;
//			}
//		}
//		
//		System.out.println("Bye.");
	}
}
