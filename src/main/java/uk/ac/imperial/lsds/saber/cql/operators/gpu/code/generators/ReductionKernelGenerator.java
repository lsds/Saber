package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;

public class ReductionKernelGenerator {

	public static Object getFunctor (ITupleSchema output, AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes) {
		
		StringBuilder b = new StringBuilder ();
		
		int [] bytes1 = new int [1]; 
		int numberOfVectors = KernelGenerator.getVectorSize(output.getTupleSize(), bytes1);
		int i;
		
		/* initf */
		b.append("inline void initf (__local output_t *p) {\n");
		
		/* Set timestamp */
		b.append ("\tp->tuple.t = 0;\n");
		for (i = 0; i < aggregationTypes.length; ++i) {
			
			switch (aggregationTypes[i]) {
			case CNT:
			case SUM:
			case AVG: b.append (String.format("\tp->tuple._%d = %s;\n", (i + 1),       "0")); break;
			case MIN: b.append (String.format("\tp->tuple._%d = %s;\n", (i + 1), "FLT_MIN")); break;
			case MAX: b.append (String.format("\tp->tuple._%d = %s;\n", (i + 1), "FLT_MAX")); break;
			default:
				throw new IllegalArgumentException("error: invalid aggregation type");
			}
		}
		/* Set count to zero */
		b.append (String.format("\tp->tuple._%d = 0;\n", (i + 1)));
		b.append ("}\n");
		
		b.append("\n");
		
		/* reducef */
		b.append("inline void reducef (__local output_t *p, __global input_t *q) {\n");
		
		/* Set timestamp */
		b.append ("\tp->tuple.t = (p->tuple.t == 0) ? : q->tuple.t;\n");
		for (i = 0; i < aggregationTypes.length; ++i) {
			
			int column = aggregationAttributes[i].getColumn();
			
			switch (aggregationTypes[i]) {
			case CNT:
				b.append (String.format("\tp->tuple._%d += 1;\n", (i + 1)));
				break;
			case SUM:
			case AVG:
				b.append (String.format("\tp->tuple._%d += __bswapfp(q->tuple._%d);\n", (i + 1), column));
				break;
			case MIN:
				b.append (String.format("\tp->tuple._%d = (p->tuple._%d > __bswapfp(q->tuple._%d)) ? __bswapfp(q->tuple._%d) : p->tuple._%d;\n", 
						(i + 1), (i + 1), column, column, (i + 1)));
				break;
			case MAX:
				b.append (String.format("\tp->tuple._%d = (p->tuple._%d < __bswapfp(q->tuple._%d)) ? __bswapfp(q->tuple._%d) : p->tuple._%d;\n", 
						(i + 1), (i + 1), column, column, (i + 1)));
				break;
			default:
				throw new IllegalArgumentException("error: invalid aggregation type");
			}
		}
		/* Set count to zero */
		b.append (String.format("\tp->tuple._%d += 1;\n", (i + 1)));
		b.append ("}\n");
		
		b.append("\n");
		
		/* cachef */
		b.append("inline void cachef (__local output_t *p, __local output_t *q) {\n");
		
		for (i = 0; i < numberOfVectors; i++)
			b.append(String.format("\tq->vectors[%d] = p->vectors[%d];\n", i, i));
		
		b.append ("}\n");
		
		b.append("\n");
		
		/* mergef */
		b.append("inline void mergef (__local output_t *p, __local output_t *q) {\n");
		
		for (i = 0; i < aggregationTypes.length; ++i) {
			
			switch (aggregationTypes[i]) {
			case CNT:
			case SUM:
			case AVG:
				b.append (String.format("\tp->tuple._%d += q->tuple._%d;\n", (i + 1), (i + 1)));
				break;
			case MIN:
				b.append (String.format("\tp->tuple._%d = (p->tuple._%d > q->tuple._%d) ? q->tuple._%d : p->tuple._%d;\n", 
						(i + 1), (i + 1), (i + 1), (i + 1), (i + 1)));
				break;
			case MAX:
				b.append (String.format("\tp->tuple._%d = (p->tuple._%d < q->tuple._%d) ? q->tuple._%d : p->tuple._%d;\n", 
						(i + 1), (i + 1), (i + 1), (i + 1), (i + 1)));
				break;
			default:
				throw new IllegalArgumentException("error: invalid aggregation type");
			}
		}
		b.append (String.format("\tp->tuple._%d += q->tuple._%d;\n", (i + 1), (i + 1)));
		b.append ("}\n");
		
		b.append("\n");
		
		/* copyf */
		b.append("inline void copyf (__local output_t *p, __global output_t *q) {\n");
		
		/* Compute average */
		boolean containsAverage = false;
		for (i = 0; i < aggregationTypes.length; ++i)
			if (aggregationTypes[i] == AggregationType.AVG)
				containsAverage = true;
		
		if (containsAverage) {
			
			int countAttribute = aggregationTypes.length + 1;
			b.append (String.format("\tint count = p->tuple._%d;\n", countAttribute));
			
			for (i = 0; i < aggregationTypes.length; ++i)
				if (aggregationTypes[i] == AggregationType.AVG)
					b.append (String.format("\tp->tuple._%d = p->tuple._%d / (float) count;\n", (i + 1), (i + 1)));
		}
		
		for (i = 0; i < numberOfVectors; i++)
			b.append(String.format("\tq->vectors[%d] = p->vectors[%d];\n", i, i));
		
		b.append ("}\n");
		
		b.append("\n");
		
		return b.toString();
	}
}
