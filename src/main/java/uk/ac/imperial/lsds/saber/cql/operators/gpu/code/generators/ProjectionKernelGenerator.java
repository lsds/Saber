package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public class ProjectionKernelGenerator {

	public static String getFunctor(ITupleSchema input, ITupleSchema output, int depth) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("inline void projectf (__local input_t *p, __local output_t *q) {\n");
		/* Copy timestamp */
		b.append("\tq->tuple.t = p->tuple.t;\n");
		int idx = 0;
		for (int i = 1; i < output.numberOfAttributes(); ++i) {
			
			idx ++;
			if (idx >= input.numberOfAttributes())
				idx = 1;
			
			if (input.getAttributeType(idx) == PrimitiveType.FLOAT && depth > 0) {
				/* Floating point expression */
				StringBuilder fp = new StringBuilder ();
				fp.append(String.format("q->tuple._%d = __bswapfp(", idx));
				
				for (int k = 0; k < depth - 1; k++)
					fp.append("3. * ");
				
				fp.append(String.format("(3. * __bswapfp(p->tuple._%d) / 2.)", idx));
				
				for (int k = 0; k < depth - 1; k++)
					fp.append(" / 2.");
				
				fp.append(");\n");
				
				b.append(String.format("\t%s", fp.toString()));
				
			} else {
				
				b.append(String.format("\tq->tuple._%d = p->tuple._%d;\n", i, idx));
			}
		}
		b.append("}\n");
		
		return b.toString();
	}
}
