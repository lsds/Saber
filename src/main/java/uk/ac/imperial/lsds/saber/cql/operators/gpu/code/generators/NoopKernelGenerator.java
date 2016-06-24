package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public class NoopKernelGenerator {

	public static String getFunctor (ITupleSchema schema) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("inline void copyf (__global input_t *p, __global output_t *q) {\n");
		int [] bytes1 = new int [1];  
		int v = KernelGenerator.getVectorSize(schema.getTupleSize(), bytes1);
		for (int i = 0; i < v; i++) {
			b.append(String.format("\tq->vectors[%d] = p->vectors[%d];\n", i, i));
		}
		b.append("}\n");
		b.append("\n");
		
		return b.toString();
	}
}
