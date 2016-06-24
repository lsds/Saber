package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class ThetaJoinKernelGenerator {

	public static String getFunctor(ITupleSchema input1, ITupleSchema input2, ITupleSchema output, 
		
		IPredicate predicate, String customPredicate, String customCopy) {
		
		StringBuilder b = new StringBuilder ();
		
		int [] bytes1 = new int [1]; 
		int [] bytes2 = new int [1]; 
		
		int v1 = KernelGenerator.getVectorSize(input1.getTupleSize(), bytes1);
		int v2 = KernelGenerator.getVectorSize(input2.getTupleSize(), bytes2);
		
		b.append("inline int selectf (__global s1_input_t *p1, __global s2_input_t *p2) {\n");
		
		if (customPredicate != null) {
			
			b.append(String.format("\t%s\n", customPredicate));
			
		} else {
			
			b.append("\tint value = 1;\n");
			b.append("\tvalue = value & ");
			
			for (int i = 0; i < predicate.numberOfPredicates(); i++) {
				
				if (i == predicate.numberOfPredicates() - 1)
					b.append("(__bswap32(p1->tuple._1) < __bswap32(p2->tuple._1));\n");
				else
					b.append(String.format("(__bswap32(p1->tuple._1) < __bswap32(p2->tuple._1)) & "));
			}
			
			b.append("\treturn value;\n");
		}
		
		b.append("}\n");
		
		b.append("\n");
		
		b.append("inline void copyf (__global s1_input_t *p1, __global s2_input_t *p2, __global output_t *q) {\n");
		
		int j = 0;
		
		if (customCopy != null) {
			
			b.append(String.format("%s\n", customCopy));
			
		} else {
			for (int i = 0; i < v1; i++)
				b.append(String.format("\tq->vectors[%d] = p1->vectors[%d];\n", j++, i));
		
			for (int i = 0; i < v2; i++)
				b.append(String.format("\tq->vectors[%d] = p2->vectors[%d];\n", j++, i));
		}
		
		b.append("}\n");
		b.append("\n");
		
		return b.toString();
	}
}
