package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class SelectionKernelGenerator {

	public static Object getFunctor (IPredicate predicate, String customPredicate) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("inline int selectf (__global input_t *p) {\n");
		
		if (customPredicate != null) {
			
			b.append(String.format("\t%s\n", customPredicate));
			
		} else {
			
			b.append("\tint value = 1;\n");
			b.append("\tint attribute_value = __bswap32(p->tuple._1);\n");
			b.append("\tvalue = value & ");
			for (int i = 0; i < predicate.numberOfPredicates(); i++) {
				if (i == predicate.numberOfPredicates() - 1)
					b.append("(attribute_value != 0); \n");
				else
					b.append(String.format("(attribute_value != %d) & ", i - predicate.numberOfPredicates() - 1));
			}
			b.append("\treturn value;\n");
		}
		
		b.append("}\n");
		
		return b.toString();
	}
}
