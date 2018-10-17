package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators;

import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;

public class SelectionCpuKernelGenerator {

	public static String getFunctor (IPredicate predicate, String customPredicate) {
		
		StringBuilder b = new StringBuilder ();

        b.append("\t \t \t if ( ");
		if (customPredicate != null) {
		    System.err.println("error: not supporting custom predicated yet");
        } else {
		    b.append(predicate.toString());
        }
        b.append(" )\n");

		
		return b.toString();
	}
}
