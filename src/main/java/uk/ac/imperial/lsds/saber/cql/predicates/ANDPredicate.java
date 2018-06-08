package uk.ac.imperial.lsds.saber.cql.predicates;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public class ANDPredicate implements IPredicate {
	
	IPredicate [] predicates;

	public ANDPredicate (IPredicate... predicates) {
		this.predicates = predicates;
	}
	
	public boolean satisfied (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		for (IPredicate p: predicates)
			if (! p.satisfied(buffer, schema, offset))
				return false;
		return true;
	}
	
	@Override
	public String toString() {
		
		StringBuilder s = new StringBuilder();
		
		for (int i = 0; i < predicates.length; ++i) {
			
			s.append("(").append(predicates[i]).append(")");
			
			if (i != predicates.length - 1)
				s.append(" AND ");
		}
		return s.toString();
	}

	public boolean satisfied(
			IQueryBuffer buffer1, ITupleSchema schema1, int offset1,
			IQueryBuffer buffer2, ITupleSchema schema2, int offset2) {
		
		for (IPredicate p: predicates)
			if (! p.satisfied(buffer1, schema1, offset1, buffer2, schema2, offset2))
				return false;
		return true;
	}
	
	public int numberOfPredicates () {
		return predicates.length;
	}
	
	public Expression getFirstExpression () {
		return null;
	}
	
	public Expression getSecondExpression () {
		return null;
	}
}