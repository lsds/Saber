package uk.ac.imperial.lsds.saber.cql.predicates;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public interface IPredicate {
	
	public boolean satisfied (IQueryBuffer buffer, ITupleSchema schema, int offset);

	public boolean satisfied (
			IQueryBuffer buffer1, ITupleSchema schema1, int offset1, 
			IQueryBuffer buffer2, ITupleSchema schema2, int offset2);
	
	@Override
	public String toString();
	
	public int numberOfPredicates();
	
	public Expression getFirstExpression ();
	
	public Expression getSecondExpression ();		
}
