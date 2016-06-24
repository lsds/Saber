package uk.ac.imperial.lsds.saber.cql.expressions.floats;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public interface FloatExpression extends Expression {
	
	public float eval (IQueryBuffer buffer, ITupleSchema schema, int offset);	
}