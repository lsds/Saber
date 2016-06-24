package uk.ac.imperial.lsds.saber.cql.expressions.longs;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public interface LongExpression extends Expression {

	public long eval(IQueryBuffer buffer, ITupleSchema schema, int offset);
}
