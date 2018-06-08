package uk.ac.imperial.lsds.saber.cql.expressions.longlongs;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public interface LongLongExpression extends Expression {

	public long evalMSB(IQueryBuffer buffer, ITupleSchema schema, int offset);
	public long evalLSB(IQueryBuffer buffer, ITupleSchema schema, int offset);
}
