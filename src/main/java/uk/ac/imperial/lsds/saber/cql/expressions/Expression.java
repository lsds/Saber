package uk.ac.imperial.lsds.saber.cql.expressions;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;

public interface Expression {

	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int    offset, IQueryBuffer dst);
	public void writeByteResult  (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset);
	
	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset);
	public void    evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte[] bytes);
	public int     evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte[] bytes, int pivot);
}
