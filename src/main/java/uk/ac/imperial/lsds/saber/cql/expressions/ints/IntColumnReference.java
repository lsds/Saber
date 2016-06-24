package uk.ac.imperial.lsds.saber.cql.expressions.ints;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;

public class IntColumnReference implements IntExpression {

	private int column;
	
	private final int size = 4;

	public IntColumnReference (int column) {
		
		if (column < 0)
			throw new IllegalArgumentException("error: column index must be greater than 0");
		
		this.column = column;
	}
	
	public int getColumn () {
		return column;
	}
	
	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		s.append("\"").append(column).append("\"");
		return s.toString();
	}

	public int eval(IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		return buffer.getInt(offset + schema.getAttributeOffset(column));
	}
	
	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int offset, IQueryBuffer dst) {
		
		int value = eval(src, schema, offset);
		dst.putInt(value);
	}
	
	public void writeByteResult (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset) {
		
		int pos = src.normalise(srcOffset + schema.getAttributeOffset(column));
		System.arraycopy(src.array(), pos, dst.array(), dstOffset, size);
	}
	
	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		int value = eval(buffer, schema, offset);
		return ExpressionsUtil.intToByteArray(value);
	}
	
	public void evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte[] bytes) {
		
		evalAsByteArray (buffer, schema, offset, bytes, 0);
	}
	
	public int evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte[] bytes, int pivot) {
		
		int value = eval(buffer, schema, offset);
		return ExpressionsUtil.intToByteArray(value, bytes, pivot);
	}
}
