package uk.ac.imperial.lsds.saber.cql.expressions.floats;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;

public class FloatColumnReference implements FloatExpression {

	private int column;
	
	private final int size = 4;

	public FloatColumnReference (int column) {
		
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

	public float eval (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		return buffer.getFloat(offset + schema.getAttributeOffset(column));
	}
	
	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int offset, IQueryBuffer dst) {
		
		float value = eval(src, schema, offset);
		dst.putFloat(value);
	}
	
	public void writeByteResult (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset) {
		
		int pos = src.normalise(srcOffset + schema.getAttributeOffset(column));
		System.arraycopy(src.array(), pos, dst.array(), dstOffset, size);
	}

	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		float value = eval(buffer, schema, offset);
		return ExpressionsUtil.floatToByteArray(value);
	}
	
	public void evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		
		evalAsByteArray (buffer, schema, offset, bytes, 0);
	}
	
	public int evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes, int pivot) {
		
		float value = eval(buffer, schema, offset);
		return ExpressionsUtil.floatToByteArray(value, bytes, pivot);
	}
}
