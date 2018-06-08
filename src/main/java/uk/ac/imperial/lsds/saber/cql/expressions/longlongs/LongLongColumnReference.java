package uk.ac.imperial.lsds.saber.cql.expressions.longlongs;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;

public class LongLongColumnReference implements LongLongExpression {

	private int column;
	
	private final int size = 16;

	public LongLongColumnReference (int column) {
		
		if (column < 0)
			throw new IllegalArgumentException("error: column index must be greater than 0");
		
		this.column = column;
	}
	
	public int getColumn () {
		return column;
	}
	
	@Override
	public String toString() {
		final StringBuilder s = new StringBuilder();
		s.append("\"").append(column).append("\"");
		return s.toString();
	}
	
	public long evalMSB (IQueryBuffer buffer, ITupleSchema schema, int offset) {
				
		return 	buffer.getLong(offset + schema.getAttributeOffset(column));
	}
	
	public long evalLSB (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		return 	buffer.getLong(offset + schema.getAttributeOffset(column) + 8);
	}

	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int offset, IQueryBuffer dst) {
		
		long msbValue = evalMSB(src, schema, offset);
		long lsbValue = evalLSB(src, schema, offset);
		dst.putLongLong(msbValue, lsbValue);
	}
	
	public void writeByteResult (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset) {
		
		int pos = src.normalise(srcOffset + schema.getAttributeOffset(column));
		System.arraycopy(src.array(), pos, dst.array(), dstOffset, size);
	}

	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		long msbValue = evalMSB(buffer, schema, offset);
		long lsbValue = evalLSB(buffer, schema, offset);
		return ExpressionsUtil.longLongToByteArray(msbValue, lsbValue);
	}

	public void evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		
		long msbValue = evalMSB(buffer, schema, offset);
		long lsbValue = evalLSB(buffer, schema, offset);
		ExpressionsUtil.longLongToByteArray(msbValue, lsbValue, bytes);
	}

	public int evalAsByteArray(IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes, int pivot) {
		
		long msbValue = evalMSB(buffer, schema, offset);
		long lsbValue = evalLSB(buffer, schema, offset);
		return ExpressionsUtil.longLongToByteArray(msbValue, lsbValue, bytes, pivot);
	}
}
