package uk.ac.imperial.lsds.saber.cql.expressions.ints;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;

public class IntSubtraction implements IntExpression {

	private IntExpression [] expressions = null;

	public IntSubtraction (IntExpression... expressions) {
		this.expressions = expressions;
	}

	public IntSubtraction (IntExpression a, IntExpression b) {
		
		this.expressions = new IntExpression[2];
		this.expressions[0] = a;
		this.expressions[1] = b;
	}

	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		for (int i = 0; i < expressions.length; i++) {
			s.append("(").append(expressions[i]).append(")");
			if (i != expressions.length - 1)
				s.append(" - ");
		}
		return s.toString();
	}

	public int eval (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		int result = this.expressions[0].eval(buffer, schema, offset);
		for (int i = 1; i < expressions.length; i++) {
			result -= expressions[i].eval(buffer, schema, offset);
		}
		return result;
	}

	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int offset, IQueryBuffer dst) {
		
		int value = eval(src, schema, offset);
		dst.putInt(value);
	}

	public void writeByteResult (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset) {
		
		int value = eval(src, schema, srcOffset);
		dst.putInt(dstOffset, value);
	}

	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		int value = eval(buffer, schema, offset);
		return ExpressionsUtil.intToByteArray(value);
	}
	
	public void evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		
		evalAsByteArray(buffer, schema, offset, bytes, 0);
	}

	public int evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes, int pivot) {
		
		int value = eval(buffer, schema, offset);
		return ExpressionsUtil.intToByteArray(value, bytes, pivot);	
	}
}
