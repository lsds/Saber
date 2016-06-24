package uk.ac.imperial.lsds.saber.cql.expressions.floats;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;

public class FloatMultiplication implements FloatExpression {

	private FloatExpression [] expressions = null;

	public FloatMultiplication (FloatExpression... expressions) {
		this.expressions = expressions;
	}

	public FloatMultiplication(FloatExpression a, FloatExpression b) {
		
		this.expressions = new FloatExpression[2];
		this.expressions[0] = a;
		this.expressions[1] = b;
	}

	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		for (int i = 0; i < expressions.length; i++) {
			s.append("(").append(expressions[i]).append(")");
			if (i != expressions.length - 1)
				s.append(" x ");
		}
		return s.toString();
	}
	
	public float eval (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		float result = this.expressions[0].eval(buffer, schema, offset);
		for (int i = 1; i < expressions.length; i++) {
			result *= expressions[i].eval(buffer, schema, offset);
		}
		return result;
	}
	
	public void appendByteResult (IQueryBuffer src, ITupleSchema schema, int offset, IQueryBuffer dst) {
		
		float value = eval(src, schema, offset);
		dst.putFloat(value);
	}

	public void writeByteResult (IQueryBuffer src, ITupleSchema schema, int srcOffset, IQueryBuffer dst, int dstOffset) {
		
		float value = eval(src, schema, srcOffset);
		dst.putFloat(dstOffset, value);
	}

	public byte [] evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		float value = eval(buffer, schema, offset);
		return ExpressionsUtil.floatToByteArray(value);
	}

	public void evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		
		evalAsByteArray(buffer, schema, offset, bytes, 0);
	}

	public int evalAsByteArray (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes, int pivot) {
		
		float value = eval(buffer, schema, offset);
		return ExpressionsUtil.floatToByteArray(value, bytes, pivot);
	}
}
