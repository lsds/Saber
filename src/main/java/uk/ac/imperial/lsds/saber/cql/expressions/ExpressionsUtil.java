package uk.ac.imperial.lsds.saber.cql.expressions;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;

public class ExpressionsUtil {

	public static final byte [] intToByteArray (int value) {
		
		return new byte [] { 
			(byte) (value >>> 24), 
			(byte) (value >>> 16),
			(byte) (value >>>  8), 
			(byte) (value)
		};
	}

	public static void intToByteArray (int value, byte [] bytes) {
		
		intToByteArray(value, bytes, 0);
	}
	
	public static int intToByteArray (int value, byte [] bytes, int pivot) {
		
		bytes[pivot + 0] = (byte) (value >>> 24);
		bytes[pivot + 1] = (byte) (value >>> 16);
		bytes[pivot + 2] = (byte) (value >>>  8);
		bytes[pivot + 3] = (byte) (value);
		
		return (pivot + 4);
	}
	
	public static final byte [] floatToByteArray (float value) {
		
		int bits = Float.floatToIntBits(value);
		
		return new byte [] { 
			(byte) ((bits)       & 0xff), 
			(byte) ((bits >>  8) & 0xff),
			(byte) ((bits >> 16) & 0xff), 
			(byte) ((bits >> 24) & 0xff) 
		};
	}
	
	public static void floatToByteArray (float value, byte [] bytes) {
		
		floatToByteArray(value, bytes, 0);
	}
	
	public static int floatToByteArray (float value, byte [] bytes, int pivot) {
		
		int bits = Float.floatToIntBits(value);
		
		bytes[pivot + 0] = (byte) ((bits)       & 0xff);
		bytes[pivot + 1] = (byte) ((bits >>  8) & 0xff);
		bytes[pivot + 2] = (byte) ((bits >> 16) & 0xff);
		bytes[pivot + 3] = (byte) ((bits >> 24) & 0xff);
		
		return (pivot + 4);
	}
	
	public static final byte [] longToByteArray (long value) {
		
		byte [] b = new byte [8];
		
		for (int i = 0; i < 8; ++i) {
			
			b[i] = (byte) (value >> (8 - i - 1 << 3));
		}
		
		return b;
	}			
	
	public static void longToByteArray (long value, byte [] bytes) {
		
		longToByteArray(value, bytes, 0);
	}
	
	public static int longToByteArray (long value, byte [] bytes, int pivot) {
		
		for (int i = 0; i < 8; ++i) {
			
			bytes[pivot + i] = (byte) (value >> (8 - i - 1 << 3));
		}
		return (pivot + 8);
	}
	
	public static final byte [] longLongToByteArray (long msbValue, long lsbValue) {
		
		byte [] b = new byte [16];
		
		for (int i = 0; i < 8; ++i) {
			
			b[i] = (byte) (msbValue >> (8 - i - 1 << 3));
		}
		
		for (int i = 0; i < 8; ++i) {
			
			b[i + 8] = (byte) (lsbValue >> (8 - i - 1 << 3));
		}
		
		return b;
	}
	
	public static void longLongToByteArray (long msbValue, long lsbValue, byte [] bytes) {
		
		longLongToByteArray(msbValue, lsbValue, bytes, 0);
	}
	
	public static int longLongToByteArray (long msbValue, long lsbValue, byte [] bytes, int pivot) {
		
		for (int i = 0; i < 8; ++i) {
			
			bytes[pivot + i] = (byte) (msbValue >> (8 - i - 1 << 3));
		}
		
		for (int i = 0; i < 8; ++i) {
			
			bytes[pivot + i + 8] = (byte) (lsbValue >> (8 - i - 1 << 3));
		}
		
		return (pivot + 16);
	}
	
	public static final ITupleSchema getTupleSchemaFromExpressions (final Expression [] expressions) {
		
		ITupleSchema schema;
		
		int [] offsets = new int[expressions.length];
		
		int idx = 0;
		Expression e;
		for (int i = 0; i < expressions.length; i++) {
			
			offsets[i] = idx;
			
			e = expressions[i];
			
			     if (e instanceof      IntExpression) idx += 4;
			else if (e instanceof 	 FloatExpression) idx += 4;
			else if (e instanceof     LongExpression) idx += 8;
			else if (e instanceof LongLongExpression) idx += 16;
		}
		
		schema = new TupleSchema (offsets, idx);
		
		/* Set types */
		for (int i = 0; i < expressions.length; i++) {
			e = expressions[i];
			     if (e instanceof      IntExpression) schema.setAttributeType(i, PrimitiveType.INT      );
			else if (e instanceof    FloatExpression) schema.setAttributeType(i, PrimitiveType.FLOAT    );
			else if (e instanceof     LongExpression) schema.setAttributeType(i, PrimitiveType.LONG     );
			else if (e instanceof LongLongExpression) schema.setAttributeType(i, PrimitiveType.LONGLONG );
		}
		
		return schema;
	}

	public static final ITupleSchema mergeTupleSchemas (final ITupleSchema x, final ITupleSchema y) {

		int [] offsets = new int [x.numberOfAttributes() + y.numberOfAttributes()];
		
		for (int i = 0; i < x.numberOfAttributes(); ++i) 
			offsets[i] = x.getAttributeOffset(i);
		
		int last = x.getTupleSize() - x.getPadLength();
		
		for (int i = 0; i < y.numberOfAttributes(); ++i) 
			offsets[i + x.numberOfAttributes()] = last + y.getAttributeOffset(i);
		
		int tupleSize = 
			x.getTupleSize() - x.getPadLength() +
			y.getTupleSize() - y.getPadLength();

		ITupleSchema result = new TupleSchema(offsets, tupleSize);
		/* Set attribute types */
		
		int j = 0;
		
		for (int i = 0; i < x.numberOfAttributes(); ++i)
			result.setAttributeType (j++, x.getAttributeType(i));
		
		for (int i = 0; i < y.numberOfAttributes(); ++i)
			result.setAttributeType (j++, y.getAttributeType(i));
		
		return result;
	}
}
