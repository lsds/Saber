package uk.ac.imperial.lsds.saber.cql.predicates;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;

public class LongComparisonPredicate implements IPredicate {
	
	/* Codes of available comparison operator */
	public static final int      EQUAL_OP = 0;
	public static final int   NONEQUAL_OP = 1;
	public static final int       LESS_OP = 2;
	public static final int    NONLESS_OP = 3;
	public static final int    GREATER_OP = 4;
	public static final int NONGREATER_OP = 5;

	/* Code of the comparison operator */
	int op;
	
	/* Values compared by this predicate */
	LongExpression v1;
	LongExpression v2;
	
	public LongComparisonPredicate (LongExpression v1, LongExpression v2) {
		this(EQUAL_OP, v1, v2);
	}
	
	public LongComparisonPredicate (int op, LongExpression v1, LongExpression v2) {
		this.op = op;
		this.v1 = v1;
		this.v2 = v2;		
	}
	
	public String getComparisonOperator () {
		return getComparisonString();
	}

	public int getOperator (boolean inverse) {
		int result = 0;
		if (inverse) {
			switch (op) {
			case   NONEQUAL_OP: result =      EQUAL_OP; break;
			case       LESS_OP: result =    GREATER_OP; break;
			case    NONLESS_OP: result = NONGREATER_OP; break;
			case    GREATER_OP: result =       LESS_OP; break;
			case NONGREATER_OP: result =    NONLESS_OP; break;
			default:            result =   NONEQUAL_OP;
			}
		} else {
			result = op;
		}
		return result;
	}
	
	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		s.append(v1.toString()).append(getComparisonString()).append(v2.toString());
		return s.toString();
	}

	private String getComparisonString () {
		String result = null;
		switch (op) {	
		case   NONEQUAL_OP: result = " != "; break;
		case       LESS_OP: result =  " < ";  break;
		case    NONLESS_OP: result = " >= "; break;
		case    GREATER_OP: result =  " > ";  break;
		case NONGREATER_OP: result = " <= "; break;
		default:            result =  " = ";
		}
		return result;
	}
	
	private boolean compare (long val1, long val2) {
		switch (op) {
		case      EQUAL_OP: return (val1 == val2);
		case   NONEQUAL_OP: return (val1 != val2);
		case       LESS_OP: return (val1 < val2); 
		case    NONLESS_OP: return (val1 > val2);
		case    GREATER_OP: return (val1 > val2);
		case NONGREATER_OP: return (val1 <= val2);
		default:
			throw new RuntimeException("error: invalid comparison predicate");
		}
	}
	
	public boolean satisfied (IQueryBuffer buffer, ITupleSchema schema, int offset) {
		
		long val1 = v1.eval(buffer, schema, offset);
		long val2 = v2.eval(buffer, schema, offset);

		return compare (val1, val2);
	}
	
	public boolean satisfied (
			IQueryBuffer buffer1, ITupleSchema schema1, int offset1,
			IQueryBuffer buffer2, ITupleSchema schema2, int offset2) {
				
		long val1 = v1.eval(buffer1, schema1, offset1);
		long val2 = v2.eval(buffer2, schema2, offset2);
		
		return compare (val1, val2);
	}
	
	public int numberOfPredicates () {
		return 1;
	}
		
	public Expression getFirstExpression() {		
		return this.v1;
	}
	
	public Expression getSecondExpression() {	
		return this.v2;
	}
}
