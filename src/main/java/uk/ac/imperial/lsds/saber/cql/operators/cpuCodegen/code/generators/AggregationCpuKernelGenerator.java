package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.cql.expressions.ColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators.KernelGenerator;

import static uk.ac.imperial.lsds.saber.cql.operators.AggregationType.MAX;
import static uk.ac.imperial.lsds.saber.cql.operators.AggregationType.MIN;

public class AggregationCpuKernelGenerator {

	public static String getIntermediateTupleDefinition (Expression [] groupByAttributes,
		
		int numberOfValues, int intermediateTupleSize) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("typedef struct {\n");
		b.append("\tint mark;\n");
		b.append("\tint pad0;\n"); /* For alignment purposes (64-bit) */
		
		int bytes = 16;
		
		/* The first attribute is always a timestamp */
		b.append("\tlong t;\n");
		/* Store the key */
		for (int i = 0; i < groupByAttributes.length; ++i) {
			String s = "";
			if (groupByAttributes[i] instanceof   IntExpression) 
				{ s = String.format("\tint key_%d;\n",   (i + 1)); bytes += 4; }
			else 
			if (groupByAttributes[i] instanceof FloatExpression) 
				{ s = String.format("\tfloat key_%d;\n", (i + 1)); bytes += 4; }
			else 
			if (groupByAttributes[i] instanceof  LongExpression) 
				{ s = String.format("\tlong key_%d;\n",  (i + 1)); bytes += 8; }
			b.append(s);
		}
		/* Store value(s) */
		for (int i = 0; i < numberOfValues; ++i) {
			b.append(String.format("\tfloat value%d;\n", (i + 1)));
		}
		
		bytes += 4 * numberOfValues;
		
		b.append("\tint count;\n");
		
		bytes += 4;
		
		int pad = intermediateTupleSize - bytes;
		if (pad > 0)
			b.append(String.format("\tuchar pad[%d];\n", pad));
		
		b.append("} intermediate_tuple_t __attribute__((aligned(1)));\n");
		
		b.append("\n");
		
		int [] bytes1 = new int [1]; 
		int numberOfVectors = KernelGenerator.getVectorSize(intermediateTupleSize, bytes1);
		
		b.append("typedef union {\n");
		b.append("\tintermediate_tuple_t tuple;\n");
		b.append(String.format("\tuchar%d vectors[%d];\n", bytes1[0], numberOfVectors));
		b.append("} intermediate_t;\n");
		
		b.append("\n");
		
		/* Key definition */
		
		b.append("typedef struct {\n");
		
		for (int i = 0; i < groupByAttributes.length; ++i) {
			String s = "";
			if (groupByAttributes[i] instanceof   IntExpression) s = String.format("\tint key_%d;\n",   (i + 1));
			else 
			if (groupByAttributes[i] instanceof FloatExpression) s = String.format("\tfloat key_%d;\n", (i + 1));
			else
			if (groupByAttributes[i] instanceof  LongExpression) s = String.format("\tlong key_%d;\n",  (i + 1));
			b.append(s);
		}
		b.append("} key_t __attribute__((aligned(1)));\n");
		
		return b.toString();
	}
	
	public static String getFunctor (ITupleSchema output, 
			
		AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, Expression [] groupByAttributes,
		
		int intermediateTupleSize) {
		
		int [] bytes1 = new int [1]; 
		int numberOfVectors = KernelGenerator.getVectorSize(intermediateTupleSize, bytes1);
		
		StringBuilder b = new StringBuilder ();
		
		/* clearf */
		b.append("inline void clearf (__global intermediate_t *p) {\n");
		
		for (int i = 0; i < numberOfVectors; i++)
			b.append(String.format("\tp->vectors[%d] = 0;\n", i));
		
		b.append ("}\n");
		
		b.append("\n");
		
		/* Pack key */
		
		b.append("inline void pack_key (__local key_t *q, __global input_t *p) {\n");
		
		for (int i = 0; i < groupByAttributes.length; i++) {
			
			if (groupByAttributes[i] instanceof IntExpression) {
				
				IntColumnReference ref = (IntColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->key_%d = __bswap32(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
				
			} else
			if (groupByAttributes[i] instanceof FloatExpression) {
				
				FloatColumnReference ref = (FloatColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->key_%d = __bswapfp(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
				
			} else
			if (groupByAttributes[i] instanceof LongExpression) {
				
				LongColumnReference ref = (LongColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->key_%d = __bswap64(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
			}
		}
		
		b.append ("}\n");
		
		/* storef */
		
		b.append("inline void storef (__global intermediate_t *q, __global input_t *p) {\n");
		
		/* Store the timestamp */
		b.append("\tq->tuple.t = __bswap64(p->tuple.t);\n");
		
		/* Store the (composite) key */
		
		for (int i = 0; i < groupByAttributes.length; i++) {
			
			if (groupByAttributes[i] instanceof IntExpression) {
				
				IntColumnReference ref = (IntColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->tuple.key_%d = __bswap32(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->tuple.key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
				
			} else
			if (groupByAttributes[i] instanceof FloatExpression) {
				
				FloatColumnReference ref = (FloatColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->tuple.key_%d = __bswapfp(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->tuple.key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
				
			} else
			if (groupByAttributes[i] instanceof LongExpression) {
				
				LongColumnReference ref = (LongColumnReference) groupByAttributes[i];
				// b.append(String.format("\tq->tuple.key_%d = __bswap64(p->tuple._%d);\n", (i + 1), ref.getColumn()));
				b.append(String.format("\tq->tuple.key_%d = p->tuple._%d;\n", (i + 1), ref.getColumn()));
			}
		}
		
		for (int i = 0; i < aggregationTypes.length; ++i) {
			b.append(String.format("\tq->tuple.value%d = __bswapfp(p->tuple._%d);\n", (i + 1), aggregationAttributes[i].getColumn()));
		}
		b.append("\tq->tuple.count = 1;\n");
		
		b.append ("}\n");
		
		b.append("\n");
		
		/* comparef */
		
		b.append("inline int comparef (__local key_t *q, __global input_t *p) {\n");
		
		b.append("\tint value = 1;\n");
		b.append("\tvalue = value & ");
		
		for (int i = 0; i < groupByAttributes.length; i++) {
			
			if (groupByAttributes[i] instanceof IntExpression) {
				
				IntColumnReference ref = (IntColumnReference) groupByAttributes[i];
				// b.append(String.format("(q->key_%d == __bswap32(p->tuple._%d))", (i + 1), ref.getColumn()));
				b.append(String.format("(q->key_%d == p->tuple._%d)", (i + 1), ref.getColumn()));
				if (i < (groupByAttributes.length - 1))
					b.append(" & ");
				else
					b.append(";");
				
			} else
			if (groupByAttributes[i] instanceof FloatExpression) {
				
				FloatColumnReference ref = (FloatColumnReference) groupByAttributes[i];
				// b.append(String.format("(q->key_%d == __bswapfp(p->tuple._%d))", (i + 1), ref.getColumn()));
				b.append(String.format("(q->key_%d == p->tuple._%d)", (i + 1), ref.getColumn()));
				if (i < (groupByAttributes.length - 1))
					b.append(" & ");
				else
					b.append(";");
				
			} else
			if (groupByAttributes[i] instanceof LongExpression) {
				
				LongColumnReference ref = (LongColumnReference) groupByAttributes[i];
				// b.append(String.format("(q->key_%d == __bswap64(p->tuple._%d))", (i + 1), ref.getColumn()));
				b.append(String.format("(q->key_%d == p->tuple._%d)", (i + 1), ref.getColumn()));
				if (i < (groupByAttributes.length - 1))
					b.append(" & ");
				else
					b.append(";");
			}
		}
		b.append("\n");
		b.append("\treturn value;\n");
		b.append ("}\n");
		
		b.append("\n");
		
		/* updatef */
		
		b.append("inline void updatef (__global intermediate_t *out, __global input_t *p) {\n");
		
		for (int i = 0; i < aggregationTypes.length; ++i) {
			
			switch (aggregationTypes[i]) {
			case CNT:
				b.append(String.format("\tatomic_inc ((global int *) &(out->tuple.value%d));\n", (i + 1)));
				break;
			case SUM:
			case AVG: 
				b.append (String.format("\tatomic_add ((global int *) &(out->tuple.value%d), convert_int_rtp(__bswapfp(p->tuple._%d)));\n",
					(i + 1), aggregationAttributes[i].getColumn()));
				break;
			case MIN:
				b.append (String.format("\tatomic_min ((global int *) &(out->tuple.value%d), convert_int_rtp(__bswapfp(p->tuple._%d)));\n",
					(i + 1), aggregationAttributes[i].getColumn()));
				break;
			case MAX:
				b.append (String.format("\tatomic_max ((global int *) &(out->tuple.value%d), convert_int_rtp(__bswapfp(p->tuple._%d)));\n",
					(i + 1), aggregationAttributes[i].getColumn()));
				break;
			default:
				throw new IllegalArgumentException("error: invalid aggregation type");
			}
		}
		
		b.append ("\tatomic_inc ((global int *) &(out->tuple.count));\n");
		
		b.append ("}\n");
		
		b.append("\n");
		
		return b.toString();
	}

	public static String getTemplateDefinition (AggregationType[] aggregationTypes, Expression[] groupByAttributes, int numberOfValues, boolean isHashTableDefinition) {
        StringBuilder b = new StringBuilder ();

        if (groupByAttributes.length > 1 || numberOfValues > 1)
            throw new UnsupportedOperationException("error: only single key-value pair is supported now");

        b.append("<");
        /* Store the key */
        for (int i = 0; i < groupByAttributes.length; ++i) {
            String s = "";
            if (groupByAttributes[i] instanceof   IntExpression)
                s = "int,";
            else
            if (groupByAttributes[i] instanceof FloatExpression)
                s = "float,";
            else
            if (groupByAttributes[i] instanceof  LongExpression)
                s = "long,";
            b.append(s);
        }
        /* Store value(s) */
        for (int i = 0; i < numberOfValues; ++i) {
            b.append(" float");
        }

        if (isHashTableDefinition) {
            for (int i = 0; i < aggregationTypes.length; i++) {
                if (aggregationTypes[i] == MIN) {
                    b.append(", MIN");
                    break;
                }
                if (aggregationTypes[i] == MAX) {
                    b.append(", MAX");
                    break;
                }
            }
        }

        b.append(">");
        return b.toString();
    }

    public static String getAggregationVariables (Expression[] groupByAttributes, String templateTypes, String templateTypesForHashTable) {
        StringBuilder b = new StringBuilder ();
        // initialise hashtable
        if (groupByAttributes!=null) {
            b.append("\thashtable" +templateTypesForHashTable+ "map; \n");
            b.append("\tht_node" +templateTypes+ " *hTable = map.getTable(); \n");
            b.append("\tconst int ht_node_size = sizeof(ht_node"+templateTypes+"); \n");
        }

        return b.toString();
    }

    public static String getWriteCompleteWindowsBlock (AggregationType[] aggregationGroupByTypes, Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        if (groupByAttributes==null) {
            throw new UnsupportedOperationException("error: it has to write a hashtable in the result...");
        }
        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        b.append(tabs+"completeWindowsResults[completeWindowsPointer].timestamp = hTable[i].timestamp; \n");
        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._1 = hTable[i].key; \n");
        if (aggregationGroupByTypes.length > 0) {
            for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                switch (aggregationGroupByTypes[i]) {
                    case SUM:
                        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._"+(i+2)+" = hTable[i].value; \n");
                        break;
                    case AVG:
                        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._"+(i+2)+" = hTable[i].value/hTable[i].counter; \n");
                        break;
                    case CNT:
                        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._"+(i+2)+" = hTable[i].counter; \n");
                        break;
                    case MIN:
                        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._"+(i+2)+" = hTable[i].value; \n");
                        break;
                    case MAX:
                        b.append(tabs+"completeWindowsResults[completeWindowsPointer]._"+(i+2)+" = hTable[i].value; \n");
                        break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        b.append(tabs+"completeWindowsPointer += hTable[i].status; \n");
        return b.toString();
	}

    public static String getWriteCompleteWindowsBlockForMerge(AggregationType[] aggregationGroupByTypes, Expression[] groupByAttributes, int bufferId,
                                                              int numberOfTabs, boolean isFound) {
        StringBuilder b = new StringBuilder ();

        if (groupByAttributes==null) {
            throw new UnsupportedOperationException("error: it has to write a hashtable in the result...");
        }
        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (bufferId == 1) {
            b.append(tabs+"/* Put timestamp */\n");
            b.append(tabs+"completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp; \n");
            b.append(tabs+"/* Put key */\n");
            b.append(tabs+"completeWindowsResults[resultIndex]._1 = buffer1[idx].key; \n");
            b.append(tabs+"/* Put value(s) */\n");
            if (aggregationGroupByTypes.length > 0) {
                for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                    switch (aggregationGroupByTypes[i]) {
                        case SUM:
                            if (!isFound)
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].value; \n");
                            else
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].value + buffer2[posInB2].value; \n");
                            break;
                        case AVG:
                            if (!isFound)
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].value/buffer1[idx].counter; \n");
                            else
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = " +
                                        "(buffer1[idx].value + buffer2[posInB2].value)/((buffer1[idx].counter + buffer2[posInB2].counter)); \n");
                            break;
                        case CNT:
                            if (!isFound)
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].counter; \n");
                            else
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].counter + buffer2[posInB2].counter; \n");
                            break;
                        case MIN:
                            if (!isFound)
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].value; \n");
                            else
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = (buffer1[idx].value < buffer2[posInB2].value) ? buffer1[idx].value : buffer2[posInB2].value; \n");
                            break;
                        case MAX:
                            if (!isFound)
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = buffer1[idx].value; \n");
                            else
                                b.append(tabs+"completeWindowsResults[resultIndex]._"+(i+2)+" = (buffer1[idx].value > buffer2[posInB2].value) ? buffer1[idx].value : buffer2[posInB2].value; \n");
                            break;
                        default:
                            throw new IllegalArgumentException("error: invalid aggregation type");
                    }
                }
            }
        } else if (bufferId == 2) {
            b.append(tabs+"/* Put timestamp */\n");
            b.append(tabs+"completeWindowsResults[resultIndex].timestamp = buffer2[idx].timestamp; \n");
            b.append(tabs+"/* Put key */\n");
            b.append(tabs+"completeWindowsResults[resultIndex]._1 = buffer2[idx].key; \n");
            b.append(tabs+"/* Put value(s) */\n");
            if (aggregationGroupByTypes.length > 0) {
                for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                    switch (aggregationGroupByTypes[i]) {
                        case SUM:
                            b.append(tabs + "completeWindowsResults[resultIndex]._" + (i + 2) + " = buffer2[idx].value; \n");
                            break;
                        case AVG:
                            b.append(tabs + "completeWindowsResults[resultIndex]._" + (i + 2) + " = buffer2[idx].value/buffer2[idx].counter; \n");
                            break;
                        case CNT:
                            b.append(tabs + "completeWindowsResults[resultIndex]._" + (i + 2) + " = buffer2[idx].counter; \n");
                            break;
                        case MIN:
                            b.append(tabs + "completeWindowsResults[resultIndex]._" + (i + 2) + " = buffer2[idx].value; \n");
                            break;
                        case MAX:
                            b.append(tabs + "completeWindowsResults[resultIndex]._" + (i + 2) + " = buffer2[idx].value; \n");
                            break;
                        default:
                            throw new IllegalArgumentException("error: invalid aggregation type");
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException("error: wrong bufferId");
        }
        b.append(tabs+"resultIndex ++; \n");
        return b.toString();
    }

    public static String getWriteOpeningWindowsBlockForMerge(AggregationType[] aggregationGroupByTypes, Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        if (groupByAttributes==null) {
            throw new UnsupportedOperationException("error: it has to write a hashtable in the result...");
        }
        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        b.append(tabs+"/* Mark occupancy */ \n");
        b.append(tabs+"openingWindowsResults[posInRes + resultIndex].status = 1; \n");
        b.append(tabs+"/* Put timestamp */ \n");
        b.append(tabs+"openingWindowsResults[posInRes + resultIndex].timestamp = buffer2[idx].timestamp; \n");
        b.append(tabs+"/* Put key and TODO: value(s) */ \n");
        b.append(tabs+"openingWindowsResults[posInRes + resultIndex].key = buffer2[idx].key; \n");

        if (aggregationGroupByTypes.length > 0) {
            for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                switch (aggregationGroupByTypes[i]) {
                    case SUM:
                        b.append(tabs + "openingWindowsResults[posInRes + resultIndex].value = buffer2[idx].value; \n");
                        break;
                    case AVG:
                        b.append(tabs + "openingWindowsResults[posInRes + resultIndex].value = buffer2[idx].value; \n");
                    case CNT:
                        b.append(tabs + "openingWindowsResults[posInRes + resultIndex].counter = buffer2[idx].counter; \n");
                        break;
                    case MIN:
                        b.append(tabs + "openingWindowsResults[posInRes + resultIndex].value = buffer2[idx].value; \n");
                        break;
                    case MAX:
                        b.append(tabs + "openingWindowsResults[posInRes + resultIndex].value = buffer2[idx].value; \n");
                        break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }

        //b.append(tabs+"resultIndex ++; \n");
        return b.toString();
	}

    public static String getInsertFunctor (AggregationType[] aggregationGroupByTypes, FloatColumnReference[] aggregationGroupByAttributes,
                                          Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (aggregationGroupByTypes.length > 0) {
            for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                switch (aggregationGroupByTypes[i]) {
                    case SUM:
                    case AVG:
                        b.append(String.format(tabs+"map.insert_and_modify(&data[currPos]._%d, data[currPos]._%d, data[currPos].timestamp);\n", ((ColumnReference)groupByAttributes[i]).getColumn(), aggregationGroupByAttributes[i].getColumn()));
                        break;
                    case CNT:
                        b.append(String.format(tabs+"map.insert_and_increment_counter(&data[currPos]._%d, data[currPos].timestamp);\n", ((ColumnReference)groupByAttributes[i]).getColumn()));
                        break;
                    case MIN:
                        b.append(String.format(tabs+"map.insert(&data[currPos]._%d, data[currPos]._%d, data[currPos].timestamp);\n", ((ColumnReference)groupByAttributes[i]).getColumn(), aggregationGroupByAttributes[i].getColumn()));
                        break;
                    case MAX:
                        b.append(String.format(tabs+"map.insert(&data[currPos]._%d, data[currPos]._%d, data[currPos].timestamp);\n", ((ColumnReference)groupByAttributes[i]).getColumn(), aggregationGroupByAttributes[i].getColumn()));
                        break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        return b.toString();
    }

    public static String getEvictFunctor (AggregationType[] aggregationGroupByTypes, FloatColumnReference[] aggregationGroupByAttributes,
                                         Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (aggregationGroupByTypes.length > 0) {
            for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
                switch (aggregationGroupByTypes[i]) {
                    case SUM:
                    case AVG:
                        b.append(String.format(tabs+"map.insert_and_modify(&data[i]._%d, -data[i]._%d, data[i].timestamp);\n", ((ColumnReference)groupByAttributes[i]).getColumn(), aggregationGroupByAttributes[i].getColumn()));
                        break;
                    case CNT:
                        b.append(String.format(tabs+"map.evict_and_decrement_counter(&data[i]._%d);\n", ((ColumnReference)groupByAttributes[i]).getColumn()));
                        break;
                    case MIN:
                        b.append(String.format(tabs+"map.evict(&data[i]._%d);\n", ((ColumnReference)groupByAttributes[i]).getColumn()));
                        break;
                    case MAX:
                        b.append(String.format(tabs+"map.evict(&data[i]._%d);\n", ((ColumnReference)groupByAttributes[i]).getColumn()));
                        break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        return b.toString();
    }

	public static String getMergeFunctor(AggregationType[] aggregationGroupByTypes, int numberOfTabs) {
		StringBuilder b = new StringBuilder ();

		String tabs = "";
		for (int i = 0; i < numberOfTabs; i++)
			tabs = tabs +"\t";

		if (aggregationGroupByTypes.length > 0) {
			for (int i = 0; i < aggregationGroupByTypes.length; ++i) {
				switch (aggregationGroupByTypes[i]) {
					case SUM:
                        b.append(tabs+"openingWindowsResults[idx].value += buffer2[posInB2].value;\n");
                        break;
					case AVG:
					    b.append(tabs+"openingWindowsResults[idx].value += buffer2[posInB2].value;\n");
					case CNT:
                        b.append(tabs+"openingWindowsResults[idx].counter += buffer2[posInB2].counter;\n");
						break;
					case MIN:
                        b.append(tabs+"openingWindowsResults[idx].value = (openingWindowsResults[idx].value < buffer2[posInB2].value) ? openingWindowsResults[idx].value : buffer2[posInB2].value;\n");
                        break;
					case MAX:
                        b.append(tabs+"openingWindowsResults[idx].value = (openingWindowsResults[idx].value > buffer2[posInB2].value) ? openingWindowsResults[idx].value : buffer2[posInB2].value;\n");
						break;
					default:
						throw new IllegalArgumentException("error: invalid aggregation type");
				}
			}
		}
		return b.toString();
	}
}
