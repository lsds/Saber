package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators;

import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;

public class ReductionCpuKernelGenerator {
    public static String getInsertFunctor(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (aggregationTypes.length > 0) {
            for (int i = 0; i < aggregationTypes.length; ++i) {
                switch (aggregationTypes[i]) {
                    case SUM:
                        b.append(String.format(tabs+"float globalVar_%d += input_tuple_t[currPos]._%d;\n", (i+1), aggregationAttributes[i]));
                        break;
                    case AVG:
                        b.append(String.format(tabs+"float globalVar_%d += input_tuple_t[currPos]._%d;\n", (i+1), aggregationAttributes[i]));
                    case CNT:
                        b.append(String.format(tabs+"float globalVar_%d ++;\n", (i)));
                        break;
                    case MIN:
                        throw new UnsupportedOperationException("error: not supported yet...");
                        //break;
                    case MAX:
                        throw new UnsupportedOperationException("error: not supported yet...");
                        //break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        return b.toString();
    }

    public static String getEvictFunctor(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (aggregationTypes.length > 0) {
            for (int i = 0; i < aggregationTypes.length; ++i) {
                switch (aggregationTypes[i]) {
                    case SUM:
                        b.append(String.format(tabs+"float globalVar_%d -= input_tuple_t[i]._%d;\n", (i + 1), aggregationAttributes[i]));
                        break;
                    case AVG:
                        b.append(String.format(tabs+"float globalVar_%d -= input_tuple_t[i]._%d;\n", (i + 1), aggregationAttributes[i]));
                    case CNT:
                        b.append(String.format(tabs+"float globalVar_%d --;\n", (i)));
                        break;
                    case MIN:
                        throw new UnsupportedOperationException("error: not supported yet...");
                        //break;
                    case MAX:
                        throw new UnsupportedOperationException("error: not supported yet...");
                        //break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        return b.toString();
    }

    public static String getMergeFunctor(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, int numberOfTabs) {
        throw new UnsupportedOperationException("error: not supporting yet.");
    }
}
