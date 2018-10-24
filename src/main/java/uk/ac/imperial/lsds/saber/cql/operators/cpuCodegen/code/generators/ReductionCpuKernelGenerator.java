package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators;

import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;

public class ReductionCpuKernelGenerator {

    public static String getAggregationVariables (AggregationType[] aggregationTypes) {
        StringBuilder b = new StringBuilder ();

        // initialise variables
        if (aggregationTypes!=null && aggregationTypes.length > 0) {
            for (int i = 0; i < aggregationTypes.length; ++i) {
                switch (aggregationTypes[i]) {
                    case SUM:
                        b.append(String.format("\tfloat globalVar_%d = ;\n", (i+1), 0.0f));
                        break;
                    case AVG:
                        b.append(String.format("\tfloat globalVar_%d = ;\n", (i+1), 0.0f));
                    case CNT:
                        b.append(String.format("\tfloat globalVar_%d = ;\n", (i), 0.0f));
                        break;
                    case MIN:
                        b.append(String.format("\tfloat TwoStacks<float, MIN> twoStacks;;\n"));
                        break;
                    case MAX:
                        b.append(String.format("\tfloat TwoStacks<float, MAX> twoStacks;;\n"));
                        break;
                    default:
                        throw new IllegalArgumentException("error: invalid aggregation type");
                }
            }
        }
        return b.toString();
    }

    public static String getInsertFunctor(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        if (aggregationTypes.length > 0) {
            for (int i = 0; i < aggregationTypes.length; ++i) {
                switch (aggregationTypes[i]) {
                    case SUM:
                        b.append(String.format(tabs+"globalVar_%d += input_tuple_t[currPos]._%d;\n", (i+1), aggregationAttributes[i].getColumn()));
                        break;
                    case AVG:
                        b.append(String.format(tabs+"globalVar_%d += input_tuple_t[currPos]._%d;\n", (i+1), aggregationAttributes[i].getColumn()));
                    case CNT:
                        b.append(String.format(tabs+"globalVar_%d ++;\n", (i)));
                        break;
                    case MIN:
                        b.append(String.format(tabs+"twoStacks.insert(input_tuple_t[currPos]._%d);\n", aggregationAttributes[i].getColumn()));
                        break;
                    case MAX:
                        b.append(String.format(tabs+"twoStacks.insert(input_tuple_t[currPos]._%d);\n", aggregationAttributes[i].getColumn()));
                        break;
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
                        b.append(String.format(tabs+"float globalVar_%d -= input_tuple_t[i]._%d;\n", (i + 1), aggregationAttributes[i].getColumn()));
                        break;
                    case AVG:
                        b.append(String.format(tabs+"float globalVar_%d -= input_tuple_t[i]._%d;\n", (i + 1), aggregationAttributes[i].getColumn()));
                    case CNT:
                        b.append(String.format(tabs+"float globalVar_%d --;\n", (i)));
                        break;
                    case MIN:
                        b.append(String.format(tabs+"twoStacks.evict();\n"));
                        break;
                    case MAX:
                        b.append(String.format(tabs+"twoStacks.evict();\n"));
                        break;
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
