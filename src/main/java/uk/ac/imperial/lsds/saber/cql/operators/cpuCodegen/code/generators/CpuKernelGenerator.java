package uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.code.generators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

public class CpuKernelGenerator {
	
	private static String load (String filename) {
		File file = new File(filename);
		try {
			byte [] bytes = Files.readAllBytes(file.toPath());
			return new String (bytes, "UTF8");
		} catch (FileNotFoundException e) {
			System.err.println(String.format("error: file %s not found", filename));
		} catch (IOException e) {
			System.err.println(String.format("error: cannot read file %s", filename));
		}
		return null;
	}
	
	public static String getHeader (ITupleSchema input1, ITupleSchema input2, ITupleSchema output) {
		
		boolean isJoin = (input2 != null);
		
		StringBuilder b = new StringBuilder ();

        b.append("#include \"uk_ac_imperial_lsds_saber_devices_TheCPU.h\" \n");
        b.append("#include <jni.h>\n");
        //b.append("#include \"hashTable.h\" \n");
		b.append("\n");
        b.append("#include <float.h>\n");
        b.append("#include <limits.h>\n");
        b.append("\n");

		/*int input1VectorSize = getVectorSize (input1.getTupleSize(), bytes1);
		int outputVectorSize = getVectorSize (output.getTupleSize(), bytes3);
		
		if (isJoin) {
			int input2VectorSize = getVectorSize (input2.getTupleSize(), bytes2);
			
			b.append(String.format("#define S1_INPUT_VECTOR_SIZE %d\n", input1VectorSize));
			b.append(String.format("#define S2_INPUT_VECTOR_SIZE %d\n", input2VectorSize));
		} else {
			b.append(String.format("#define INPUT_VECTOR_SIZE %d\n", input1VectorSize));
		}
		b.append(String.format("#define OUTPUT_VECTOR_SIZE %d\n", outputVectorSize));
		b.append("\n");*/
		
		if (isJoin)
			b.append(getInputHeader (input1, "s1"));
		else
			b.append(getInputHeader (input1, null));
		b.append("\n");
		
		if (isJoin) {
			b.append(getInputHeader (input2, "s2"));
			b.append("\n");
		}
		
		b.append(getOutputHeader (output));
		b.append("\n");
		
		return b.toString();
	}
	
	private static String getInputHeader (ITupleSchema schema, String prefix) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("typedef struct {\n");
		/* The first attribute is always a timestamp */
		b.append("\tlong timestamp;\n");
		for (int i = 1; i < schema.numberOfAttributes(); i++) {
			
			PrimitiveType type = schema.getAttributeType(i);
			
			switch(type) {
			case INT:   b.append(String.format("\tint _%d;\n",   i)); break;
			case FLOAT: b.append(String.format("\tfloat _%d;\n", i)); break;
			case LONG:  b.append(String.format("\tlong _%d;\n",  i)); break;
			
			case UNDEFINED:
				System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
				System.exit(1);
			}
		}
		
		if (schema.getPad().length > 0)
			b.append(String.format("\tuchar pad[%d];\n", schema.getPad().length));

        if (prefix == null)
            b.append("} input_tuple_t __attribute__((aligned(1)));\n");
        else
            b.append(String.format("} %s_input_tuple_t __attribute__((aligned(1)));\n", prefix));
        b.append("\n");

		return b.toString();
	}
	
	private static String getOutputHeader (ITupleSchema schema) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("typedef struct {\n");
		
		if (schema.getAttributeType(0) == PrimitiveType.LONG) {
			/* The first long attribute is assumed to be always a timestamp */
			b.append("\tlong timestamp;\n");
			for (int i = 1; i < schema.numberOfAttributes(); i++) {
				
				PrimitiveType type = schema.getAttributeType(i);
				
				switch(type) {
				case INT:   b.append(String.format("\tint _%d;\n",   i)); break;
				case FLOAT: b.append(String.format("\tfloat _%d;\n", i)); break;
				case LONG:  b.append(String.format("\tlong _%d;\n",  i)); break;
				
				case UNDEFINED:
					System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
					System.exit(1);
				}
			}
		} else {
			for (int i = 0; i < schema.numberOfAttributes(); i++) {
				
				PrimitiveType type = schema.getAttributeType(i);
				
				switch(type) {
				case INT:   b.append(String.format("\tint _%d;\n",   (i + 1))); break;
				case FLOAT: b.append(String.format("\tfloat _%d;\n", (i + 1))); break;
				case LONG:  b.append(String.format("\tlong _%d;\n",  (i + 1))); break;
				
				case UNDEFINED:
					System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
					System.exit(1);
				}
			}
		}
		
		if (schema.getPad().length > 0)
			b.append(String.format("\tuchar pad[%d];\n", schema.getPad().length));
		
		b.append("} output_tuple_t __attribute__((aligned(1)));\n");
		b.append("\n");
		
		return b.toString();
	}
	
	public static String getWindowDefinition (WindowDefinition windowDefinition) {
		
		StringBuilder b = new StringBuilder();
		
		if (windowDefinition.isRangeBased())
			b.append("#define RANGE_BASED\n");
		else
			b.append("#define COUNT_BASED\n");
		
		b.append("\n");

        b.append(String.format("#define WINDOW_SIZE      %dL\n", windowDefinition.getSize()));
        b.append(String.format("#define WINDOW_SLIDE     %dL\n", windowDefinition.getSlide()));
		b.append(String.format("#define PANES_PER_WINDOW %dL\n", windowDefinition.numberOfPanes()));
		b.append(String.format("#define PANES_PER_SLIDE  %dL\n", windowDefinition.panesPerSlide()));
		b.append(String.format("#define PANE_SIZE        %dL\n", windowDefinition.getPaneSize()));

        b.append(String.format("#define BUFFER_SIZE      %d\n", SystemConf.CIRCULAR_BUFFER_SIZE));
        b.append("\n");
        b.append("\n");

        return b.toString();
	}

	public static String getHashTableDefinition (AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes,
                                                  Expression [] groupByAttributes, int keyLength, int valueLength) {

		StringBuilder b = new StringBuilder();

		String filename = null;
		if (groupByAttributes==null) {
			b.append("//a hashtable isn't required in this case \n");
            return b.toString();
		}
		else {

            b.append("#include <cstdlib> \n");
            b.append("#include <cstring> \n");

            b.append(String.format("#define MAP_SIZE         %d\n", SystemConf.C_HASH_TABLE_SIZE));
            b.append(String.format("#define KEY_SIZE         %d\n", keyLength));
            b.append(String.format("#define VALUE_SIZE       %d\n", valueLength));

            for (int i = 0; i < aggregationTypes.length; ++i) {
                if (aggregationTypes[i] != AggregationType.CNT &&
                        aggregationTypes[i] != AggregationType.SUM &&
                        aggregationTypes[i] != AggregationType.AVG) {
                    filename = SystemConf.SABER_HOME + "/clib/cpu_templates/non_inv_hashtable_tmpl";
                    b.append(String.format("#define BUCKET_SIZE       %d\n", SystemConf.C_HASH_TABLE_BUCKET_SIZE));
                } else {
                    filename = SystemConf.SABER_HOME + "/clib/cpu_templates/hashtable_tmpl";
                }
            }


            b.append(load(filename)).append("\n");
            b.append("\n");

            return b.toString();
		}
	}

	public static String getProjectionOperator (String filename, ITupleSchema input, ITupleSchema output, int depth) {
        throw new UnsupportedOperationException("error: not supported yet...");
	}
	
	public static String getSelectionOperator (IPredicate predicate, String customPredicate) {
        return SelectionCpuKernelGenerator.getFunctor (predicate, customPredicate);
	}
	
	public static String getThetaJoinOperator(String filename, ITupleSchema left, ITupleSchema right, ITupleSchema output,
		IPredicate predicate, String customPredicate, String customCopy) {
        throw new UnsupportedOperationException("error: not supported yet...");
	}
	
	public static String getReductionInsertOperator (AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, int numberOfTabs) {
        return ReductionCpuKernelGenerator.getInsertFunctor (aggregationTypes, aggregationAttributes, numberOfTabs);
	}

    public static String getReductionEvictOperator (AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, int numberOfTabs) {
        return ReductionCpuKernelGenerator.getEvictFunctor (aggregationTypes, aggregationAttributes, numberOfTabs);
    }

    public static String getReductionMergeOperator (AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, int numberOfTabs) {
        return ReductionCpuKernelGenerator.getMergeFunctor (aggregationTypes, aggregationAttributes, numberOfTabs);
    }

	public static String getAggregationInsertOperator (AggregationType [] aggregationGroupByTypes, FloatColumnReference [] aggregationGroupByAttributes,
		Expression [] groupByAttributes, int numberOfTabs) {
        return AggregationCpuKernelGenerator.getInsertFunctor (aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, numberOfTabs);
	}

    public static String getAggregationEvictOperator (AggregationType [] aggregationGroupByTypes, FloatColumnReference [] aggregationGroupByAttributes,
                                                       Expression [] groupByAttributes, int numberOfTabs) {
        return AggregationCpuKernelGenerator.getEvictFunctor (aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, numberOfTabs);
    }

    public static String getAggregationMergeOperator(AggregationType[] aggregationGroupByTypes, int numberOfTabs) {
        return AggregationCpuKernelGenerator.getMergeFunctor (aggregationGroupByTypes, numberOfTabs);
    }

    public static String getTemplateTypes(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, Expression[] groupByAttributes,
                                          boolean isHashTableDefinition) {
        StringBuilder b = new StringBuilder ();
        String s = AggregationCpuKernelGenerator.getTemplateDefinition (aggregationTypes, groupByAttributes, aggregationAttributes.length, isHashTableDefinition);
        b.append(s);
        return b.toString();
    }

    public static String getSignature(String node) {
        StringBuilder b = new StringBuilder ();
        String s =
                "JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_singleOperatorComputation\n" +
                "  (JNIEnv * env, jobject obj, jobject buffer, jint bufferStartPointer, jint bufferEndPointer,\n" +
                "   jobject openingWindowsBuffer, jobject closingWindowsBuffer, jobject pendingWindowsBuffer, jobject completeWindowsBuffer,\n" +
                "   jobject openingWindowsStartPointers, jobject closingWindowsStartPointers,\n" +
                "   jobject pendingWindowsStartPointers, jobject completeWindowsStartPointers,\n" +
                "   jlong streamStartPointer, jint openingWindowsPointer, jint closingWindowsPointer,\n" +
                "   jint pendingWindowsPointer, jint completeWindowsPointer,\n" +
                "   jobject arrayHelperBuffer) {\n" +
                "\n" +
                "    (void) obj;\n" +
                "\n" +
                "    // Input Buffer\n" +
                "    input_tuple_t *data= (input_tuple_t *) env->GetDirectBufferAddress(buffer);\n" +
                "\n" +
                "    // Output Buffers\n" +
                "    "+node+" *openingWindowsResults = ("+node+" *) env->GetDirectBufferAddress(openingWindowsBuffer); // the results here are in the\n" +
                "    "+node+" *closingWindowsResults = ("+node+" *) env->GetDirectBufferAddress(closingWindowsBuffer); // form of the hashtable\n" +
                "    "+node+" *pendingWindowsResults = ("+node+" *) env->GetDirectBufferAddress(pendingWindowsBuffer);\n" +
                "    output_tuple_t *completeWindowsResults = (output_tuple_t *) env->GetDirectBufferAddress(completeWindowsBuffer); // the results here are packed\n" +
                "    int * arrayHelper = (int *) env->GetDirectBufferAddress(arrayHelperBuffer);\n" +
                "    int *openingWindowsPointers = (int *) env->GetDirectBufferAddress(openingWindowsStartPointers);\n" +
                "    int *closingWindowsPointers = (int *) env->GetDirectBufferAddress(closingWindowsStartPointers);\n" +
                "    int *pendingWindowsPointers = (int *) env->GetDirectBufferAddress(pendingWindowsStartPointers);\n" +
                "    int *completeWindowsPointers = (int *) env->GetDirectBufferAddress(completeWindowsStartPointers);\n" +
                "\n" +
                "    // Set the first pointer for all types of windows\n" +
                "    openingWindowsPointers[0] = openingWindowsPointer;\n" +
                "    closingWindowsPointers[0] = closingWindowsPointer;\n" +
                "    pendingWindowsPointers[0] = pendingWindowsPointer;\n" +
                "    completeWindowsPointers[0] = completeWindowsPointer;\n" +
                "\n" +
                "    /*printf(\"---- \\n\");\n" +
                "    printf(\"bufferStartPointer %d \\n\", bufferStartPointer);\n" +
                "    printf(\"bufferEndPointer %d \\n\", bufferEndPointer);\n" +
                "    printf(\"streamStartPointer %d \\n\", streamStartPointer);\n" +
                "    printf(\"data[bufferStartPointer].timestamp %lu, vehicle %d \\n\", data[bufferStartPointer].timestamp, data[bufferStartPointer]._1);\n" +
                "    printf(\"data[bufferEndPointer-1].timestamp %lu, vehicle %d \\n\", data[bufferEndPointer-1].timestamp, data[bufferEndPointer]._1);\n" +
                "    fflush(stdout);\n" +
                "    printf(\"windowSize %ld \\n\", WINDOW_SIZE);\n" +
                "    printf(\"windowSlide %ld \\n\", WINDOW_SLIDE);\n" +
                "    printf(\"openingWindowsPointer %d \\n\", openingWindowsPointer);\n" +
                "    printf(\"closingWindowsPointer %d \\n\", closingWindowsPointer);\n" +
                "    printf(\"pendingWindowsPointer %d \\n\", pendingWindowsPointer);\n" +
                "    printf(\"completeWindowsPointer %d \\n\", completeWindowsPointer);\n" +
                "    printf(\"panesPerWindow %ld \\n\", PANES_PER_WINDOW);\n" +
                "    printf(\"panesPerSlide %ld \\n\", PANES_PER_SLIDE);\n" +
                "    printf(\"windowPaneSize %ld \\n\", PANE_SIZE);\n" +
                "    printf(\"---- \\n\");\n" +
                "    fflush(stdout);*/" +
                "\n" +
                "    int openingWindows = 0;\n" +
                "    int closingWindows = 0;\n" +
                "    int pendingWindows = 0;\n" +
                "    int completeWindows = 0;\n" +
                "\n" +
                "    // Query specific variables\n" +
                "    const int startPositionsSize = (int) ((bufferEndPointer-bufferStartPointer) / WINDOW_SLIDE);\n" +
                "    const int endPositionsSize = startPositionsSize; //- WINDOW_SIZE/1024 + 1;\n" +
                "    int *startPositions = (int *) malloc(startPositionsSize* 2 * sizeof(int));\n" +
                "    int *endPositions = (int *) malloc(endPositionsSize* 2 * sizeof(int));\n" +
                "\n" +
                "    for (int i = 0; i < startPositionsSize*2; i++) {\n" +
                "        startPositions[i] = -1;\n" +
                "        endPositions[i] = -1;\n" +
                "    }\n" +
                "\n" +
                "    long tempPane;\n" +
                "    long tempCompletePane = (data[bufferStartPointer].timestamp%WINDOW_SLIDE==0) ?\n" +
                "                            (data[bufferStartPointer].timestamp / PANE_SIZE) :\n" +
                "                            (data[bufferStartPointer].timestamp / PANE_SIZE) + 1;\n" +
                "    long tempOpenPane = (data[bufferStartPointer].timestamp%WINDOW_SLIDE==0) ?\n" +
                "                        (data[bufferStartPointer].timestamp / PANE_SIZE) - 1 :\n" +
                "                        (data[bufferStartPointer].timestamp / PANE_SIZE);\n" +
                "    startPositions[0] = bufferStartPointer;\n" +
                "    int currentSlide = 1;\n" +
                "    int currentWindow = 0;\n" +
                "    int currPos = bufferStartPointer;\n" +
                "\n" +
                "    if (data[bufferEndPointer-1].timestamp < data[bufferStartPointer].timestamp) {\n" +
                "        printf(\"The input is messed up...\");\n" +
                "        exit(-1);\n" +
                "    }\n" +
                "\n" +
                "    long activePane;\n" +
                "    bool hasComplete = ((data[bufferEndPointer - 1].timestamp - data[bufferStartPointer].timestamp) / PANE_SIZE) >=\n" +
                "                       PANES_PER_WINDOW;\n" +
                "    int firstPane = data[bufferStartPointer].timestamp / PANE_SIZE;\n" +
                "    bool startingFromPane = (bufferStartPointer==0) ? true : (data[bufferStartPointer].timestamp % firstPane == 0);\n";

        b.append(s).append("\n");
        return b.toString();
    }

    public static String getGroupByVariables(Expression[] groupByAttributes, String templateTypes, String templateTypesForHashTable) {
        StringBuilder b = new StringBuilder ();
        String s = AggregationCpuKernelGenerator.getAggregationVariables (groupByAttributes, templateTypes, templateTypesForHashTable);
        b.append(s).append("\n");
        return b.toString();
    }

    public static String getReductionVariables(AggregationType[] aggregationTypes) {
        StringBuilder b = new StringBuilder ();
        String s = ReductionCpuKernelGenerator.getAggregationVariables (aggregationTypes);
        b.append(s).append("\n");
        return b.toString();
    }

    public static String getWriteCompleteWindowsBlock (AggregationType[] aggregationGroupByTypes, Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        // having clause
        String s = AggregationCpuKernelGenerator.getWriteCompleteWindowsBlock (aggregationGroupByTypes, groupByAttributes, numberOfTabs);

        b.append(s).append("\n");
        return b.toString();
    }

    public static String getComputationBlockForInsert (WindowDefinition windowDefinition, AggregationType[] aggregationTypes,
                                                      FloatColumnReference[] aggregationAttributes, AggregationType[] aggregationGroupByTypes,
                                                      FloatColumnReference[] aggregationGroupByAttributes, Expression[] groupByAttributes,
                                                      Expression[] expressions, IPredicate predicate, ITupleSchema inputSchema, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        // selection
        if (predicate!=null) {
            b.append(tabs + getSelectionOperator(predicate, null) + "{");
            numberOfTabs++;
        }
        // single group aggregate
        if (aggregationAttributes!=null && aggregationAttributes.length > 0)
            b.append(getReductionInsertOperator(aggregationTypes, aggregationAttributes, numberOfTabs));

        // group by aggregate
        if (aggregationGroupByAttributes!=null && aggregationGroupByAttributes.length > 0)
            b.append(getAggregationInsertOperator(aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, numberOfTabs));

        // end of selection
        if (predicate!=null)
            b.append(tabs+"}");

        return b.toString();
    }

    public static String getComputationBlockForEvict (WindowDefinition windowDefinition, AggregationType[] aggregationTypes,
                                                      FloatColumnReference[] aggregationAttributes, AggregationType[] aggregationGroupByTypes,
                                                      FloatColumnReference[] aggregationGroupByAttributes, Expression[] groupByAttributes,
                                                      Expression[] expressions, IPredicate predicate, ITupleSchema inputSchema, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        String tabs = "";
        for (int i = 0; i < numberOfTabs; i++)
            tabs = tabs +"\t";

        // selection
        if (predicate!=null) {
            b.append(tabs + getSelectionOperator(predicate, null) + "{");
            numberOfTabs++;
        }
        // single group aggregate
		if (aggregationAttributes!=null && aggregationAttributes.length > 0)
            b.append(getReductionEvictOperator(aggregationTypes, aggregationAttributes, numberOfTabs));

        // group by aggregate
		if (aggregationGroupByAttributes!=null && aggregationGroupByAttributes.length > 0)
            b.append(getAggregationEvictOperator(aggregationGroupByTypes, aggregationGroupByAttributes, groupByAttributes, numberOfTabs));

        // end of selection
        if (predicate!=null)
            b.append(tabs+"}");

        return b.toString();
    }

    public static String getWriteCompleteWindowsBlockForMerge(AggregationType[] aggregationGroupByTypes,
                                                              Expression[] groupByAttributes, int bufferId, int numberOfTabs, boolean isFound) {
        StringBuilder b = new StringBuilder ();

        // having clause
        String s = AggregationCpuKernelGenerator.getWriteCompleteWindowsBlockForMerge(aggregationGroupByTypes, groupByAttributes, bufferId, numberOfTabs, isFound);

        b.append(s).append("\n");
        return b.toString();
    }

    public static String getMergeOpeningWindowsBlock(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes,
                                                     AggregationType[] aggregationGroupByTypes, FloatColumnReference[] aggregationGroupByAttributes,
                                                     int numberOfTabs) {
        StringBuilder b = new StringBuilder ();


        // single group aggregate
        if (aggregationAttributes!=null && aggregationAttributes.length > 0)
            b.append(getReductionMergeOperator(aggregationTypes, aggregationAttributes, numberOfTabs));

        // group by aggregate
        if (aggregationGroupByAttributes!=null && aggregationGroupByAttributes.length > 0)
            b.append(getAggregationMergeOperator(aggregationGroupByTypes, numberOfTabs));

        return b.toString();
    }

    public static String getWriteOpeningWindowsBlockForMerge(AggregationType[] aggregationGroupByTypes, Expression[] groupByAttributes, int numberOfTabs) {
        StringBuilder b = new StringBuilder ();

        // having clause
        String s = AggregationCpuKernelGenerator.getWriteOpeningWindowsBlockForMerge(aggregationGroupByTypes, groupByAttributes, numberOfTabs);

        b.append(s).append("\n");
        return b.toString();
	}

    public static String sw_p1 =
            "    // the beginning of the stream. Check if we have at least one complete window so far!\n" +
            "    if (streamStartPointer == 0) {\n" +
            "        tempPane = data[bufferStartPointer].timestamp / PANE_SIZE;\n" +
            "        // compute the first window and check if it is complete!\n" +
            "        while (currPos < bufferEndPointer) {\n" +
            "            activePane = data[currPos].timestamp / PANE_SIZE;\n" +
            "            if (activePane - tempPane >= PANES_PER_SLIDE) {\n" +
            "                tempPane = activePane;\n" +
            "                startPositions[currentSlide] = currPos;\n" +
            "                currentSlide++;\n" +
            "            }\n" +
            "            if (activePane - tempCompletePane >= PANES_PER_WINDOW) {\n" +
            "                endPositions[currentWindow] = currPos;\n" +
            "                currentWindow++;\n" +
            "                //currPos++;\n" +
            "                completeWindows++;\n" +
            "                break;\n" +
            "            }\n";
    public static String sw_p2 =
            "            currPos++;\n" +
            "        }\n" +
            "\n" +
            "    } else if ((data[bufferEndPointer - 1].timestamp / PANE_SIZE) <\n" +
            "               PANES_PER_WINDOW) { //we still have a pending window until the first full window is closed.\n" +
            "        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / PANE_SIZE :\n" +
            "                   data[BUFFER_SIZE / sizeof(input_tuple_t) - 1].timestamp / PANE_SIZE;\n" +
            "        while (currPos < bufferEndPointer) {\n";
    public static String get_sw_p3 (String setupValues) {
        return
            "            activePane = data[currPos].timestamp / PANE_SIZE;\n" +
            "            if (activePane - tempPane >= PANES_PER_SLIDE) { // there may be still opening windows\n" +
            "                tempPane = activePane;\n" +
            "                startPositions[currentSlide] = currPos;\n" +
            "                currentSlide++;\n" +
            "            }\n" +
            "            currPos++;\n" +
            "        }\n" +
            "    } else { // this is not the first batch, so we get the previous panes for the closing and opening windows\n" +
            "        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / PANE_SIZE :\n" +
            "                   data[BUFFER_SIZE / sizeof(input_tuple_t) - 1].timestamp / PANE_SIZE; // TODO: fix this!!\n" +
            "        // compute the closing windows util we reach the first complete window. After this point we start to remove slides!\n" +
            "        // There are two discrete cases depending on the starting timestamp of this batch. In the first we don't count the last closing window, as it is complete.\n" +
            "        //printf(\"data[bufferStartPointer].timestamp %ld \\n\", data[bufferStartPointer].timestamp);\n" +
            "        while (currPos < bufferEndPointer) {\n" +
            "            activePane = data[currPos].timestamp / PANE_SIZE;\n" +
            "            if (activePane - tempCompletePane >= PANES_PER_WINDOW) { // complete window\n" +
            "                if (startPositions[currentSlide - 1] !=\n" +
            "                    currPos) { // check if I have already added this position in the previous step!\n" +
            "                    startPositions[currentSlide] = currPos;\n" +
            "                    currentSlide++;\n" +
            "                }\n" +
            "                endPositions[currentWindow] = currPos;\n" +
            "                currentWindow++;\n" +
            "                //currPos++;\n" +
            "                completeWindows++;\n" +
            "                break;\n" +
            "            }\n" +
            "            // Does the second check stand???\n" +
            "            if (activePane - tempPane >= PANES_PER_SLIDE &&\n" +
            "                activePane >= PANES_PER_WINDOW) {//activePane - tempPane < PANES_PER_WINDOW) { // closing window\n" +
            "                tempPane = activePane;\n" +
            "                "+setupValues+"\n" +
            "                // write result to the closing windows\n" +
            "                std::memcpy(closingWindowsResults + closingWindowsPointer, hTable, MAP_SIZE * ht_node_size);\n" +
            "                closingWindowsPointer += MAP_SIZE;\n" +
            "                closingWindows++;\n" +
            "                closingWindowsPointers[closingWindows] = closingWindowsPointer; //- 1;\n" +
            "                //printf(\"activePane %d \\n\", activePane);\n" +
            "            }\n";
    }
    public static String sw_p4 =
            "            if (activePane - tempOpenPane >= PANES_PER_SLIDE) { // new slide and possible opening windows\n" +
            "                tempOpenPane = activePane;\n" +
            "                // count here and not with the closing windows the starting points of slides!!\n" +
            "                //startPositions[currentSlide] = currPos;\n" +
            "                //currentSlide++;\n" +
            "                if (startPositions[currentSlide - 1] !=\n" +
            "                    currPos) { // check if I have already added this position in the previous step!\n" +
            "                    startPositions[currentSlide] = currPos;\n" +
            "                    currentSlide++;\n" +
            "                }\n" +
            "            }\n" +
            "            currPos++;\n" +
            "        }\n" +
            "        // remove the extra values so that we have the first complete window\n" +
            "        if (completeWindows > 0 && !startingFromPane) {\n" +
            "            for (int i = bufferStartPointer; i < startPositions[1]; i++ ) {\n" ;
    public static String get_sw_p5 (String setValues){
        return
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "    "+setValues+"\n" +
            "    int tempStartPosition;\n" +
            "    int tempEndPosition;\n" +
            "    // Check if we have one pending window\n" +
            "    if (streamStartPointer != 0 && !hasComplete) {  // (completeWindows == 0 && closingWindows == 0) {\n" +
            "        // write results\n" +
            "        std::memcpy(pendingWindowsResults + pendingWindowsPointer, hTable, MAP_SIZE * ht_node_size);\n" +
            "        pendingWindowsPointer += MAP_SIZE;\n" +
            "        pendingWindows++;\n" +
            "        pendingWindowsPointers[pendingWindows] = pendingWindowsPointer; // - 1;\n" +
            "    }\n" +
            "\n" +
            "    if (completeWindows == 0 && ((streamStartPointer == 0) || (currentSlide > 1 && data[startPositions[0]].timestamp%WINDOW_SLIDE==0))) { // We only have one opening window, so we write it and return...\n" +
            "        // write results\n" +
            "        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);\n" +
            "        openingWindowsPointer += MAP_SIZE;\n" +
            "        openingWindows++;\n" +
            "        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;\n" +
            "    } else if (completeWindows > 0) { // we have at least one complete window...\n" +
            "\n" +
            "        // write results and pack them for the first complete window in the batch\n" +
            "        for (int i = 0; i < MAP_SIZE; i++) {\n";
    }
    public static String sw_p6 =
            "        }\n" +
            "        // write in the correct slot, as the value has already been incremented!\n" +
            "        completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;\n";
    public static String sw_p7 =
            "        currPos++;\n" +
            "\n" +
            "        // compute the rest windows\n" +
            "        currPos = endPositions[0];\n" +
            "        tempPane = data[currPos].timestamp/PANE_SIZE; //currStartPos = data[currPos].timestamp; //startPositions[currentWindow];\n" +
            "        int removalIndex = (startingFromPane) ? currentWindow : currentWindow + 1; \n" +
            "        while (currPos < bufferEndPointer) {\n" +
            "            // remove previous slide\n" +
            "            tempStartPosition = startPositions[removalIndex - 1];\n" +
            "            tempEndPosition = startPositions[removalIndex];\n" +
            "            for (int i = tempStartPosition; i < tempEndPosition; i++) {\n";
    public static String get_sw_p8(String setValues) {
        return
            "            }\n" +
            "            // add elements from the next slide\n" +
            "            currPos = endPositions[currentWindow - 1] + 1; // take the next position, as we have already computed this value\n" +
            "            while (true) {\n" +
            "\n" +
            "                activePane = data[currPos].timestamp/PANE_SIZE;\n" +
            "                // complete windows\n" +
            "                if (activePane - tempPane >= PANES_PER_SLIDE ) { //&& (data[bufferEndPointer-1].timestamp/PANE_SIZE) - activePane>= PANES_PER_WINDOW-1\n" +
            "                    tempPane = data[currPos].timestamp/PANE_SIZE;\n" +
            "                    startPositions[currentSlide] = currPos;\n" +
            "                    currentSlide++;\n" +
            "                    endPositions[currentWindow] = currPos;\n" +
            "                    currentWindow++;\n" +
            "                    // write and pack the complete window result\n" +
            "                    "+setValues+"\n" +
            "                    for (int i = 0; i < MAP_SIZE; i++) {\n";
    }
    public static String sw_p9 =
            "                    }\n" +
            "                    completeWindows++;\n" +
            "                    completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;\n" +
            "                    // increment before exiting for a complete window\n";
    public static String sw_p10 =
            "                    currPos++;\n" +
            "                    break;\n" +
            "                }\n";
    public static String get_sw_p11(String setValues) {
        return
            "                currPos++;\n" +
            "                if (currPos >= bufferEndPointer) { // we have reached the first open window after all the complete ones\n" +
            "                    // write the first open window if we have already computed the result, otherwise remove the respective tuples\n" +
            "                    if ((data[bufferEndPointer-1].timestamp/PANE_SIZE) -\n" +
            "                                (data[tempEndPosition].timestamp/PANE_SIZE) < PANES_PER_WINDOW) {\n" +
            "                        "+setValues+"\n"+
            "                        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);\n" +
            "                        openingWindowsPointer += MAP_SIZE;\n" +
            "                        openingWindows++;\n" +
            "                        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;\n" +
            "                    }\n" +
            "                    break;\n" +
            "                }\n" +
            "            }\n" +
            "            removalIndex++;\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    // compute the rest opening windows\n" +
            "    //currentWindow += (openingWindows==1); // if opening windows are already one, we have to compute one less\n" +
            "    if (completeWindows > 0) {\n" +
            "        currentWindow = (startingFromPane) ? currentWindow : currentWindow + 1;\n" +
            "    }\n" +
            "    while (currentWindow < currentSlide - 1) {\n" +
            "        // remove previous slide\n" +
            "        tempStartPosition = startPositions[currentWindow];\n" +
            "        tempEndPosition = startPositions[currentWindow + 1];\n" +
            "        currentWindow++;\n" +
            "        if (tempStartPosition == tempEndPosition) continue; \n" +
            "        for (int i = tempStartPosition; i < tempEndPosition; i++) {\n";
    }
    public static String get_sw_p12 (String setValues) {
        return
            "        }\n" +
            "        // write result to the opening windows\n" +
            "        "+setValues+"\n"+
            "        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);\n" +
            "        openingWindowsPointer += MAP_SIZE;\n" +
            "        openingWindows++;\n" +
            "        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;\n" +
            "\n" +
            "    }\n" +
            "\n" +
                    "\n" +
                    "/*printf(\"bufferStartPointer %d \\n\", bufferStartPointer);\n" +
                    "printf(\"bufferEndPointer %d \\n\", bufferEndPointer);\n" +
                    "printf(\"streamStartPointer %d \\n\", streamStartPointer);\n" +
                    "printf(\"data[bufferStartPointer].timestamp %lu, vehicle %d \\n\", data[bufferStartPointer].timestamp, data[bufferStartPointer]._1);\n" +
                    "printf(\"data[bufferEndPointer-1].timestamp %lu, vehicle %d \\n\", data[bufferEndPointer-1].timestamp, data[bufferEndPointer]._1);\n" +
                    "printf(\"openingWindowsPointer %d \\n\", openingWindowsPointer);\n" +
                    "printf(\"closingWindowsPointer %d \\n\", closingWindowsPointer);\n" +
                    "printf(\"pendingWindowsPointer %d \\n\", pendingWindowsPointer);\n" +
                    "printf(\"completeWindowsPointer %d \\n\", completeWindowsPointer);\n" +
                    "printf(\"openingWindows %d \\n\", openingWindows);\n" +
                    "printf(\"closingWindows %d \\n\", closingWindows);\n" +
                    "printf(\"pendingWindows %d \\n\", pendingWindows);\n" +
                    "printf(\"completeWindows %d \\n\", completeWindows);\n" +
                    "printf(\"---- \\n\");\n" +
                    "fflush(stdout);\n" +
                    "\n" +
                    "printf(\"bufferStartPointer %d \\n\", bufferStartPointer);\n" +
                    "printf(\"bufferEndPointer %d \\n\", bufferEndPointer);\n" +
                    "printf(\"streamStartPointer %d \\n\", streamStartPointer);\n" +
                    "printf(\"first timestamp %lu \\n\", data[bufferStartPointer].timestamp);\n" +
                    "printf(\"second timestamp %lu \\n\", data[bufferEndPointer-1].timestamp);\n" +
                    "printf(\"streamStartPointer %d \\n\", streamStartPointer);\n" +
                    "printf(\"openingWindows %d \\n\", openingWindows);\n" +
                    "if (openingWindows > 0) {\n" +
                    "    printf(\"occupancy, timestamp, key, value \\n\");\n" +
                    "    for (int i = 0;  i < openingWindows; i++) {\n" +
                    "        int base = i * MAP_SIZE;\n" +
                    "        for (int j = 0; j < MAP_SIZE ; j++) {\n" +
                    "            printf(\" %d, %ld, %d, %d \\n\", openingWindowsResults[base + j].status, openingWindowsResults[base + j].timestamp,\n" +
                    "            openingWindowsResults[base + j].key, openingWindowsResults[base + j].counter);\n" +
                    "        }\n" +
                    "        printf(\"------ \\n\");\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "printf(\"closingWindows %d \\n\", closingWindows);\n" +
                    "if (closingWindows > 0) {\n" +
                    "    printf(\"occupancy, timestamp, key, value \\n\");\n" +
                    "    for (int i = 0;  i < closingWindows; i++) {\n" +
                    "        int base = i * MAP_SIZE;\n" +
                    "        for (int j = 0; j < MAP_SIZE ; j++) {\n" +
                    "            printf(\" %d, %ld, %d, %d \\n\", closingWindowsResults[base + j].status, closingWindowsResults[base + j].timestamp,\n" +
                    "            closingWindowsResults[base + j].key, closingWindowsResults[base + j].counter);\n" +
                    "        }\n" +
                    "        printf(\"------ \\n\");\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "printf(\"pendingWindows %d \\n\", pendingWindows);\n" +
                    "if (pendingWindows > 0) {\n" +
                    "    printf(\"occupancy, timestamp, key, value \\n\");\n" +
                    "    for (int i = 0;  i < pendingWindows; i++) {\n" +
                    "        int base = i * MAP_SIZE;\n" +
                    "        for (int j = 0; j < MAP_SIZE; j++) {\n" +
                    "            printf(\" %d, %ld, %d, %d \\n\", pendingWindowsResults[base + j].status, pendingWindowsResults[base + j].timestamp,\n" +
                    "            pendingWindowsResults[base + j].key, pendingWindowsResults[base + j].counter);\n" +
                    "        }\n" +
                    "        printf(\"------ \\n\");\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "printf(\"completeWindows %d \\n\", completeWindows);\n" +
                    "fflush(stdout);\n" +
                    "\n" +
                    "if (completeWindows > 0) {\n" +
                    "    printf(\"timestamp, key, value \\n\");\n" +
                    "    for (int i = 0;  i < completeWindows; i++) {\n" +
                    "        int base = i * MAP_SIZE;\n" +
                    "        for (int j = 0; j < MAP_SIZE; j++) {\n" +
                    "            printf(\"%ld, %d, %f \\n\", data[bufferStartPointer].timestamp,\n" +
                    "            completeWindowsResults[base + j]._1, completeWindowsResults[base + j]._2);\n" +
                    "        }\n" +
                    "        printf(\"------ \\n\");\n" +
                    "    }\n" +
                    "}\n" +
                    "\n" +
                    "printf(\"----xxx---- \\n\");\n" +
                    "fflush(stdout);*/" +
            "    // free resources!!!\n" +
            "    free(startPositions);\n" +
            "    free(endPositions);\n" +
            "    map.deleteHashtable();\n" +
            "\n" +
            "    // return the variables required for consistent logic with the Java part\n" +
            "    arrayHelper[0] = openingWindowsPointer * ht_node_size;\n" +
            "    arrayHelper[1] = closingWindowsPointer * ht_node_size;\n" +
            "    arrayHelper[2] = pendingWindowsPointer * ht_node_size;\n" +
            "    arrayHelper[3] = completeWindowsPointer * sizeof(output_tuple_t);\n" +
            "    arrayHelper[4] = openingWindows;\n" +
            "    arrayHelper[5] = closingWindows;\n" +
            "    arrayHelper[6] = pendingWindows;\n" +
            "    arrayHelper[7] = completeWindows;\n" +
            "\n" +
            "    return 0;\n" +
            "}\n" +
            "\n";
    }

    public static String getMergeFunction_p1(String templateTypes, String templateTypesForHashTable) {
        return
            "JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_optimisedAggregateHashTables\n" +
            "  (JNIEnv * env, jobject obj,\n" +
            "  jobject buff1, jint start1, jint end1,\n" +
            "  jobject buff2, jint start2, jint end2,\n" +
            "  jint keyLength, jint valueLength, jint intermediateTupleSize, jint mapSize,\n" +
            "  jint numberOfValues,\n" +
            "  jint outputSchemaPad, jboolean pack,\n" +
            "  jobject openingWindowsBuffer, jobject completeWindowsBuffer, jint resultBufferPosition) {\n" +
            "\n" +
            "    (void) obj;\n" +
            "\n" +
            "    // Input Buffers\n" +
            "    ht_node"+templateTypes+" *buffer1= (ht_node"+templateTypes+" *) env->GetDirectBufferAddress(buff1);\n" +
            "    ht_node"+templateTypes+" *buffer2= (ht_node"+templateTypes+" *) env->GetDirectBufferAddress(buff2);\n" +
            "    hashtable"+templateTypesForHashTable+" map1 (buffer1, MAP_SIZE);\n" +
            "    hashtable"+templateTypesForHashTable+" map2 (buffer2, MAP_SIZE);\n" +
            "    //int len = env->GetDirectBufferCapacity(buffer);\n" +
            "    //const int inputSize = len/32; // 32 is the size of the tuple here\n" +
            "\n" +
            "    // temp variables for the merging\n" +
            "    int posInB2;\n" +
            "    bool isFound;\n" +
            "    int resultIndex = (pack) ? resultBufferPosition/sizeof(output_tuple_t) : resultBufferPosition; //sizeof(ht_node);\n" +
            "    int posInRes = 0;\n" +
            "    int *pendingValidPos; // keep the valid items from the pending window, as they will be reused by other opening windows!\n" +
            "    int pendingIndex = 0;\n" +
            "\n" +
            "    // Output Buffers\n" +
            "    ht_node"+templateTypes+" *openingWindowsResults;\n" +
            "    output_tuple_t *completeWindowsResults; // the results here are packed\n" +
            "\n" +
            "    if (!pack) {\n" +
            "        openingWindowsResults = (ht_node"+templateTypes+" *) env->GetDirectBufferAddress(openingWindowsBuffer);\n" +
            "        pendingValidPos = (int *) malloc(MAP_SIZE * sizeof(int));\n" +
            "    } else {\n" +
            "        completeWindowsResults = (output_tuple_t *) env->GetDirectBufferAddress(completeWindowsBuffer);\n" +
            "    }\n" +
            "\n" +
            "    /* Iterate over tuples in first table. Search for key in the hash table.\n" +
            "     * If found, merge the two entries. */\n" +
            "    for (int idx = start1; idx < end1; idx++) {\n" +
            "\n" +
            "        if (buffer1[idx].status != 1) /* Skip empty slot */\n" +
            "            continue;\n" +
            "\n" +
            "        // search in the correct hashtable by moving the respective pointer\n" +
            "        isFound = map2.get_index(&buffer2[start2], &buffer1[idx].key, posInB2); //ht_get_index(&buffer2[start2], buffer1[idx].key, MAP_SIZE, posInB2);\n" +
            "        if (posInB2 < 0) {\n" +
            "            printf (\"error in C: open-adress hash table is full \\n\");\n" +
            "            exit(1);\n" +
            "        }\n" +
            "        posInB2+=start2; // get the correct index;\n" +
            "\n" +
            "        if (!isFound) {\n" +
            "            if (pack) {\n" +
            "                /* Copy tuple based on output schema */\n" +
            "\n";
    }
    public static String mergeFunction_p2 =
            //"                /* Put timestamp */\n" +
            //"                completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;\n" +
            //"                /* Put key */\n" +
            //"                completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;\n" +
            //"                /* TODO: Put value(s) */\n" +
            //"                for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                    completeWindowsResults[resultIndex].count = buffer1[idx].counter;\n" +
            //"                }\n" +
            "                // Do I need padding here ???\n" +
            "\n" +
            //"                resultIndex++;\n" +
            "            } else {\n" +
            "                // we operating already on the hashtable,\n" +
            "                // as b1 and openingWindowsResults are the same, so we don't need to copy anything!\n" +
            "\n" +
            "                /* Create a new hash table entry */\n" +
            "                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, MAP_SIZE, posInRes);\n" +
            "                if (posInRes < 0 || isFound) {\n" +
            "                    printf (\"error in C: failed to insert new key in intermediate hash table \\n\");\n" +
            "                    exit(1);\n" +
            "                }*/\n" +
            //"                /* Mark occupancy */\n" +
            //"                //openingWindowsResults[posInRes + resultIndex].status = 1;\n" +
            //"                /* Put timestamp */\n" +
            //"                //openingWindowsResults[posInRes + resultIndex].timestamp = buffer1[idx].timestamp;\n" +
            //"                /* Put key and TODO: value(s) */\n" +
            //"                /*openingWindowsResults[posInRes + resultIndex].key = buffer1[idx].key;\n" +
            //"                for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                    openingWindowsResults[posInRes + resultIndex].counter = buffer1[idx].counter;\n" +
            //"                }*/\n" +
            //"                /* Put count */\n" +
            "            }\n" +
            "        } else { // merge values based on the number of aggregated values and their types!\n" +
            "            // TODO: now is working only for count!\n" +
            "            if (pack) {\n" +
            "                /* Copy tuple based on output schema */\n" +
            "\n";
            //"                /* Put timestamp */\n" +
            //"                completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;\n" +
            //"                /* Put key */\n" +
            //"                completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;\n" +
            //"                /* TODO: Put value(s) */\n" +
            //"                for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                    // TODO: check for types\n" +
            //"\n" +
            //"                    completeWindowsResults[resultIndex].count = buffer1[idx].counter + buffer2[posInB2].counter;\n" +
            //"                }\n" +

    public static String mergeFunction_p3 =
            "                // Do I need padding here ???\n" +
            "\n" +
            //"                resultIndex++;\n" +
            "            } else {\n" +
            "                /* Create a new hash table entry */\n" +
            "                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, MAP_SIZE, posInRes);\n" +
            "\n" +
            "                if (posInRes < 0 || isFound) {\n" +
            "                    printf (\"error in C: failed to insert new key in intermediate hash table \\n\");\n" +
            "                    exit(1);\n" +
            "                }*/\n" +
            "\n" +
            "                /* Mark occupancy */\n" +
            "                //openingWindowsResults[idx].status = 1;\n" +
            "                /* Put timestamp */\n" +
            "                //openingWindowsResults[idx].timestamp = buffer1[idx].timestamp;\n" +
            "                /* Put key and TODO: value(s) */\n" +
            "                //openingWindowsResults[idx].key = buffer1[idx].key;\n";
            //"                for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                    // TODO: check for types\n" +
            //"\n" +
            //"                    openingWindowsResults[idx].counter += buffer2[posInB2].counter; //buffer1[idx].counter ;\n" +
            //"                }\n" +
    public static String mergeFunction_p4 =
            "                /* Put count */\n" +
            "            }\n" +
            "            // Unmark occupancy in second buffer\n" +
            "            buffer2[posInB2].status = 0;\n" +
            "            // if it is pending, keep the position in order to restore it later\n" +
            "            if (!pack) {\n" +
            "                pendingValidPos[pendingIndex++] = posInB2;\n" +
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    /* Iterate over the remaining tuples in the second table. */\n" +
            "    for (int idx = start2; idx < end2; idx ++) {\n" +
            "\n" +
            "        if (buffer2[idx].status != 1) /* Skip empty slot */\n" +
            "            continue;\n" +
            "\n" +
            "        if (pack) {\n" +
            "            /* Copy tuple based on output schema */\n" +
            "\n";
            //"            /* Put timestamp */\n" +
            //"            completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;\n" +
            //"            /* Put key */\n" +
            //"            completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;\n" +
            //"            /* TODO: Put value(s) */\n" +
            //"            for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                completeWindowsResults[resultIndex].count = buffer1[idx].counter;\n";
            //"            }\n"
    public static String mergeFunction_p5 =
            "            // Do I need padding here ???\n" +
            "\n" +
            //"            resultIndex++;\n" +
            "        } else {\n" +
            "            /* Create a new hash table entry */\n" +
            "            isFound = map2.get_index(&openingWindowsResults[resultIndex], &buffer2[idx].key, posInRes); //ht_get_index(&openingWindowsResults[resultIndex], buffer2[idx].key, MAP_SIZE, posInRes);\n" +
            "\n" +
            "            if (posInRes < 0 || isFound) {\n" +
            "\n" +
            "                printf (\"error in C: failed to insert new key in intermediate hash table \\n\");\n" +
            "                exit(1);\n" +
            "            }\n" +
            "\n";

    public static String getMergeFunction_p6(String templateTypes) {
        return
            //"            /* Mark occupancy */\n" +
            //"            openingWindowsResults[posInRes + resultIndex].status = 1;\n" +
            //"            /* Put timestamp */\n" +
            //"            openingWindowsResults[posInRes + resultIndex].timestamp = buffer2[idx].timestamp;\n" +
            //"            /* Put key and TODO: value(s) */\n" +
            //"            openingWindowsResults[posInRes + resultIndex].key = buffer2[idx].key;\n" +
            //"            for (int i = 0; i < numberOfValues; i++) {\n" +
            //"                openingWindowsResults[posInRes + resultIndex].counter = buffer2[idx].counter;\n" +
            //"            }\n" +
            //"            /* Put count */\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "\n" +
            "    if (!pack) {\n" +
            "\n" +
            "        resultIndex += MAP_SIZE;\n" +
            "        // Remark occupancy in second buffer if it is a pending window\n" +
            "        for (int i = 0; i < pendingIndex; i++) {\n" +
            "            buffer2[pendingValidPos[i]].status = 1;\n" +
            "        }\n" +
            "        free(pendingValidPos);\n" +
            "    }\n" +
            "\n" +
            "    // return the variables required for consistent logic with the Java part\n" +
            "    return (pack) ? resultIndex*sizeof(output_tuple_t) : (resultIndex+MAP_SIZE)*sizeof(ht_node"+templateTypes+") ;\n" +
            "}\n" +
            "\n";
    }

    public static String getMergeHelperFunction(String templateTypes) {
        return
            "JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_getIntermediateTupleSize\n" +
            "  (JNIEnv *env, jobject obj) {\n" +
            "    (void) obj;\n" +
            "    (void) env;\n" +
            "    return sizeof(ht_node"+templateTypes+");\n" +
            "}\n" +
            "\n";
    }

    public static String getChangeTimestampsFunction() {
        return
            "JNIEXPORT jlong JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_changeTimestamps\n" +
            "  (JNIEnv *env, jobject obj, jobject buffer, jint startPos, jint endPos, jint dataLength, jlong timestamp) {\n" +
            "\n" +
            "    (void) obj;\n" +
            "\n" +
            "    // Input Buffer\n" +
            "    input_tuple_t *inputBuffer= (input_tuple_t *) env->GetDirectBufferAddress(buffer);\n" +
            "\n" +
            "    int start = startPos/sizeof(input_tuple_t);\n" +
            "    int end = endPos/sizeof(input_tuple_t);\n" +
            "    int changeOffset = dataLength/sizeof(input_tuple_t);\n" +
            "\n" +
            "    int temp = 0;\n" +
            "    //timestamp++;\n" +
            "\n" +
            "    //printf(\"%d, %d, %ld \\n\", start, end, timestamp);\n" +
            "    //fflush(stdout);\n" +
            "    for (int i = start; i < end; i++) {\n" +
            "        if (temp%changeOffset==0)\n" +
            "            timestamp++;\n" +
            "        inputBuffer[i].timestamp = timestamp;\n" +
            "        temp++;\n" +
            "    }\n" +
            "\n" +
            "    return timestamp;\n" +
            "\n" +
            "}\n";
    }
}
