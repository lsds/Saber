package uk.ac.imperial.lsds.saber.cql.operators.gpu.code.generators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class KernelGenerator {
	
	public static int getVectorSize (int size, int [] b) {
		
		int bytes = 16;
		
		while (size < bytes)
			bytes /= 2;
		
		if (size % bytes != 0)
			throw new IllegalArgumentException
				(String.format("error: size (%d) is not a multiple of the vector `uchar%d`", size, bytes));
		
		b[0] = bytes;
		return size / bytes;
	}
	
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
	
	private static String getHeader (ITupleSchema input1, ITupleSchema input2, ITupleSchema output) {
		
		boolean isJoin = (input2 != null);
		
		StringBuilder b = new StringBuilder ();
		
		b.append("#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics: enable\n");
		b.append("#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics: enable\n");
		b.append("#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable\n");
		b.append("#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics: enable\n");
		b.append("#pragma OPENCL EXTENSION cl_khr_fp64: enable\n");
		b.append("\n");
		
		b.append("#pragma OPENCL EXTENSION cl_khr_byte_addressable_store: enable\n");
		b.append("\n");
		
		b.append(String.format("#include \"%s/clib/templates/byteorder.h\"", SystemConf.SABER_HOME));
		b.append("\n");
		
		int [] bytes1 = new int [1]; 
		int [] bytes2 = new int [1];
		int [] bytes3 = new int [1];
		
		int input1VectorSize = getVectorSize (input1.getTupleSize(), bytes1);
		int outputVectorSize = getVectorSize (output.getTupleSize(), bytes3);
		
		if (isJoin) {
			int input2VectorSize = getVectorSize (input2.getTupleSize(), bytes2);
			
			b.append(String.format("#define S1_INPUT_VECTOR_SIZE %d\n", input1VectorSize));
			b.append(String.format("#define S2_INPUT_VECTOR_SIZE %d\n", input2VectorSize));
		} else {
			b.append(String.format("#define INPUT_VECTOR_SIZE %d\n", input1VectorSize));
		}
		b.append(String.format("#define OUTPUT_VECTOR_SIZE %d\n", outputVectorSize));
		b.append("\n");
		
		if (isJoin)
			b.append(getInputHeader (input1, "s1", bytes1[0]));
		else
			b.append(getInputHeader (input1, null, bytes1[0]));
		b.append("\n");
		
		if (isJoin) {
			b.append(getInputHeader (input2, "s2", bytes2[0]));
			b.append("\n");
		}
		
		b.append(getOutputHeader (output, bytes3[0]));
		b.append("\n");
		
		return b.toString();
	}
	
	private static String getInputHeader (ITupleSchema schema, String prefix, int vector) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("typedef struct {\n");
		/* The first attribute is always a timestamp */
		b.append("\tlong t;\n");
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
		
		b.append("typedef union {\n");
		if (prefix == null) {
			b.append("\tinput_tuple_t tuple;\n");
			b.append(String.format("\tuchar%d vectors[INPUT_VECTOR_SIZE];\n", vector));
			b.append("} input_t;\n");
		} else {
			b.append(String.format("\t%s_input_tuple_t tuple;\n", prefix));
			b.append(String.format("\tuchar%d vectors[%s_INPUT_VECTOR_SIZE];\n", vector, prefix.toUpperCase()));
			b.append(String.format("} %s_input_t;\n", prefix));
		}
		
		return b.toString();
	}
	
	private static String getOutputHeader (ITupleSchema schema, int vector) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append("typedef struct {\n");
		
		if (schema.getAttributeType(0) == PrimitiveType.LONG) {
			/* The first long attribute is assumed to be always a timestamp */
			b.append("\tlong t;\n");
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
		
		b.append("typedef union {\n");
		b.append("\toutput_tuple_t tuple;\n");
		b.append(String.format("\tuchar%d vectors[OUTPUT_VECTOR_SIZE];\n", vector));
		b.append("} output_t;\n");
		
		return b.toString();
	}
	
	private static String getWindowDefinition (WindowDefinition windowDefinition) {
		
		StringBuilder b = new StringBuilder();
		
		if (windowDefinition.isRangeBased())
			b.append("#define RANGE_BASED\n");
		else
			b.append("#define COUNT_BASED\n");
		
		b.append("\n");
		
		b.append(String.format("#define PANES_PER_WINDOW %dL\n", windowDefinition.numberOfPanes()));
		b.append(String.format("#define PANES_PER_SLIDE  %dL\n", windowDefinition.panesPerSlide()));
		b.append(String.format("#define PANE_SIZE        %dL\n", windowDefinition.getPaneSize()));
		
		return b.toString();
	}
	
	public static String getDummyOperator (String filename, ITupleSchema input, ITupleSchema output) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader(input, null, output)).append("\n");
		
		b.append(NoopKernelGenerator.getFunctor(input)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();
	}
	
	public static String getProjectionOperator (String filename, ITupleSchema input, ITupleSchema output, int depth) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader (input, null, output)).append("\n");
		
		b.append(ProjectionKernelGenerator.getFunctor (input, output, depth)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();
	}
	
	public static String getSelectionOperator (String filename, ITupleSchema input, ITupleSchema output, 
		
		IPredicate predicate, String customPredicate) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader (input, null, output)).append("\n");
		
		b.append(SelectionKernelGenerator.getFunctor (predicate, customPredicate)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();
	}
	
	public static String getThetaJoinOperator(String filename, ITupleSchema left, ITupleSchema right, ITupleSchema output,
			
		IPredicate predicate, String customPredicate, String customCopy) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader (left, right, output)).append("\n");
		
		b.append(ThetaJoinKernelGenerator.getFunctor (left, right, output, predicate, customPredicate, customCopy)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();	
	}
	
	public static String getReductionOperator (String filename, ITupleSchema input, ITupleSchema output, 
		
		WindowDefinition windowDefinition, AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader (input, null, output)).append("\n");
		
		b.append(getWindowDefinition(windowDefinition)).append("\n");
		
		b.append(ReductionKernelGenerator.getFunctor(output, aggregationTypes, aggregationAttributes)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();
	}
	
	public static String getAggregationOperator (String filename, ITupleSchema input, ITupleSchema output, 
		
		WindowDefinition windowDefinition, AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, 
		
		Expression [] groupByAttributes) {
		
		StringBuilder b = new StringBuilder ();
		
		b.append(getHeader (input, null, output)).append("\n");
		
		b.append(getWindowDefinition(windowDefinition)).append("\n");
		
		int   keyLength = 4 *     groupByAttributes.length;
		int valueLength = 4 * aggregationAttributes.length;
		
		int intermediateTupleSize = 
				1 << (32 - Integer.numberOfLeadingZeros((keyLength + valueLength + 20) - 1));
		
		String s = AggregationKernelGenerator.getIntermediateTupleDefinition 
				(groupByAttributes, aggregationAttributes.length, intermediateTupleSize);
		
		b.append(s).append("\n");
		
		b.append(AggregationKernelGenerator.getFunctor 
				(output, aggregationTypes, aggregationAttributes, groupByAttributes, intermediateTupleSize)).append("\n");
		
		b.append(load(filename)).append("\n");
		
		return b.toString();
	}
}
