package uk.ac.imperial.lsds.saber.devices;

import uk.ac.imperial.lsds.saber.SystemConf;

import java.nio.ByteBuffer;

public class TheCPU {

	// set this to use the correct library
	private static final String cpuLibrary = SystemConf.SABER_HOME + "/clib/libCPU.so";
    private static final String cpuGenLibrary = SystemConf.SABER_HOME + "/clib/libCPUGen.so";
	
	/*static {
		try {
			System.load (cpuLibrary);
		} catch (final UnsatisfiedLinkError e) {
			System.err.println("error: failed to load CPU library from " + cpuLibrary);
			System.exit(1);
		}
	}*/

	public void load () {
		try {
		    if (SystemConf.GENERATE)
		    	System.load (cpuGenLibrary);
		    else
                System.load (cpuLibrary);
		} catch (final UnsatisfiedLinkError e) {
			System.err.println("error: failed to load CPU");
			System.exit(1);
		}
	}

	private static final TheCPU cpuInstance = new TheCPU ();
	
	public static TheCPU getInstance () { return cpuInstance; }
	
	/* Thread affinity functions */
	public native int getNumCores ();
	public native int bind (int cpu);
	public native int unbind ();
	public native int getCpuId ();
	public native int init_clock (ByteBuffer result);
	public native int optimisedDistinct (ByteBuffer buffer, int bufferSize, int bufferStartPointer, int bufferEndPointer,
											ByteBuffer openingWindowsBuffer, ByteBuffer closingWindowsBuffer,
											ByteBuffer pendingWindowsBuffer, ByteBuffer completeWindowsBuffer,
											ByteBuffer openingWindowsStartPointers, ByteBuffer closingWindowsStartPointers,
											ByteBuffer pendingWindowsStartPointers, ByteBuffer completeWindowsStartPointers,
											long streamStartPointer, long windowSize, long windowSlide, long windowPaneSize,
											int openingWindowsPointer, int closingWindowsPointer, int pendingWindowsPointer,
											int completeWindowsPointer,
										 	ByteBuffer arrayHelper,
										 	int mapSize);
	public native int optimisedAggregateHashTables (ByteBuffer buffer1, int start1, int end1,
                                                    ByteBuffer buffer2, int start2, int end2,
                                                    int keyLength, int valueLength, int intermediateTupleSize, int mapSize,
                                                    int numberOfValues, /*ByteBuffer aggregationTypes,*/
                                                    int outputSchemaPad, boolean pack,
                                                    ByteBuffer openingWindowsBuffer, ByteBuffer completeWindowsBuffer,
                                                    int resultBufferPosition);

	public native long changeTimestamps (ByteBuffer result, int startPos, int endPos, int dataLength, long timestamp);


	public native int singleOperatorComputation (ByteBuffer buffer, int bufferStartPointer, int bufferEndPointer,
                                                 ByteBuffer openingWindowsBuffer, ByteBuffer closingWindowsBuffer,
                                                 ByteBuffer pendingWindowsBuffer, ByteBuffer completeWindowsBuffer,
                                                 ByteBuffer openingWindowsStartPointers, ByteBuffer closingWindowsStartPointers,
                                                 ByteBuffer pendingWindowsStartPointers, ByteBuffer completeWindowsStartPointers,
                                                 long streamStartPointer, int openingWindowsPointer, int closingWindowsPointer,
                                                 int pendingWindowsPointer, int completeWindowsPointer,
                                                 ByteBuffer arrayHelper);

	public native int getIntermediateTupleSize ();
}
