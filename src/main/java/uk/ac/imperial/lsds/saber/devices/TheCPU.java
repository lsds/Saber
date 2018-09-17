package uk.ac.imperial.lsds.saber.devices;

import uk.ac.imperial.lsds.saber.SystemConf;

import java.nio.ByteBuffer;

public class TheCPU {

	// set this to use the correct library
	private static final String cpuLibrary = /*SystemConf.SABER_HOME*/
			"/home/george/idea/workspace/row_saber/Saber" + "/clib/libCPU.so";
	
	static {
		try {
			System.load (cpuLibrary);
		} catch (final UnsatisfiedLinkError e) {
			System.err.println("error: failed to load CPU library from " + cpuLibrary);
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
	public native int optimisedDistinct (ByteBuffer buffer, int bufferSize, int bufferStartPointer, int bufferEndPointer,
											ByteBuffer openingWindowsBuffer, ByteBuffer closingWindowsBuffer,
											ByteBuffer pendingWindowsBuffer, ByteBuffer completeWindowsBuffer,
											ByteBuffer openingWindowsStartPointers, ByteBuffer closingWindowsStartPointers,
											ByteBuffer pendingWindowsStartPointers, ByteBuffer completeWindowsStartPointers,
											long streamStartPointer, long windowSize, long windowSlide, long windowPaneSize,
											int openingWindowsPointer, int closingWindowsPointer, int pendingWindowsPointer,
											int completeWindowsPointer,
										 	ByteBuffer arrayHelper);
}
