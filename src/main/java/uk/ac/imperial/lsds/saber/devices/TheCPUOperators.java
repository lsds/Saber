package uk.ac.imperial.lsds.saber.devices;

import uk.ac.imperial.lsds.saber.SystemConf;

import java.nio.ByteBuffer;

public class TheCPUOperators {
	
	private static final String cpuOperatorsLibrary = SystemConf.SABER_HOME + "/clib/libCPUOperators.so";
	
	static {
		try {
			System.load (cpuOperatorsLibrary);
		} catch (final UnsatisfiedLinkError e) {
			System.err.println("error: failed to load CPU library from " + cpuOperatorsLibrary);
			System.exit(1);
		}
	}
	
	private static final TheCPUOperators cpuOperatorsInstance = new TheCPUOperators();
	
	public static TheCPUOperators getInstance () { return cpuOperatorsInstance; }

	public native int byteBufferMethod (ByteBuffer buffer, ByteBuffer resultBuffer);
}
