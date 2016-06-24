package uk.ac.imperial.lsds.saber.devices;

import uk.ac.imperial.lsds.saber.SystemConf;

public class TheCPU {
	
	private static final String cpuLibrary = SystemConf.SABER_HOME + "/clib/libCPU.so";
	
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
}
