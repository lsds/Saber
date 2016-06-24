package uk.ac.imperial.lsds.saber;

public class Utils {

	public static long pack (long systemTimestamp, long tupleTimestamp) {
		return (systemTimestamp << 32) | tupleTimestamp;
	}
	
	public static int getSystemTimestamp (long value) {
		return (int) (value >> 32);
	}
	
	public static int getTupleTimestamp (long value) {
		return (int) value;
	}
}
