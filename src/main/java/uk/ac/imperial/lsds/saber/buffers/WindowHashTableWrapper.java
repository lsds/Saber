package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.SystemConf;

public class WindowHashTableWrapper {
	
	public static final int TSTAMP_OFFSET = 8;
	public static final int KEY_OFFSET = 16;
	
	/* Note that the following value must be a power of 2. */
	public static final int N = SystemConf.HASH_TABLE_SIZE;
	
	ByteBuffer content;
	
	int start, end;
	
	int keyLength, valueLength, tupleLength;
	
	int slots;
	
	public int numberOfSlots () {
		return slots;
	}
	
	public boolean isInitialised () {
		return (slots != 0);
	}
	
	public WindowHashTableWrapper () {
		content = null;
		start = end = 0;
		keyLength = valueLength = tupleLength = 0;
		slots = 0;
	}
	
	public void configure (ByteBuffer content, int start, int end, int keyLength, int valueLength) {
		
		if ((end - start) != N)
			throw new IllegalStateException ("error: invalid window hash table content size");
		
		this.content = content;
		
		this.start = start;
		this.end = end;
		
		this.keyLength = keyLength;
		this.valueLength = valueLength;
		
		/* +occupancy (8), +timestamp (8), +count (4): +20 bytes */
		tupleLength = 
			1 << (32 - Integer.numberOfLeadingZeros((keyLength + valueLength + 20) - 1));
		
		slots = N / tupleLength;
	}
	
	public int getTimestampOffset (int idx) {
		return (idx + TSTAMP_OFFSET);
	}
	
	public ByteBuffer getContent () {
		return this.content;
	}
	
	public int getKeyOffset (int idx) {
		return (idx + KEY_OFFSET);
	}
	
	public int getValueOffset (int idx) {
		return (idx + KEY_OFFSET + keyLength);
	}
	
	public int getCountOffset (int idx) {
		return (idx + KEY_OFFSET + keyLength + valueLength);
	}
	
	public int getIntermediateTupleSize () {
		return tupleLength;
	}
	
	/* Linear scan of the hash table */
	public int getNext (int h) {
		return (start + (h & (slots - 1)) * tupleLength);
	}
	
	public int getIndex (byte [] array, int offset, int length, boolean [] found) {

		int h = JenkinsHashFunctions.hash(array, offset, length, 1) & (slots - 1);
		int idx = start + h * tupleLength;
		
		int attempts = 0;
		while (attempts < slots) {
			if (content.get(idx) == 1) {
				if (compare (array, offset, length, idx) == 0) {
					found[0] = true;
					return idx;
				}
			} else {
                found[0] = false;
                return idx;
            }
			attempts ++;
			idx = getNext (++h);
		}
		found[0] = false;
		return -1;
	}

	public int findIndex (byte [] array, int offset, int length, boolean [] found) {

		int h = JenkinsHashFunctions.hash(array, offset, length, 1) & (slots - 1);
		int idx = start + h * tupleLength;

		int attempts = 0;
		while (attempts < slots) {
			if (content.get(idx) == 1) {
				if (compare (array, offset, length, idx) == 0) {
					found[0] = true;
					return idx;
				}
			}
			attempts ++;
			idx = getNext (++h);
		}
		found[0] = false;
		return -1;
	}
	
	private int compare (byte [] array, int offset, int length, int index) {
		int n = (index + KEY_OFFSET) + length;
		for (int i = (index + KEY_OFFSET), j = offset; i < n; i++, j++) {
			byte v1 = content.get(i);
			byte v2 = array[j];
			if (v1 == v2)
				continue;
			if (v1 < v2)
				return -1;
			return +1;
		}
		return 0;	
	}
}
