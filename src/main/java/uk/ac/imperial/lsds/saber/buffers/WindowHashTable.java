package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.SystemConf;

public class WindowHashTable {
	
	public static final int TSTAMP_OFFSET = 8;
	public static final int KEY_OFFSET = 16;
	
	/* Note that the following value must be a power of 2. */
	public static final int N = SystemConf.HASH_TABLE_SIZE;
	
	ByteBuffer content;
	
	int id;
	
	long autoIndex;
	
	int keyLength, valueLength, tupleLength;
	
	int slots = 0;
	
	public int numberOfSlots () {
		return slots;
	}
	
	public boolean isInitialised () {
		return (slots != 0);
	}
	
	public WindowHashTable (int id, long autoIndex) {
		
		content = ByteBuffer.allocate(N);
		
		for (int i = 0; i < content.capacity(); ++i)
			content.put((byte) 0);
		
		this.id = id;
		this.autoIndex = autoIndex;
		
		keyLength = valueLength = tupleLength = 0;
		slots = 0;
	}
	
	public int getId () {
		return id;
	}
	
	public void setId (int id) {
		this.id = id;
	}
	
	public long getAutoIndex () {
		return autoIndex;
	}
	
	public ByteBuffer getBuffer () {
		return content;
	}
	
	public String toString () {
		return String.format("[WindowHashTable %03d pool-%02d %6d bytes/tuple %6d slots]", 
				autoIndex, id, tupleLength, slots);
	}
	
	public int getTimestampOffset (int idx) {
		return (idx + TSTAMP_OFFSET);
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
	
	public void setTupleLength (int keyLength, int valueLength) {
		
		this.keyLength = keyLength;
		this.valueLength = valueLength;
		/* +occupancy (8), +timestamp (8), +count (4): +20 bytes */
		tupleLength = 
			1 << (32 - Integer.numberOfLeadingZeros((keyLength + valueLength + 20) - 1));
		slots = N / tupleLength;
		/* System.out.println(String.format("[DBG] [WindowHashTable] tuple is %d bytes long; %d slots", 
		 * tupleLength, slots)); */
	}
	
	/* Linear scan of the hash table */
	private int getNext (int h) {
		return (h & (slots - 1)) * tupleLength;
	}
	
	public int getIndex (byte [] tupleKey, boolean [] found) {
		int h = JenkinsHashFunctions.hash(tupleKey, 1) & (slots - 1);
		int idx = h * tupleLength;
		int attempts = 0;
		while (attempts < slots) {
			byte mark = content.get(idx);
			if (mark == 1) {
				if (compare (tupleKey, idx) == 0) {
					found[0] = true;
					return idx;
				}
			} else
			if (mark == 0) { 
				found[0] = false;
				return idx;
			}
			attempts ++;
			idx = getNext (++h);
		}
		return -1;
	}
	
	private int compare (byte [] tupleKey, int offset) {
		int n = (offset + KEY_OFFSET) + tupleKey.length;
		for (int i = (offset + KEY_OFFSET), j = 0; i < n; i++, j++) {
			byte v1 = this.content.get(i);
			byte v2 = tupleKey[j];
			if (v1 == v2)
				continue;
			if (v1 < v2)
				return -1;
			return +1;
		}
		return 0;
	}
	
	public void clear () {
		for (int i = 0; i < N; i += tupleLength) {
			content.put(i, (byte) 0);
		}
		slots = 0;
		keyLength = valueLength = tupleLength = 0;
		content.clear();
	}
	
	public void release () {
		clear();
		WindowHashTableFactory.free(this);
	}
}
