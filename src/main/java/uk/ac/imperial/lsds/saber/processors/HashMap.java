package uk.ac.imperial.lsds.saber.processors;

import java.util.Arrays;

import uk.ac.imperial.lsds.saber.buffers.JenkinsHashFunctions;

public class HashMap {
	
	/* Note that the following value must be a power of two (see `hash`). */
	private int _SIZE = 1024;
				
	private class ThreadMapNode {
		
		public byte [] tupleKey;
		public int value;
		
		public ThreadMapNode next;
		
		public ThreadMapNode (byte [] tupleKey, int value, ThreadMapNode next) {
			
			this.tupleKey = tupleKey;
			this.value = value;
			this.next = next;
		}
	}
	
	ThreadMapNode [] content;
	int size = 0;
	int nextValue = 0;

	public HashMap () {
		content = new ThreadMapNode [_SIZE];
		for (int i = 0; i < content.length; i++)
			content[i] = null;
	}		
	
	public int register (byte [] tupleKey) {
		int idx;
		idx = get (tupleKey);
		if (idx < 0)
			idx = put (tupleKey, 0);
		return idx;
	}
	
	public int register (byte [] tupleKey, int value) {
		int idx;
		idx = get (tupleKey);
		if (idx < 0)
			idx = put (tupleKey, value);
		return idx;
	}
	
	public int size () {
		return this.size;
	}
		
	private int put (byte [] tupleKey, int value) {
		/* Lookup element hash code in the table.
		 *  Ideally, there is no chaining. 
		 */
		int h = JenkinsHashFunctions.hash(tupleKey, 1) & (_SIZE - 1);
		//System.out.println("key: "+ tupleKey[15]+" index: " + h);
		ThreadMapNode q = content[h];
		ThreadMapNode p = new ThreadMapNode(tupleKey, value, null);
		if (q == null) {
			content[h] = p;
			size++;
		} else {
			//System.out.println("duplicate");
/*			System.err.println(String.format("warning: chaining entry for thread %d in ThreadMap", tupleKey.toString()));*/
			while (q.next != null)
				q = q.next;
			q.next = p;
			size++;
		}
		return p.value;
	}
	
	public int get (byte [] tupleKey) {
		int h = JenkinsHashFunctions.hash(tupleKey, 1) & (_SIZE - 1);
		ThreadMapNode q = content[h];
		if (q == null)
			return -1;
		
		if (Arrays.equals(q.tupleKey, tupleKey))
			return q.value;
		
		while (!Arrays.equals(q.tupleKey, tupleKey) && q.next != null) {
			q = q.next;
			if (q.tupleKey.equals(tupleKey))
				return q.value;
		}
		return -1;
	}
}
