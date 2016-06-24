package uk.ac.imperial.lsds.saber.devices;

import java.lang.reflect.Field;

import sun.misc.Unsafe;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;

@SuppressWarnings("restriction")
public class HeapMemoryManager {
	
	private static Unsafe getUnsafeMemory () {
		try {
			Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			return (Unsafe) theUnsafe.get (null);
			
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}
	
	private int Q;
	private int N;
	
	private IQueryBuffer [][]  inputs;
	private IQueryBuffer [][] outputs;
	
	private int  [][] start; /* Start and end pointers for inputs, since */
	private int  [][]   end; /* they are positioned in a circular buffer */
	
	private static Unsafe theUnsafe;
	
	public HeapMemoryManager (int Q, int N) {
		
		this.Q = Q;
		this.N = N;
		
		 inputs = new IQueryBuffer [Q][N];
		outputs = new IQueryBuffer [Q][N];
		
		start = new int [Q][N];
		  end = new int [Q][N];
		
		theUnsafe = getUnsafeMemory ();
	}
	
	private void checkQueryIndexBounds (int index) {
		if (index < 0 || index >= Q)
			throw new ArrayIndexOutOfBoundsException ("error: invalid query id");
	}
	
	public void checkBufferIndexBounds (int index) {
		if (index < 0 || index >= N)
			throw new ArrayIndexOutOfBoundsException ("error: invalid buffer id");
	}
	
	public void setInputBuffer (int qid, int ndx, IQueryBuffer buffer) {
		setInputBuffer (qid, ndx, buffer, 0, buffer.capacity());
	}
	
	public void setInputBuffer (int qid, int bid, IQueryBuffer buffer, int startP, int endP) {
		/* Check bounds */
		checkQueryIndexBounds  (qid);
		checkBufferIndexBounds (bid);
		
		inputs [qid][bid] = buffer;
		start  [qid][bid] = startP;
		end    [qid][bid] =   endP;
	}
	
	public void setOutputBuffer (int qid, int bid, IQueryBuffer output) {
		/* Check bounds */
		checkQueryIndexBounds  (qid);
		checkBufferIndexBounds (bid);
		
		outputs [qid][bid] = output;
	}
	
	public IQueryBuffer getOutputBuffer (int qid, int bid) {
		/* Check bounds */
		checkQueryIndexBounds  (qid);
		checkBufferIndexBounds (bid);
		
		return this.outputs [qid][bid];
	}
	
	public void inputDataMovementCallback (int qid, int bid, long address, int size) {
		/* Check bounds */
		checkQueryIndexBounds  (qid);
		checkBufferIndexBounds (bid);
		
		int length = end[qid][bid] - start[qid][bid];
		if (length > size)
			throw new ArrayIndexOutOfBoundsException(
				String.format("error: data movement of buffer [%d][%d] is out of bounds (length=%d size=%d)", 
				qid, bid, length, size));
		
		// System.out.println(String.format("[DBG] write %10d bytes from buffer [%d][%d] to @%d (dst buffer size is %d bytes)", 
		//	length, qid, bid, address, size));
		
		int startP = start[qid][bid];
		
		int endP  = end[qid][bid];
		if (endP >= inputs[qid][bid].capacity())
			endP  = inputs[qid][bid].normalise((long) endP);
		
		if (endP > startP) {
			theUnsafe.copyMemory (
				inputs[qid][bid].array(), 
				Unsafe.ARRAY_BYTE_BASE_OFFSET + startP, 
				null, 
				address, 
				length
			);
		} else {
			/* Copy in two parts: from start to `capacity - start` (left) 
			 * and from 0 to `end` */
			
			int leftP  = inputs[qid][bid].capacity() - startP;
			
			// System.out.println(String.format("[DBG] copy in two parts: part I %10d bytes part II %10d bytes", leftP, endP));
			
			if (leftP < 0) {
				throw new RuntimeException (String.format("error: start %d end %d (%d) capacity %d", 
						startP, end[qid][bid], endP, inputs[qid][bid].capacity()));
			}
			
			theUnsafe.copyMemory (
				inputs[qid][bid].array(), 
				Unsafe.ARRAY_BYTE_BASE_OFFSET + startP, 
				null, 
				address, 
				leftP
			);
			
			theUnsafe.copyMemory (
				inputs[qid][bid].array(), 
				Unsafe.ARRAY_BYTE_BASE_OFFSET + 0, 
				null, 
				address + leftP, 
				endP
			);
		}
	}
	
	public void outputDataMovementCallback (int qid, int bid, long address, int size) {
		/* Check bounds */
		checkQueryIndexBounds  (qid);
		checkBufferIndexBounds (bid);
		
		int length = outputs[qid][bid].capacity();
		if (length < size) {
			throw new ArrayIndexOutOfBoundsException(String.format("error: data movement to buffer [%d][%d] is out of bounds", 
				qid, bid));
		}
		
		// System.out.println(String.format("[DBG] read %10d bytes from @%d to buffer [%d][%d] (dst buffer size is %d bytes)", 
		//		size, address, qid, bid, length));
		
		if (size > 0) {
			theUnsafe.copyMemory(
				null, 
				address, 
				outputs[qid][bid].array(), 
				Unsafe.ARRAY_BYTE_BASE_OFFSET, 
				size
			);
		}
		
		/* Set the position of the output buffer */
		outputs[qid][bid].position(size);
	}
}
