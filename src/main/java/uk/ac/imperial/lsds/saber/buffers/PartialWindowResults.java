package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import uk.ac.imperial.lsds.saber.SystemConf;

public class PartialWindowResults {
	
	private static final int N = SystemConf.PARTIAL_WINDOWS;
	
	int pid; /* The worker thread that requested this object */
	
	IQueryBuffer buffer; /* The buffer that holds the partial window results */
	
	boolean empty;
	
	int size;
	public int count;
	
	//int [] startPointers;
	ByteBuffer startPointers;
	
	public PartialWindowResults (int pid) {
		
		this.pid = pid;
		
		buffer = null;
		size = 0;
		count = 0;
		// System.out.println("[DBG] new int []");
		//startPointers = new int [N];
        startPointers = ByteBuffer.allocateDirect(4 * N);
        startPointers.order( ByteOrder.LITTLE_ENDIAN);
		for (int i = 0; i < N; i++)
		    startPointers.putInt(i*4, -1);
			//startPointers[i] = -1;
		startPointers.position(0);
	}
	
	public IQueryBuffer getBuffer () {
		if (buffer == null)
			buffer = UnboundedQueryBufferFactory.newInstance();
		return buffer;
	}
	
	public boolean isEmpty () {
		if (buffer == null)
			return true;
		else
			return (count == 0 && size == 0);
	}
	
	public void release () {
		size = 0;
		for (int i = 0; i < count; i++)
            startPointers.putInt(i*4, -1);
            //startPointers[i] = -1;
        startPointers.position(0);
        count = 0;
		/* Release this object */
		PartialWindowResultsFactory.free (pid, this);
	}
	
	public void init () {
		this.size = 0;
		this.count = 0;
	}
	
	public void nullify () {
		if (buffer != null)
			buffer.clear();
		size = 0;
		for (int i = 0; i < count; i++)
            startPointers.putInt(i*4,-1);
            //startPointers[i] = -1;
		count = 0;
        startPointers.position(0);
	}
	
	public void increment () {
		if (buffer == null)
			throw new IllegalStateException ("error: buffer in partial window result is null");
		if (count >= N)
			throw new IndexOutOfBoundsException ("error: partial window result index out of bounds");
        startPointers.putInt(buffer.position());
        count++;
		//startPointers[count++] = buffer.position();
	}
	
	public int getStartPointer (int idx) {
		if (idx < 0 || idx >= count)
			throw new ArrayIndexOutOfBoundsException("error: partial window result index out of bounds");
		return startPointers.getInt(idx*4); //startPointers[idx];
	}
	
	public int numberOfWindows () {
		return count;
	}
	
	public void append (ByteBuffer windowBuffer) {
		if (count >= N)
			throw new IndexOutOfBoundsException ("error: partial window result index out of bounds");

        //startPointers[count++] = getBuffer().position();
        startPointers.putInt(getBuffer().position());
        count++;
		buffer.put(windowBuffer, 0, windowBuffer.position()*4);
	}
	
	public void append (PartialWindowResults closingWindows) {
		int offset = buffer.position();
		for (int wid = 0; wid < closingWindows.numberOfWindows(); ++wid) {
			if (count >= N)
				throw new IndexOutOfBoundsException ("error: partial window result index out of bounds");
			//startPointers[count++] = offset + closingWindows.getStartPointer(wid);
            startPointers.putInt(offset + closingWindows.getStartPointer(wid));
            count++;
		}
		buffer.put(closingWindows.getBuffer().getByteBuffer(), 0, closingWindows.getBuffer().position());
	}
	
	public void prepend (PartialWindowResults openingWindows, int start, int added, int windowSize) {
		
		int count_ = count + added;
		
		if (count_ >= N)
			throw new IndexOutOfBoundsException ("error: partial window result index out of bounds");
		
		if (buffer == null)
			getBuffer();
		
		/* Shift-down windows */
		
		int norm = openingWindows.getStartPointer(start);
		
		int end = start + added - 1;
		int offset = openingWindows.getStartPointer(end) - norm + windowSize;
		
		for (int i = count - 1; i >= 0; i--) {
			//startPointers[i + added] = startPointers[i] + offset;
            startPointers.putInt((i + added)*4, startPointers.getInt(i*4) + offset);
			int src = startPointers.getInt(i*4);
			int dst = startPointers.getInt((i + added)*4);
			buffer.position(dst);
			buffer.put(buffer, src, windowSize);
		}
		
		for (int i = 0, w = start; i < added; ++i, ++w) {
			//startPointers[i] = openingWindows.getStartPointer(w) - norm;
            startPointers.putInt(i*4, openingWindows.getStartPointer(w) - norm);
            int src = openingWindows.getStartPointer(w);
			int dst = startPointers.getInt(i*4);
			buffer.position(dst);
			buffer.put(openingWindows.getBuffer().getByteBuffer(), src, windowSize);
		}
		
		count = count_;
		buffer.position(count * windowSize);
	}

	/*
	 * This method is called when we fetch complete windows from the GPU.
	 * 
	 * The results are already packed. The pointers are not correct (all
	 * windows point to the same location -the end of the buffer- but it
	 * does not matter because we only append to this result set.
	 */
	public void setCount (int numberOfCompleteWindows, int position) {
		buffer.position(position);
		for (int i = 0; i < numberOfCompleteWindows; ++i) {
			if (count >= N)
				throw new IndexOutOfBoundsException ("error: partial window result index out of bounds");
			//startPointers[count++] = buffer.position();
            startPointers.putInt(buffer.position());
            count++;
		}
	}

	public ByteBuffer getStartPointers() {
	    return startPointers;
    }

    public void setCount (int count) {
        this.count = count;
        startPointers.position(count * 4);
    }
}
