package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.SystemConf;

public class RelationalTableQueryBuffer implements IQueryBuffer {
	
	private static final int _default_capacity = SystemConf.RELATIONAL_TABLE_BUFFER_SIZE;
	
	private int id;
	
	private boolean isDirect = false;
	
	private byte [] data = null;
	
	private int size;
	
	private final PaddedAtomicLong start;
	private final PaddedAtomicLong end;
	
	private long mask;
	private long wraps;
	
	private ByteBuffer buffer;
	
	private AtomicLong bytesProcessed;
	
	private PaddedLong h;
	
	private static int nextPowerOfTwo (int size) {
		
		return 1 << (32 - Integer.numberOfLeadingZeros(size - 1));
	}
	
	public RelationalTableQueryBuffer (int id) {
		
		this (id, _default_capacity, false);
	}
	
	public RelationalTableQueryBuffer (int id, int capacity) {
		
		this (id, capacity, false);
	}
	
	public RelationalTableQueryBuffer (int id, int _size, boolean isDirect) {
		
		if (_size <= 0)
			throw new IllegalArgumentException("error: buffer size must be greater than 0");
		
		this.size = nextPowerOfTwo (_size); /* Set size to the next power of 2 */
		
		if (Integer.bitCount(this.size) != 1)
			throw new IllegalArgumentException("error: buffer size must be a power of 2");
		
		System.out.println(String.format("[DBG] q %d %d bytes", id, size));
		
		this.isDirect = isDirect;
		this.id = id;
		
		start = new PaddedAtomicLong (0L);
		end   = new PaddedAtomicLong (0L);
		
		mask = this.size - 1;
		
		wraps = 0;
		
		bytesProcessed = new AtomicLong (0L);
		
		h = new PaddedLong (0L);
		
		if (! this.isDirect) {
			
			data   = new byte [this.size];
			buffer = ByteBuffer.wrap(data);
			
		} else {
			
			buffer = ByteBuffer.allocateDirect(this.size);
		}		
	}	
	
	public int getInt (int offset) {
		
		return buffer.getInt(normalise(offset));
	}
	
	public float getFloat (int offset) {
		
		return buffer.getFloat(normalise(offset));
	}
	
	public long getLong (int offset) {
		
		return buffer.getLong(normalise(offset));
	}
	
	public long getMSBLongLong (int offset) {

		return buffer.getLong(normalise(offset));
	}
	
	public long getLSBLongLong (int offset) {

		return buffer.getLong(normalise(offset) + 8);
	}
	
	public byte [] array () {
		
		if (isDirect)
			throw new UnsupportedOperationException("error: cannot get byte array from a direct buffer");
		
		return data;
	}
	
	public ByteBuffer getByteBuffer () {
		return buffer;
	}
	
	public int capacity () {
		return size;
	}
	
	public int remaining () {
		
		long tail =   end.get();
		long head = start.get();
		
		if (tail < head)
			return (int) (head - tail);
		else
			return size - (int) (tail - head);
	}
	
	public boolean hasRemaining () {
		return (remaining() > 0);
	}
	
	public boolean hasRemaining (int length) {
		
		return (remaining() >= length);
	}
	
	public int limit () {
		return size;
	}
	
	public void close () {
		return;
	}
	
	public void clear () {
		return;
	}
	
	public int putInt (int value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}
	
	public int putInt (int index, int value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}
	
	public int putFloat (float value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}
	
	public int putFloat (int index, float value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}
	
	public int putLong (long value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}
	
	public int putLong (int index, long value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a relational table buffer");
	}

	public int putLongLong (long msbValue, long lsbValue) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putLongLong (int index, long msbValue, long lsbValue) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int put (byte [] values) {
		
		return put (values, values.length);
	}
	
	public int put (byte [] values, int length) {
		// allow only the first time to add data
		if (end.get() != 0) {
			//throw new UnsupportedOperationException("error: cannot add new data in the relational table");
			//buffer.rewind();
			return normalise (end.get());
		}
		
		if (isDirect)
			throw new UnsupportedOperationException("error: cannot put array to a direct buffer");
	
		if (values == null || length <= 0)
			throw new NullPointerException ("error: cannot put null to a relational table buffer");

		final long _end = end.get();
		final long wrapPoint = (_end + length - 1) - size;
		if (h.value <= wrapPoint) {
			h.value = start.get();
			if (h.value <= wrapPoint) {
				debug (); 
				return -1;
			}
		}
		
		int index = normalise (_end);
	
		System.arraycopy(values, 0, data, index, length);
	
		int p = normalise (_end + length);
		if (p <= index)
			wraps ++;
	 	buffer.position(p); 
		end.lazySet(_end + length);
	 	debug (); 
		return index;			
	}
	
	public int put (byte [] values, int offset, int length) {
		
		if (offset > 0)
			throw new UnsupportedOperationException("error: cannot put byte array with an offset of non-zero to a relational table buffer");
		
		return put (values, length);
	}
	
	public int put (IQueryBuffer buffer) {
		
		return put (buffer.array());
	}
	
	public int put (IQueryBuffer buffer, int offset, int length) {
		
		if (offset > 0)
			throw new UnsupportedOperationException("error: cannot put byte array with an offset of non-zero to a relational table buffer");
		
		return put (buffer.array(), length);
	}
	
	public void free (int offset) {
		final long _start = start.get();
		final int index = normalise (_start);
		final int bytes;
		/* Measurements */
		if (offset <= index)
			bytes = size - index + offset + 1;
		else
			bytes = offset - index + 1;
		
		/* debug(); */
		
		bytesProcessed.addAndGet(bytes);
		
		/* Set new start pointer */
		start.lazySet(_start + bytes);
	}
	
	public void release () {
		return ;
	}	
	
	public int normalise (long index) {
		
		return (int) (index & mask); /* Iff. size is a power of 2 */
	}
		
	public long getBytesProcessed () {
		
		return bytesProcessed.get(); 
	}
	
	public long getWraps () {
		
		return wraps;
	}
	
	public void debug () {
		long head = start.get();
		long tail = end.get();
		int remaining = (tail < head) ? (int) (head - tail) : (size - (int) (tail - head));
		System.out.println(
		String.format("[DBG] %s start %20d [%20d] end %20d [%20d] %7d wraps %20d bytes remaining", 
		" ", normalise(head), head, normalise(tail), tail, getWraps(), remaining));
	}

	public void appendBytesTo (int offset, int length, IQueryBuffer dst) {
		
		if (isDirect || dst.isDirect())
			throw new UnsupportedOperationException("error: cannot append bytes from/to a direct buffer");
		
		int start = normalise(offset);
		
		dst.put(data, start, length);
	}
	
	public void appendBytesTo (int start, int end, byte [] dst) {
		
		if (isDirect)
			throw new UnsupportedOperationException("error: cannot append bytes to a byte array from a direct buffer");
		
		if (end > start) {
			
			System.arraycopy (data, start, dst, 0, end - start);
			
		} else { /* Copy in two parts */
			
			System.arraycopy (data, start, dst,            0, size - start);
			System.arraycopy (data,     0, dst, size - start,          end);
		}
	}
	
	public int position () {
		
		throw new UnsupportedOperationException("error: cannot get the position of a relational table buffer");
	}

	public void position(int index) {
		
		throw new UnsupportedOperationException("error: cannot set the position of a relational table buffer");
	}
	
	public boolean isDirect () {
		
		return this.isDirect;
	}
	
	public int getBufferId () {
		
		return this.id;
	}	
}
