package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.SystemConf;

public class CircularQueryBuffer implements IQueryBuffer {
	
	private static final int _default_capacity = SystemConf.CIRCULAR_BUFFER_SIZE;
	
	private int id;
	
	private boolean isDirect = false;
	
	private byte [] data = null;
	
	public int size;
	
	private final PaddedAtomicLong start;
	private final PaddedAtomicLong end;
	
	private long mask;
	private long wraps;
	
	private ByteBuffer buffer;
	
	private AtomicLong bytesProcessed;
	
	private PaddedLong h;
	
	/* parallelize the work of dispatcher thread */
	public boolean isParallel;
	private CircularBufferWorker [] workers;
	public AtomicInteger isReady;
	public Latch isBufferFilledLatch;
	public long timestamp = 0;
	public long timestampBase = 0;
	public int globalIndex;
	public int globalLength;
	public int numberOfThreads;
	private boolean isFirst = true;
	public int counter = -1;
	public byte[] inputBuffer;
	public CountDownLatch latch; 
	/* 												*/

	private static int nextPowerOfTwo (int size) {
		
		return 1 << (32 - Integer.numberOfLeadingZeros(size - 1));
	}
	
	public CircularQueryBuffer (int id) {
		
		this (id, _default_capacity, false);
	}
	
	public CircularQueryBuffer (int id, int capacity) {
		
		this (id, capacity, false);
	}
	
	public CircularQueryBuffer (int id, int _size, boolean isDirect) { 
		
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

		/* parallelize the work of dispatcher thread */
		isParallel = true;
		if (this.id == 0) {
			numberOfThreads = 2;
			int coreToBind = 1;
			isReady = new AtomicInteger(-1);
			workers = new CircularBufferWorker [numberOfThreads];
			for (int i = 0; i < workers.length; i++) {
				workers[i] = new CircularBufferWorker (this, i + coreToBind);
				Thread thread = new Thread(workers[i]);
				thread.start();
			}			
			isBufferFilledLatch = new Latch(numberOfThreads);
			latch = new CountDownLatch(numberOfThreads);
			timestampBase = System.currentTimeMillis();
			timestamp = System.currentTimeMillis() - timestampBase;
		}		
		
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
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putInt (int index, int value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putFloat (float value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putFloat (int index, float value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putLong (long value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
	}
	
	public int putLong (int index, long value) {
		
		throw new UnsupportedOperationException("error: cannot put value to a circular buffer");
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
		return put( values, length, 0);
	}
	
	public int put (byte [] values, int length, int offset) {
		if (isDirect)
			throw new UnsupportedOperationException("error: cannot put array to a direct buffer");
		
		if (values == null || length <= 0)
			throw new NullPointerException ("error: cannot put null to a circular buffer");
		
		final long _end = end.get();
		final long wrapPoint = (_end + length - 1) - size;
		if (h.value <= wrapPoint) {
			h.value = start.get();
			if (h.value <= wrapPoint) {
				/* debug (); */
				return -1;				
			}
		}
		int index = normalise (_end);
		
		if (id == 0) {
			//set the pointers
			globalIndex = index;
			globalLength = length/numberOfThreads;
			timestamp = System.currentTimeMillis() - timestampBase;
			inputBuffer = values;
			
			if (this.isReady.get() == Integer.MAX_VALUE)
				this.isReady.set(-1);
			else
				this.isReady.incrementAndGet();//compareandswap
			
			while (this.isBufferFilledLatch.getCount()!=0)
				Thread.yield();

			this.isBufferFilledLatch.setLatch(numberOfThreads);
		} else {
			
			if (length > (size - index)) { 
			
				if (offset != 0)
					throw new NullPointerException ("error: copy in two part if the offset is greater than 0");
				
				int right = size - index;
				int left  = length - (size - index);
				
				System.arraycopy(values, 0, data, index, right);
				System.arraycopy(values, size - index, data, 0, left);
				
				//throw new IllegalStateException();
				
			} else {
				
				System.arraycopy(values, offset, data, index, length);
	
			}
		}
		
		
		int p = normalise (_end + length);
		if (p <= index)
			wraps ++;
		/* buffer.position(p); */
		end.lazySet(_end + length);
		/* debug (); */
		return index;
	}
	
	public int put (IQueryBuffer buffer) {
		
		return put (buffer.array());
	}
	
	public int put (IQueryBuffer buffer, int offset, int length) {
				
		return put (buffer.array(), length, offset);
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
		
		throw new UnsupportedOperationException("error: cannot get the position of a circular buffer");
	}

	public void position(int index) {
		
		throw new UnsupportedOperationException("error: cannot set the position of a circular buffer");
	}
	
	public boolean isDirect () {
		
		return this.isDirect;
	}
	
	public int getBufferId () {
		
		return this.id;
	}
}
