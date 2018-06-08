package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;

public interface IQueryBuffer {
	
	public int getBufferId ();
	
	public int   		   getInt 	(int offset);
	public float 		 getFloat 	(int offset);
	public long  		  getLong 	(int offset);
	public long    getMSBLongLong 	(int offset);
	public long    getLSBLongLong 	(int offset);
	
	public byte [] array ();
	
	public void appendBytesTo (int  start, int    end, byte []      dst);
	public void appendBytesTo (int offset, int length, IQueryBuffer dst);
	
	public ByteBuffer getByteBuffer ();
	
	public int capacity ();
	
	public int normalise (long index);
	
	public int remaining ();
	public boolean hasRemaining ();
	
	public void position (int index);
	public int position ();
	
	public int limit ();
	
	public void close ();
	public void clear ();	
	
	public int putInt (int value);
	public int putInt (int index, int value);
	
	public int putFloat (float value);
	public int putFloat (int index, float value);
	
	public int putLong (long value);
	public int putLong (int index, long value);
	
	public int putLongLong (long msbValue, long lsbValue);
	public int putLongLong (int index, long msbValue, long lsbValue);
	
	public int put (byte [] src);
	public int put (byte [] src, int offset, int length);
	public int put (byte [] src, int length);
	
	public int put (IQueryBuffer src);
	public int put (IQueryBuffer src, int offset, int length);
	
	public void free (int offset);
	public void release ();
	
	public long getBytesProcessed ();
	
	public boolean isDirect ();
}
