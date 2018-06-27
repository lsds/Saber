package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;

public interface IQueryBuffer {
	
	public int getBufferId ();

	public byte []  getColumnMap();
	public void  resetColumnMap();

	public int   		   getInt 	(int offset);
	public float 		 getFloat 	(int offset);
	public long  		  getLong 	(int offset);
	public long    getMSBLongLong 	(int offset);
	public long    getLSBLongLong 	(int offset);

	// columnar access methods
	public int   		   getInt 	(int offset, int column);
	public float 		 getFloat 	(int offset, int column);
	public long  		  getLong 	(int offset, int column);
	public long    getMSBLongLong 	(int offset, int column);
	public long    getLSBLongLong 	(int offset, int column);

	public byte [] array ();
	
	public void appendBytesTo (int  start, int    end, byte []      dst);
	public void appendBytesTo (int offset, int length, IQueryBuffer dst);

    public void appendBytesToColumn (int offset, int length, IQueryBuffer dst, int column);
    public void appendBytesToColumns (int offset, int length, IQueryBuffer dst, int column);
	
	public ByteBuffer getByteBuffer ();

    public ByteBuffer [] getByteBuffers ();
	
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

	// columnar put methods
    public int putInt (int value, int column, boolean isColumnar);
    public int putInt (int index, int value, int column);

    public int putFloat (float value, int column);
    public int putFloat (int index, float value, int column);

    public int putLong (long value, int column);
    public int putLong (int index, long value, int column);

    public int putLongLong (long msbValue, long lsbValue, int column);
    public int putLongLong (int index, long msbValue, long lsbValue, int column);

	public int put (byte [] src);
	public int put (byte [] src, int offset, int length);
	public int put (byte [] src, int length);

	public int put (byte [] src, int offset, int length, int column);

	public int put (byte [][] src);
	public int put (byte [][] src, int offset, int length);
	public int put (byte [][] src, int length);
	
	public int put (IQueryBuffer src);
	public int put (IQueryBuffer src, int offset, int length);

    public int put (ByteBuffer src, int offset, int length);
    public int put (ByteBuffer src, int offset, int length, int column);


    public void free (int offset);
	public void release ();
	
	public long getBytesProcessed ();
	
	public boolean isDirect ();
}
