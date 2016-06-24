package uk.ac.imperial.lsds.saber.buffers;
import java.util.concurrent.atomic.AtomicLong;

public class PaddedAtomicLong extends AtomicLong {

	private static final long serialVersionUID = 1L;

	public PaddedAtomicLong (final long value) {
		super(value);
	}
	
	public volatile long _2, _3, _4, _5, _6, _7 = 7L;
	
	public long dummy () {
		return (_2 + _3 + _4 + _5 + _6 + _7);
	} 
}
