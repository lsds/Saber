package uk.ac.imperial.lsds.saber;

public class WindowDefinition {
	
	public enum WindowType { 
		
		ROW_BASED , RANGE_BASED;
		
		public static WindowType fromString (String type) {
			
			if (type.toLowerCase().contains("row"))
				return ROW_BASED;
			else
			if (type.toLowerCase().contains("range"))
				return RANGE_BASED;
			else
				throw new IllegalArgumentException(String.format("error: unknown window type %s", type));
		}
	}
	
	private WindowType type;
	
	private long  size;
	private long slide;
	
	private long paneSize;
	
	public WindowDefinition (WindowType type, long size, long slide) {
		this.type  =  type;
		this.size  =  size;
		this.slide = slide;
		this.paneSize = gcd (this.size, this.slide);
	}
	
	public long getSize () {
		return this.size;
	}
	
	public long getSlide () {
		return this.slide;
	}
	
	public WindowType getWindowType () {
		return this.type;
	}
	
	public long getPaneSize () {
		return this.paneSize;
	}
	
	public long numberOfPanes () {
		return (this.size / this.paneSize);
	}
	
	public long panesPerSlide () {
		return (this.slide / this.paneSize);
	}
	
	public boolean isRowBased () {
		return (type.compareTo(WindowType.ROW_BASED) == 0);
	}
	
	public boolean isRangeBased () {
		return (type.compareTo(WindowType.RANGE_BASED) == 0);
	}
	
	public boolean isTumbling () {
		return (this.size == this.slide);
	}
	
	private long gcd (long a, long b) {
		if (b == 0) 
			return a;
		return 
			gcd (b, a % b);
	}
	
	public String toString () {
		return String.format("[%s %d slide %d]", ((isRowBased()) ? "row" : "range"), size, slide); 
	}
}
