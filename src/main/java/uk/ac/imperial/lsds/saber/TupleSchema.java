package uk.ac.imperial.lsds.saber;

public class TupleSchema implements ITupleSchema {
	
	public enum PrimitiveType {
		
		UNDEFINED, INT, FLOAT, LONG, LONGLONG;
	}
	
	private int [] offsets;
	private int contentSize;
	
	private int tupleSize;
	
	private byte [] pad;
	
	private PrimitiveType [] types; /* 0:undefined 1:int, 2:float, 3:long, 4: long long */
	private String [] names;
	
	private String name;
	
	public TupleSchema (int [] offsets, int contentSize) {
		
		if (contentSize <= 0)
			throw new IllegalArgumentException("error: tuple size must be greater than 0");
		
		this.offsets = offsets;
		this.contentSize = contentSize;
		
		this.tupleSize = this.contentSize;
		int padLength = 0;
		/* Expand size, if needed, to ensure that tuple size is a power of 2 */
		if ((this.contentSize & (this.contentSize - 1)) != 0) {
			
			this.tupleSize = 1;
			while (this.contentSize > this.tupleSize)
				this.tupleSize *= 2;
			
			padLength = this.tupleSize - this.contentSize;
		}
		
		this.pad = new byte [padLength];
		/* Initialise dummy content */
		if (padLength > 0)
			for (int i = 0; i < padLength; ++i)
				this.pad[i] = 0;
		
		types = new PrimitiveType [numberOfAttributes()];
		for (int i = 0; i < numberOfAttributes(); ++i)
			types[i] = PrimitiveType.UNDEFINED;
		
		names = new String [numberOfAttributes()];
		/* The first attribute is always a timestamp */
		names[0] = "timestamp";
		for (int i = 1; i < numberOfAttributes(); ++i)
			names[i] = null;
		
		name = null;
	}
	
	public int numberOfAttributes () {
		
		return offsets.length;
	}
	
	public int getAttributeOffset (int index) {
		
		if (index < 0 || index >= offsets.length)
			throw new IllegalArgumentException("error: invalid tuple attribute index");
		
		return offsets[index];
	}
	
	public int getTupleSize () {
		
		return tupleSize;
	}
	
	public byte [] getPad () {
		
		return pad;
	}
	
	public int getPadLength () {
		
		return pad.length;
	}
	
	public int [] getOffsets () {
		
		return offsets;
	}
	
	public void setAttributeType (int idx, PrimitiveType type) {
		
		if (idx < 0 || idx >= types.length)
			throw new IllegalArgumentException("error: invalid tuple attribute index");
		
		types[idx] = type;
	}
	
	public PrimitiveType getAttributeType (int index) {
		
		if (index < 0 || index >= types.length)
			throw new IllegalArgumentException("error: invalid tuple attribute index");
		
		return types[index];
	}
	
	public void setAttributeName (int idx, String name) {
		
		if (idx < 0 || idx >= names.length)
			throw new IllegalArgumentException("error: invalid tuple attribute index");
		
		names[idx] = name;
		
	}
	
	public String getAttributeName (int index) {
		
		if (index < 0 || index >= names.length)
			throw new IllegalArgumentException("error: invalid tuple attribute index");
		
		return (names[index] == null) ? String.format("%d", index) : names[index];
	}
	
	public void setName (String name) {
		this.name = name;
	}
	
	public String getName () {
		if (name == null)
			return "Stream";
		return name;
	}
	
	public String getSchema () {
		StringBuilder s = new StringBuilder(getName());
		s.append(" (");
		for (int i = 0; i < numberOfAttributes(); ++i) {
			s.append(getAttributeName(i));
			s.append(": ");
			switch (getAttributeType(i)) {
			case   	  INT: s.append(  "int"); break;
			case 	FLOAT: s.append("float"); break;
			case  	 LONG: s.append( "long"); break;
			case LONGLONG: s.append("long long"); break;
			default:
				throw new IllegalStateException("error: invalid tuple attribute type");
			}
			if (i < (numberOfAttributes() - 1))
				s.append(", ");
		}
		s.append(")");
		return s.toString();
	}
}
