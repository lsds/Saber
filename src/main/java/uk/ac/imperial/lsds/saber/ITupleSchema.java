package uk.ac.imperial.lsds.saber;

import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public interface ITupleSchema {

	public int numberOfAttributes ();
	public int getTupleSize ();
	
	public int [] getOffsets ();
	public int getAttributeOffset (int index);

	public byte [] getPad ();
	public int getPadLength ();
	
	public void setAttributeType (int index, PrimitiveType type);
	public PrimitiveType getAttributeType (int index);
	
	public void setAttributeName (int index, String name);
	public String getAttributeName (int index);
	
	public String getName ();
	public String getSchema ();
}
