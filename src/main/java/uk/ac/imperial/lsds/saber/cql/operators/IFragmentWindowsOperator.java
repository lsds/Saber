package uk.ac.imperial.lsds.saber.cql.operators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;

public interface IFragmentWindowsOperator {

	public boolean hasGroupBy ();
	
	public ITupleSchema getOutputSchema ();
	
	public int getKeyLength ();
	
	public int getValueLength ();
	
	public int numberOfValues ();
	
	public AggregationType getAggregationType ();
	
	public AggregationType getAggregationType (int idx);
	
	public void createRelationalHashTable (WindowBatch batch, int offset);
	
	public boolean isHashJoin ();
}
