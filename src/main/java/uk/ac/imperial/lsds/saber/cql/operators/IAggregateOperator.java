package uk.ac.imperial.lsds.saber.cql.operators;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public interface IAggregateOperator {

	public boolean hasGroupBy ();
	
	public ITupleSchema getOutputSchema ();
	
	public int getKeyLength ();
	
	public int getValueLength ();
	
	public int numberOfValues ();
	
	public AggregationType getAggregationType ();
	
	public AggregationType getAggregationType (int idx);
}
