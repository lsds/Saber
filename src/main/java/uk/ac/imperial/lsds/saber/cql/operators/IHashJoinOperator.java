package uk.ac.imperial.lsds.saber.cql.operators;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;

public interface IHashJoinOperator {	
	
	public ITupleSchema getOutputSchema ();
	
	public void createHashTable (WindowBatch batch, int offset);
}
