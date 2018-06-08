package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;

public interface YahooBenchmarkQuery {
	
	public QueryApplication getApplication ();

	public ITupleSchema getSchema ();	
}
