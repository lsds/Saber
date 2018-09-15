package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;

public interface BenchmarkQuery {
	
	public QueryApplication getApplication();

	public ITupleSchema getSchema();
}