package uk.ac.imperial.lsds.saber;

public class QueryConf {
	//
	// 1KB = 1024
	// 1MB = 1048576
	// 1GB = 1073741824
	//
	private int batchSize = 1048576;

	public QueryConf (int batchSize) {

		this.batchSize = batchSize;
	}

	public int getBatchSize () {
		return batchSize;
	}
}
