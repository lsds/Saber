package uk.ac.imperial.lsds.saber.cql.operators;

import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public interface IOperatorCode {

	public void processData (WindowBatch batch, IWindowAPI api);
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api);
	
	public void configureOutput (int queryId);
	public void processOutput   (int queryId, WindowBatch batch);
	
	public void setup();
}
