package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class NoOp implements IOperatorCode {
	
	public static final boolean debug = false;

	public NoOp () {
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		/* Copy input to output */
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		
		int start = batch.getBufferStartPointer();
		int end = batch.getBufferEndPointer();
		
		if (debug) {
			int mark = batch.getLatencyMark();
			if (mark >= 0)
				System.out.println(String.format("[DBG] task %6d mark %10d", batch.getTaskId(), batch.getLatencyMark()));
		}
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		inputBuffer.appendBytesTo (start, end, outputBuffer.array());
		outputBuffer.position(batch.getBatchSize());
		
		batch.setBuffer(outputBuffer);
		
		api.outputWindowBatchResult(batch);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}

	public void configureOutput (int queryId) {
		
		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	public void processOutput (int queryId, WindowBatch batch) {
		
		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
		
	}

	public void setup() {
		
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}
}
