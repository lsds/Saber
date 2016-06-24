package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class Projection implements IOperatorCode {

	private Expression [] expressions;
	
	private ITupleSchema outputSchema;
	
	public Projection (Expression [] expressions) {
		this.expressions = expressions;
		this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
	}
	
	public Projection (Expression expression) {
		this.expressions = new Expression [] { expression };
		this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
	}
	
	@Override
	public String toString () {
		StringBuilder s = new StringBuilder();
		s.append("Projection (");
		for (Expression e: expressions)
			s.append(e.toString() + " ");
		s.append(")");
		return s.toString();
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();
		
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			for (int i = 0; i < expressions.length; ++i) {
				
				expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
			}
			outputBuffer.put(outputSchema.getPad());
		}
		
		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();
		
		/* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
		batch.setBuffer(outputBuffer);
		batch.setSchema(outputSchema);
		
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
