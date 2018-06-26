package uk.ac.imperial.lsds.saber.dispatchers;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.handlers.ResultHandler;

import java.nio.ByteBuffer;

public interface ITaskDispatcher {

	public void setup();
	
	public void dispatch               (byte [] data, int length);
	public void dispatchToFirstStream  (byte [] data, int length);
	public void dispatchToSecondStream (byte [] data, int length);

    public void dispatch               (ByteBuffer buffer, int length);
    public void dispatchToFirstStream  (ByteBuffer buffer, int length);
    public void dispatchToSecondStream (ByteBuffer buffer, int length);
	
	public IQueryBuffer getBuffer      ();
	public IQueryBuffer getFirstBuffer ();
	public IQueryBuffer getSecondBuffer();
	
	public boolean tryDispatch               (byte [] data, int length);
	public boolean tryDispatchToFirstStream  (byte [] data, int length);
	public boolean tryDispatchToSecondStream (byte [] data, int length);

	public boolean tryDispatch               (ByteBuffer buffer, int length);
	public boolean tryDispatchToFirstStream  (ByteBuffer buffer, int length);
	public boolean tryDispatchToSecondStream (ByteBuffer buffer, int length);

	public long getBytesGenerated();

	public void setAggregateOperator (IAggregateOperator operator);

	public ResultHandler getHandler();
}
