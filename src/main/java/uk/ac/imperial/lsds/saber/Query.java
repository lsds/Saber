package uk.ac.imperial.lsds.saber;

import java.util.Set;

import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.dispatchers.ITaskDispatcher;
import uk.ac.imperial.lsds.saber.dispatchers.JoinTaskDispatcher;
import uk.ac.imperial.lsds.saber.dispatchers.TaskDispatcher;
import uk.ac.imperial.lsds.saber.tasks.TaskQueue;

public class Query {
	
	/* Note that since we implement an N-way join as a series of binary 
	 * joins, the maximum number of upstream operators should be 2
	 */
	private int numberOfUpstreamQueries;
	private int numberOfDownstreamQueries;
	
	private long timestampReference = 0L;
	
	private int id;
	
	private String name;
	private String sql;

	private Set<QueryOperator> operators = null;

	private QueryOperator mostUpstreamOperator = null;
	private QueryOperator mostDownstreamOperator = null;

	private QueryApplication parent;

	private Query [] upstreamQueries   = null;
	private Query [] downstreamQueries = null;

	private ITaskDispatcher dispatcher;

	private ITupleSchema firstSchema, secondSchema;
	private WindowDefinition firstWindow, secondWindow;
	
	private QueryConf queryConf;
	
	private LatencyMonitor latencyMonitor;

	private boolean isLeft = false;
	
	public Query (
			int id, 
			Set<QueryOperator> operators, 
			ITupleSchema schema, 
			WindowDefinition window, 
			QueryConf queryConf) {
		
		this(id, operators, schema, window, null, null, queryConf, 0L);
	}
	
	public Query (
			int id, 
			Set<QueryOperator> operators, 
			ITupleSchema schema, 
			WindowDefinition window, 
			QueryConf queryConf,
			long timestampReference) {
		
		this(id, operators, schema, window, null, null, queryConf, timestampReference);
	}
	
	public Query(
			int id, 
			Set<QueryOperator> operators,
			ITupleSchema firstSchema, 
			WindowDefinition firstWindow, 
			ITupleSchema secondSchema, 
			WindowDefinition secondWindow,
			QueryConf queryConf) {
		
		this(id, operators, firstSchema, firstWindow, secondSchema, secondWindow, queryConf, 0L);
	}
	
	public Query (
			int id, 
			Set<QueryOperator> operators,
			ITupleSchema firstSchema, 
			WindowDefinition firstWindow, 
			ITupleSchema secondSchema, 
			WindowDefinition secondWindow,
			QueryConf queryConf,
			long timestampReference) {
		
		this.id = id;
		this.name = this.sql = null;
		
		this.operators = operators;
		
		this.firstSchema = firstSchema;
		this.firstWindow = firstWindow;
		
		this.secondSchema = secondSchema;
		this.secondWindow = secondWindow;
		
		this.queryConf = queryConf;
		
		this.timestampReference = timestampReference;
		
		for (QueryOperator op: this.operators) {
			
			op.setParent(this);
			
			if (op.isMostUpstream())
				mostUpstreamOperator = op;
			
			if (op.isMostDownstream())
				mostDownstreamOperator = op;
		}
		
		upstreamQueries = new Query[2];
		downstreamQueries = new Query[2];
		
		for (int i = 0; i < 2; ++i)
			upstreamQueries[i] = downstreamQueries[i] = null;
		
		numberOfUpstreamQueries = 0;
		numberOfDownstreamQueries = 0;
		
		latencyMonitor = new LatencyMonitor (this.timestampReference);
		if (! SystemConf.LATENCY_ON)
			this.latencyMonitor.disable();
		
		dispatcher = null;
	}
	
	public void setName (String name) {
		this.name = name;
	}
	
	public void setSQLExpression (String sql) {
		this.sql = sql;
	}
	
	public String getName () {
		if (name != null)
			return name;
		else
			return String.format("Query %d", id);
	}

	public String getSQLExpression () {
		if (sql != null)
			return sql;
		else {
			StringBuilder s = new StringBuilder ();
			s.append(mostUpstreamOperator.toString()).append(" ");
			if (firstWindow != null)
				s.append(firstWindow.toString());
			if (secondWindow != null)
				s.append(" ").append(secondWindow.toString());
			return s.toString();
		}
	}
	
	public int getId () {
		return id;
	}
	
	public boolean isMostUpstream () {
		return (numberOfUpstreamQueries == 0);
	}

	public boolean isMostDownstream () {
		return (numberOfDownstreamQueries == 0);
	}
	
	public QueryOperator getMostUpstreamOperator () {
		return this.mostUpstreamOperator;
	}

	public QueryOperator getMostDownstreamOperator () {
		return this.mostDownstreamOperator;
	}

	public QueryApplication getParent () {
		return parent;
	}

	public void setParent (QueryApplication parent) {
		this.parent = parent;
	}

	public ITaskDispatcher getTaskDispatcher () {
		return dispatcher;
	}
	
	public TaskQueue getExecutorQueue () {
		return parent.getExecutorQueue();
	}

	public WindowDefinition getWindowDefinition () {
		return firstWindow;
	}

	public ITupleSchema getSchema () {
		return firstSchema;
	}
	
	public WindowDefinition getFirstWindowDefinition () {
		return getWindowDefinition();
	}

	public ITupleSchema getFirstSchema() {
		return getSchema();
	}
	
	public WindowDefinition getSecondWindowDefinition () {
		return secondWindow;
	}

	public ITupleSchema getSecondSchema () {
		return secondSchema;
	}
	
	public QueryConf getQueryConf () {	
		return queryConf;
	}

	public LatencyMonitor getLatencyMonitor () {
		return latencyMonitor;
	}
	
	public void setup () {
		
		if ((secondSchema == null) && (secondWindow == null))
			dispatcher = new TaskDispatcher (this);
		else 
			dispatcher = new JoinTaskDispatcher (this);
		
		dispatcher.setup();
		
		for (QueryOperator op: operators) {
			op.setup();
		}
	}
	
	public void connectTo (Query query) {
		
		if (numberOfDownstreamQueries >= downstreamQueries.length)
			throw new ArrayIndexOutOfBoundsException("error: invalid number of downstream queries in query");
		
		int idx = numberOfDownstreamQueries++;
		this.downstreamQueries [idx] = query;
		query.setUpstreamQuery (this);
	}
	
	private void setUpstreamQuery (Query query) {
		/* If this is the first upstream query that we register, then set it 
		 * to be the left one (in a two-way join) */
		if (numberOfUpstreamQueries == 0)
			query.setLeft(true);
		
		if (numberOfUpstreamQueries >= upstreamQueries.length)
			throw new ArrayIndexOutOfBoundsException("error: invalid number of upstream queries in query");
		
		int idx = numberOfUpstreamQueries++;
		this.upstreamQueries[idx] = query;
	}
	
	private void setLeft (boolean isLeft) {
		this.isLeft = isLeft;
	}
	
	public boolean isLeft () {
		return isLeft;
	}
	
	public Query getUpstreamQuery () {
		return upstreamQueries[0];
	}
	
	public Query getUpstreamQuery (int idx) {
		return upstreamQueries[idx];
	}
	
	public Query getDownstreamQuery () {
		return downstreamQueries[0];
	}
	
	public Query getDownstreamQuery (int idx) {
		return downstreamQueries[idx];
	}
	
	public int getNumberOfUpstreamQueries () {
		return numberOfUpstreamQueries;
	}
	
	public int getNumberOfDownstreamQueries () {
		return numberOfDownstreamQueries;
	}
	
	public void setAggregateOperator (IAggregateOperator operator) {
		dispatcher.setAggregateOperator (operator);
	}
}
