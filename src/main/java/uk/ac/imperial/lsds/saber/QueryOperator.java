package uk.ac.imperial.lsds.saber;

import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;


public class QueryOperator {

	private Query parent;
	
	private QueryOperator downstream;
	private QueryOperator upstream;
	
	private IOperatorCode cpuCode;
	private IOperatorCode gpuCode;

	public QueryOperator (IOperatorCode cpuCode, IOperatorCode gpuCode) {
		this.cpuCode = cpuCode;
		this.gpuCode = gpuCode;
		downstream = upstream = null;
	}
	
	public IOperatorCode getGpuCode() {
		return gpuCode;
	}
	
	public void setParent (Query parent) {
		this.parent = parent;
	}
	
	public Query getParent () {
		return parent;
	}

	public void connectTo (QueryOperator operator) {
		downstream = operator;
		operator.setUpstream (this);
	}
	
	private void setUpstream (QueryOperator operator) {
		upstream = operator;
	}

	public boolean isMostUpstream () {
		return (upstream == null);
	}

	public boolean isMostDownstream () {
		return (downstream == null);
	}
	
	public QueryOperator getDownstream () {
		return downstream;
	}
	
	public int getId () {
		if (parent != null)
			return parent.getId();
		return -1;
	}

	public void process (WindowBatch batch, IWindowAPI api, boolean GPU) {
		
		if (GPU)
			gpuCode.processData(batch, api);
		else
			cpuCode.processData(batch, api);
	}
	
	public void process (WindowBatch first, WindowBatch second, IWindowAPI api, boolean GPU) {
		
		if (GPU)
			gpuCode.processData(first, second, api);
		else
			cpuCode.processData(first, second, api);
	}

	public void setup() {
		if (gpuCode != null && SystemConf.GPU)
			gpuCode.setup();
	}
	
	public String toString () {
		if (cpuCode != null)
			return cpuCode.toString();
		else
			return gpuCode.toString();
	}
}
