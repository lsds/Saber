package uk.ac.imperial.lsds.saber.www;

import org.eclipse.jetty.util.MultiMap;

public class ThroughputHandler implements IHandler {
	
	MeasurementQueue queue;
	
	public ThroughputHandler (int limit) {
		queue = new MeasurementQueue (limit);
	}
	
	public void addMeasurement (long timestamp, float cpuValue, float gpuValue) {
		queue.add (timestamp, cpuValue, gpuValue);
	}
	
	public Object getAnswer (MultiMap<String> params) {
		
		String mode = params.get("mode").get(0);
		
		long start = params.containsKey("start") ? Long.valueOf(params.getValue("start", 0)) : -1L;
		long  stop = params.containsKey( "stop") ? Long.valueOf(params.getValue( "stop", 0)) : -1L; 
		long  step = params.containsKey( "step") ? Long.valueOf(params.getValue( "step", 0)) : -1L;
		
		// System.out.println(String.format("[DBG] Throughput request: mode %s start %d stop %d step %d",
		//		mode, start, stop, step));
		
		return queue.fill (mode, start, stop, step);
	}
}
