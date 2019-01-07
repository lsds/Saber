package uk.ac.imperial.lsds.saber.www;

import java.util.Map;

public class ThroughputHandler implements IHandler {
	
	MeasurementQueue queue;
	
	public ThroughputHandler (int limit) {
		queue = new MeasurementQueue (limit);
	}
	
	public void addMeasurement (long timestamp, float cpuValue, float gpuValue) {
		queue.add (timestamp, cpuValue, gpuValue);
	}
	
	@Override
	public Object getAnswer(Map<String, String[]> map) {
		
		String mode = map.get("mode")[0];
		
		long start = map.containsKey("start") ? Long.valueOf(map.get("start")[0]) : -1L;
		long  stop = map.containsKey( "stop") ? Long.valueOf(map.get( "stop")[0]) : -1L; 
		long  step = map.containsKey( "step") ? Long.valueOf(map.get( "step")[0]) : -1L;
		
		// System.out.println(String.format("[DBG] Throughput request: mode %s start %d stop %d step %d",
		//		mode, start, stop, step));
		
		return queue.fill (mode, start, stop, step);
	}
}
