package uk.ac.imperial.lsds.saber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;

public class LatencyMonitor {

	long count;
	double min, max, avg;
	
	long timestampReference = 0L;
	
	double latency;
	
	AtomicBoolean active;
	
	ArrayList<Double> measurements;
	
	public LatencyMonitor (long timestampReference) {
		
		this.timestampReference = timestampReference;
		
		count = 0;
		
		min = Double.MAX_VALUE;
		max = Double.MIN_VALUE;
		avg = 0.0;
		
		latency = 0.0;
		
		active = new AtomicBoolean(true);
		
		measurements = new ArrayList<Double>();
	}
	
	public void disable () {
		active.set(false);
	}
	
	@Override
	public String toString () {
		
		if (count < 2)
			return null;
		
		avg = latency / count;
		
		return String.format("avg %10.3f min %10.3f max %10.3f",
			avg,
			min,
			max
			);
	}
	
	public void monitor (IQueryBuffer buffer, int mark) {
		
		if (! this.active.get()) {
			return ;
		}
		
		double dt = 0;
		
		/* Check buffer */
		
		long t1 = (long) Utils.getSystemTimestamp (buffer.getLong(mark));
		
		long t2 = (System.nanoTime() - timestampReference) / 1000L;
		dt = (t2 - t1) / 1000.; /* In milliseconds */
		
		measurements.add(dt);
		
		latency += dt;
		count += 1;
		
		min = (dt < min) ? dt : min;
		max = (dt > max) ? dt : max;
		return ;
	}

	public void stop() {
		
		active.set(false);
		
		int length = measurements.size();
		
		System.out.println(String.format("[MON] [LatencyMonitor] %10d measurements", length));
		
		if (length < 1)
			return;
		
		double [] array = new double [length];
		int i = 0;
		for (Double d: measurements)
			array[i++] = d.doubleValue();
		Arrays.sort(array);
		
		System.out.println(String.format("[MON] [LatencyMonitor] 5th %10.3f 25th %10.3f 50th %10.3f 75th %10.3f 99th %10.3f", 
			evaluateSorted(array,  5D),
			evaluateSorted(array, 25D),
			evaluateSorted(array, 50D),
			evaluateSorted(array, 75D),
			evaluateSorted(array, 99D)
			));
	}
	
	private double evaluateSorted (final double[] sorted, final double p) {
		
		double n = sorted.length;
		double pos = p * (n + 1) / 100;
		double fpos = Math.floor(pos);
		int intPos = (int) fpos;
		double dif = pos - fpos;
		
		if (pos < 1) {
			return sorted[0];
		}
		
		if (pos >= n) {
			return sorted[sorted.length - 1];
		}
		
		double lower = sorted[intPos - 1];
		double upper = sorted[intPos];
		return lower + dif * (upper - lower);
	}
}
