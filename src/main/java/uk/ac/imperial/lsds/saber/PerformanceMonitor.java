package uk.ac.imperial.lsds.saber;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.dispatchers.ITaskDispatcher;
import uk.ac.imperial.lsds.saber.tasks.TaskFactory;

public class PerformanceMonitor implements Runnable {
	
	int counter = 0;
	
	private long time, _time = 0L;
	private long dt;
	private boolean firstLatencyMeasurement = true;
		
	private QueryApplication application;
	private int size;
		
	private Measurement [] measurements;
	
	static final Comparator<Query> ordering = new Comparator<Query>() {
		public int compare(Query q, Query p) {
			return (q.getId() < p.getId()) ? -1 : 1;
		}
	};
	
	public PerformanceMonitor (QueryApplication application) {
		
		this.application = application;
			
		size = application.getQueries().size();
		measurements = new Measurement [size];
		List<Query> L = new ArrayList<Query>(application.getQueries());
		Collections.sort(L, ordering);
		int idx = 0;
		for (Query query : L) {
			System.out.println(String.format("[MON] [MultiOperator] S %3d", query.getId()));
			measurements[idx++] = 
				new Measurement (
					query.getId(), 
					query.getTaskDispatcher(),
					query.getLatencyMonitor()
				);
		}
	}
	
	public void run () {
		
		while (true) {
			
			try { 
				Thread.sleep(SystemConf.PERFORMANCE_MONITOR_INTERVAL); 
			} catch (Exception e) 
			{}
			
			time = System.currentTimeMillis();
			dt = time - _time;
			
			StringBuilder b = new StringBuilder();
			b.append("[MON]");
			
			for (int i = 0; i < size; i++)
				b.append(measurements[i].info(dt));
			
			b.append(String.format(" q %6d", application.getExecutorQueueSize()));
			if (SystemConf.SCHEDULING_POLICY == SystemConf.SchedulingPolicy.HLS)
				b.append(application.getExecutorQueueCounts());
			/* Append factory sizes */
			b.append(String.format(" t %6d",                 TaskFactory.count.get()));
			b.append(String.format(" w %6d",          WindowBatchFactory.count.get()));
			b.append(String.format(" b %6d", UnboundedQueryBufferFactory.count.get()));
			b.append(String.format(" p %6d", PartialWindowResultsFactory.count.get()));
			
			/* Append policy */
			b.append(" policy " + application.policyToString());
			
			/* Update WWW measurements */
			application.RESTfulUpdate (time);
			
			System.out.println(b);
			
			_time = time;
			
			if (SystemConf.DURATION > 0) {
				
				if (counter++ > SystemConf.DURATION) {
					
					for (int i = 0; i < size; i++)
						measurements[i].stop();
					
					System.out.println("[MON] Done.");
					System.out.flush();
					break;
				}
			}
		}
	}
	
	class Measurement {
		
		int id;
		
		IQueryBuffer firstBuffer, secondBuffer;
		
		LatencyMonitor monitor;
		
		ITaskDispatcher dispatcher;
		
		double Dt;
		
		double _1MB_ = 1048576.0;
		
		long bytesProcessed, _bytesProcessed = 0;
		long bytesGenerated, _bytesGenerated = 0;
		
		double MBpsProcessed, MBpsGenerated;
		
		long time, _time = 0;
		long timestampReference;
		double latency, latencyMin, latencyMax, latencyAvg, latencySum, latencyDelta, deltaHelper, thoughputAvg, throughputSum;
		int latencyCounter, throughputCounter;

		public Measurement (int id, ITaskDispatcher dispatcher, LatencyMonitor monitor) {
			this.id = id;
			
			this.dispatcher = dispatcher;
			this.monitor = monitor;
			
			firstBuffer  = this.dispatcher.getFirstBuffer();
			secondBuffer = this.dispatcher.getSecondBuffer();
			
			timestampReference = System.nanoTime();
			latencyMin = Double.MAX_VALUE;
			latencyMax = Double.MIN_VALUE;
			latencySum = 0.;
			latencyCounter = 1;			
			latencyDelta = 0;
			deltaHelper = 0;
			throughputCounter = 0;
			throughputSum = 0;
		}
			
		public void stop () {
			monitor.stop();
		}
		
		@Override
		public String toString () {
			return null;
		}
		
		public String info (long delta) {
			
			String s = "";
			
			NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);
		    String numberAsString;
		    
			bytesProcessed = firstBuffer.getBytesProcessed();
			if (secondBuffer != null)
				bytesProcessed += secondBuffer.getBytesProcessed();
			
			bytesGenerated = dispatcher.getBytesGenerated();
						
			if (_bytesProcessed > 0) {
				
				Dt = (delta / 1000.0);
				
				MBpsProcessed = (bytesProcessed - _bytesProcessed) / _1MB_ / Dt;
				MBpsGenerated = (bytesGenerated - _bytesGenerated) / _1MB_ / Dt;
				
				// TODO: Measure only the sources
				throughputCounter++;
				if (this.id == 0) {
					throughputSum += MBpsProcessed;
					thoughputAvg = ((throughputSum / throughputCounter) * _1MB_) / 128; // tuple size to get the records per second
					numberAsString = numberFormat.format((double) thoughputAvg);
					System.out.println("Throughput Average(records/sec) " + numberAsString);				
				}
				
				deltaHelper++;
				// TODO: Measure only the last operator
				if (this.id == 1 && MBpsGenerated > 0) {

					time = (System.nanoTime() - timestampReference) / 1000L;
					
					if (firstLatencyMeasurement) 
						firstLatencyMeasurement = false;
					else {
						latency = (time - _time) / 1000.;
						
						latencyMin = (latency < latencyMin) ? latency : latencyMin;
						latencyMax = (latency > latencyMax) ? latency : latencyMax;
						
						latencySum += latency;
						if (_time > 0) {
							latencyCounter++;
							latencyAvg = latencySum / latencyCounter;						
						}
						
						latencyDelta = latency - deltaHelper * SystemConf.PERFORMANCE_MONITOR_INTERVAL;
						deltaHelper = 0;
						
						System.out.format("Latency(ms) %10.3f, Delta %7.3f, Min %10.3f, Max %10.3f, Avg %10.3f", latency, latencyDelta, latencyMin, latencyMax, latencyAvg);
						System.out.println();
					}
					_time = time;
				}			
				
				s = String.format(" S%03d %10.3f MB/s output %10.3f MB/s [%s]", 
					id, 
					MBpsProcessed, 
					MBpsGenerated, 
					monitor);
			}
			
			_bytesProcessed = bytesProcessed;
			_bytesGenerated = bytesGenerated;
			
			return s;
		}
	}
}
