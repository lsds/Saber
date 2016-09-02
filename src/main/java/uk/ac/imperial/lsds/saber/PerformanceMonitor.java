package uk.ac.imperial.lsds.saber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.dispatchers.ITaskDispatcher;
import uk.ac.imperial.lsds.saber.tasks.TaskFactory;

public class PerformanceMonitor implements Runnable {
	
	int counter = 0;
	
	private long time, _time = 0L;
	private long dt;
		
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

		public Measurement (int id, ITaskDispatcher dispatcher, LatencyMonitor monitor) {
			this.id = id;
			
			this.dispatcher = dispatcher;
			this.monitor = monitor;
			
			firstBuffer  = this.dispatcher.getFirstBuffer();
			secondBuffer = this.dispatcher.getSecondBuffer();
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
			
			bytesProcessed = firstBuffer.getBytesProcessed();
			if (secondBuffer != null)
				bytesProcessed += secondBuffer.getBytesProcessed();
			
			bytesGenerated = dispatcher.getBytesGenerated();
			
			if (_bytesProcessed > 0) {
				
				Dt = (delta / 1000.0);
				
				MBpsProcessed = (bytesProcessed - _bytesProcessed) / _1MB_ / Dt;
				MBpsGenerated = (bytesGenerated - _bytesGenerated) / _1MB_ / Dt;
				
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
