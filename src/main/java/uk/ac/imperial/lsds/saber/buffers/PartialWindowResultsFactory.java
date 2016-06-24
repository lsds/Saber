package uk.ac.imperial.lsds.saber.buffers;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.SystemConf;

public class PartialWindowResultsFactory {
	
	public static final int N = SystemConf.THREADS;
	
	private static int _pool_size = 0;
	
	public static AtomicLong count;
	
	@SuppressWarnings("unchecked")
	public static ConcurrentLinkedQueue<PartialWindowResults> [] pool = 
					(ConcurrentLinkedQueue<PartialWindowResults> []) new ConcurrentLinkedQueue [N];
	
	static {
		
		for (int pid = 0; pid < N; ++pid) {
			pool[pid] = new ConcurrentLinkedQueue<PartialWindowResults>();
			int i = _pool_size;
			while (i-- > 0) {
				PartialWindowResults r = new PartialWindowResults (pid);
				pool[pid].add (r);
			}
		}
		count = new AtomicLong (N * _pool_size);
	}
	
	public static PartialWindowResults newInstance (int pid) {
		
		PartialWindowResults r = pool[pid].poll();
		if (r == null) {
			r = new PartialWindowResults (pid);
			count.incrementAndGet();
		}
		r.init();
		return r;
	}
	
	public static void free (int pid, PartialWindowResults r) {
		pool[pid].offer (r);
	}
}
