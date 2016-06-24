package uk.ac.imperial.lsds.saber.buffers;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import uk.ac.imperial.lsds.saber.SystemConf;

public class WindowHashTableFactory {
	
	private static final int N = SystemConf.THREADS;
	
	private static long idx = 0;
	
	private static int _pool_size = 0;
	
	public static AtomicLong count;
	
	@SuppressWarnings("unchecked")
	public static ConcurrentLinkedQueue<WindowHashTable> [] pool = 
					(ConcurrentLinkedQueue<WindowHashTable> []) new ConcurrentLinkedQueue [N];
	
	static {
		
		for (int pid = 0; pid < N; pid++) {
			
			pool[pid] = new ConcurrentLinkedQueue<WindowHashTable>();
			
			int i = _pool_size;
			while (i-- > 0)
				pool[pid].add (new WindowHashTable(pid, idx++));
		}
		
		count = new AtomicLong (idx);
	}
	
	public static WindowHashTable newInstance (int pid) {
		
		WindowHashTable t = pool[pid].poll();
		if (t == null) {
			idx = count.getAndIncrement();
			t = new WindowHashTable (pid, idx);
		} else {
			t.setId (pid);
		}
		return t;
	}
	
	public static void free (WindowHashTable t) {
		int pid = t.getId();
		pool[pid].offer (t);
	}
}
