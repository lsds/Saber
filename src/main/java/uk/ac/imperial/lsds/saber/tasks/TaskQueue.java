package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.atomic.AtomicIntegerArray;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;

/* 
 * Based on the non-blocking queue of M. Herlihy and N. Shavit
 * ("The Art of Multiprocessor programming").
 */

public class TaskQueue {
		
	private AbstractTask head;
	
	private AtomicIntegerArray [] count;
	
	public TaskQueue (int nqueries) {
		
		head = new Task ();
		AbstractTask tail = new Task (Integer.MAX_VALUE, null, null);
		while (! head.next.compareAndSet(null, tail, false, false));
		
		count = new AtomicIntegerArray [2];
		for (int i = 0; i < 2; ++i)
			count[i] = new AtomicIntegerArray (nqueries);
	}
	
	private int incrementAndGet (int p, int q) {
		int val = count[p].get(q);
		while (! count[p].compareAndSet(q, val, val + 1))
			val = count[p].get(q);
		return val + 1;
	}
	
	private void resetCounter (int p, int q) {
		count[p].set(q, 0);
	}
	
	public int getCount (int p, int q) {
		return count[p].get(q);
	}
	
	private int getPreferredProcessor (int [][] policy, int q) {
		int result;
		if (policy[0][q] > policy[1][q])
			result = 0;
		else
			result = 1;
		// System.out.println(String.format("[DBG] q %d preferred processor is %d (policy[0][] = %6d policy[1][] = %6d)", 
		//		q, result, policy[0][q], policy[1][q]));
		return result;
	}
	
	/* Lock-free */ /* Insert at the end of the queue */
	public boolean add (AbstractTask task) {
		// int attempts = 0;
		while (true) {
			TaskWindow window = TaskWindow.findTail(head);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			if (curr.taskid != Integer.MAX_VALUE) {
				return false;
			} else {
				task.next.set(curr, false);
				if (pred.next.compareAndSet(curr, task, false, false)) {
					return true; 
				}
			}
		}
	}
	
	public AbstractTask getNextTask (int [][] policy, int p) {
		boolean snip;
		while (true) {
			TaskWindow window;
			if (SystemConf.SCHEDULING_POLICY == SystemConf.SchedulingPolicy.FIFO)
				window = TaskWindow.findHead(head);
			else
				window = TaskWindow.findNextSkipCost(head, policy, p, count);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			/* Check if curr is not the tail of the queue */
			if (curr.taskid == Integer.MAX_VALUE) {
				return null;
			} else {
				/* Mark curr as logically removed */
				AbstractTask succ = curr.next.getReference();
				snip = curr.next.compareAndSet(succ, succ, false, true);
				if (!snip) {
					continue;
				}
				pred.next.compareAndSet(curr, succ, false, false); 
				/* Nodes are rewired; return curr */
				if (SystemConf.SCHEDULING_POLICY == SystemConf.SchedulingPolicy.HLS) {
					int preferred = getPreferredProcessor (policy, curr.queryid);
					if (getCount(preferred, curr.queryid) >= SystemConf.SWITCH_THRESHOLD) {
						// System.out.println(String.format("[DBG] reset counter p=%d q=%d", preferred, curr.queryid));
						resetCounter (preferred, curr.queryid);
					}
					incrementAndGet(p, curr.queryid);
				}
				return curr;
			}
		}
	}
	
	public AbstractTask staticTaskAssignment (int p) {
		
		int q; /* Query id */
		if (p == 0)
			q = 0;
		else
			q = 1;
		boolean snip;
		while (true) {
			TaskWindow window = TaskWindow.findQueryTask (head, q);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			/* Check if curr is not the tail of the queue */
			if (curr.taskid == Integer.MAX_VALUE) {
				return null;
			} else {
				/* Mark curr as logically removed */
				AbstractTask succ = curr.next.getReference();
				snip = curr.next.compareAndSet(succ, succ, false, true);
				if (!snip)
					continue;
				pred.next.compareAndSet(curr, succ, false, false); 
				return curr;
			}
		}
	}
	
	public AbstractTask poll (int [][] policy, int p) {
		if (SystemConf.SCHEDULING_POLICY == SchedulingPolicy.STATIC)
			return staticTaskAssignment (p);
		return getNextTask (policy, p);
	}
	
	/* Wait-free, but approximate queue size 
	 * 
	 */
	public int size () {
		boolean [] marked = { false };
		int count = 0;
		AbstractTask t;
		for (t = head.next.getReference(); t != null && !marked[0]; t = t.next.get(marked)) {
			if (t.taskid < Integer.MAX_VALUE) {
				count ++;
			}
		}
		return count;
	}
	
	/* Wait-free, but approximate print out (for debugging) 
	 * 
	 */
	public void dump () {
		boolean [] marked = { false };
		int count = 0;
		System.out.print("Q: ");
		AbstractTask t;
		for (t = head.next.getReference(); t != null && !marked[0]; t = t.next.get(marked)) {
			if (t.taskid < Integer.MAX_VALUE) {
		 		System.out.print(String.format("%s ", t.toString()));
				count ++;
			}
		}
		System.out.println(String.format("(%d tasks)", count));
	}
}
