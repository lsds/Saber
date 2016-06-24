package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.atomic.AtomicIntegerArray;

import uk.ac.imperial.lsds.saber.SystemConf;

class TaskWindow {
	
	public AbstractTask pred, curr;
	
	public TaskWindow (AbstractTask pred, AbstractTask curr) {
		this.pred = pred;
		this.curr = curr;
	}

	public static TaskWindow findTail (AbstractTask head) {
		AbstractTask pred = null;
		AbstractTask curr = null;
		AbstractTask succ = null;
		boolean [] marked = { false };
		boolean snip;
		retry: while (true) {
			pred = head;
			curr = pred.next.getReference();
			while (true) {
				succ = curr.next.get(marked);
				while (marked[0]) {
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if (! snip)
						continue retry;
					curr = pred.next.getReference();
					succ = curr.next.get(marked);
				}
				if ((curr.taskid >= Integer.MAX_VALUE))
					return new TaskWindow (pred, curr);
				pred = curr;
				curr = succ;
			}
		}
	}

	public static TaskWindow findHead (AbstractTask head) {
		AbstractTask pred = null;
		AbstractTask curr = null;
		AbstractTask succ = null;
		boolean [] marked = { false };
		boolean snip;
		retry: while (true) {
			pred = head;
			curr = pred.next.getReference();
			while (true) {
				succ = curr.next.get(marked);
				while (marked[0]) {
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if (! snip) {
						continue retry;
					}
					curr = pred.next.getReference();
					succ = curr.next.get(marked);
				}
				return new TaskWindow (pred, curr);
			}
		}
	}
	
	public static TaskWindow findQueryTask (AbstractTask head, int query) {
		AbstractTask pred = null;
		AbstractTask curr = null;
		AbstractTask succ = null;
		boolean [] marked = { false };
		boolean snip;
		retry: while (true) {
			pred = head;
			curr = pred.next.getReference();
			while (true) {
				succ = curr.next.get(marked);
				while (marked[0]) {
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if (! snip)
						continue retry;
					curr = pred.next.getReference();
					succ = curr.next.get(marked);
				}
				if (curr.queryid == query || curr.taskid == Integer.MAX_VALUE)
					return new TaskWindow (pred, curr);
				pred = curr;
				curr = succ;
			}
		}
	}
	
	private static boolean select (AbstractTask t, int [][] policy, int p, int _p, AtomicIntegerArray [] count, double cost) {
		if (! SystemConf.HYBRID)
			return true;
		
		int q = t.queryid;
		int preferred;
		if (policy[p][q] == policy[_p][q]) {
			/* CPU always wins */
			preferred = 1;
		} else {
			preferred = (policy[p][q] > policy[_p][q]) ? p : _p;
		}
		// System.out.println(String.format("[DBG] in select(): task %d.%d p %d pref %d p.count=%d pref.count=%d",
		//		q, t.taskid, p, preferred, count[p].get(q), count[preferred].get(q)));
		if (
			(p == preferred && ( count[p].get(q) <  SystemConf.SWITCH_THRESHOLD)) ||
			(p != preferred && ((count[preferred].get(q) >= SystemConf.SWITCH_THRESHOLD)  || (cost >= 1. / (double) policy[p][q])))
		) {
			return true;
		}
		return false;
	}
	
	public static TaskWindow findNextSkipCost (AbstractTask head, int[][] policy, int p, AtomicIntegerArray [] count) {
		AbstractTask pred = null;
		AbstractTask curr = null;
		AbstractTask succ = null;
		boolean [] marked = { false };
		boolean snip;
		int _p = (p + 1) % 2; /* The other processor */
		double skip_cost = 0.;
		retry: while (true) {
			pred = head;
			curr = pred.next.getReference();
			if (curr.taskid == Integer.MAX_VALUE)
				return new TaskWindow (pred, curr);
			while (true) {
				succ = curr.next.get(marked);
				while (marked[0]) {
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if (! snip)
						continue retry;
					curr = pred.next.getReference();
					succ = curr.next.get(marked);
				}
				
				if (curr.taskid == Integer.MAX_VALUE)
					return new TaskWindow (pred, curr);
				
				if (select (curr, policy, p, _p, count, skip_cost)) {
					return new TaskWindow (pred, curr);
				}
				
				if (SystemConf.HYBRID)
					skip_cost += 1. / (double) policy[_p][curr.queryid];
				
				pred = curr;
				curr = succ;
			}
		}
	}
}

