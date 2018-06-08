package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.atomic.AtomicMarkableReference;


	public class LockFreeTaskQueue {		
		
		private AbstractTask head, tail;
		
		public LockFreeTaskQueue () {
			
			head = new Task (Integer.MIN_VALUE, null, null);
			tail = new Task (Integer.MAX_VALUE, null, null);
			
			while (! head.next.compareAndSet(null, tail, false, false));
		}
		
		private TaskWindow find (int key) {
			
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
					
					if ((curr.taskid >= key))
						return new TaskWindow (pred, curr);
					
					pred = curr;
					curr = succ;
				}
			}
		}
		
		public int size () {
			
			return 0;
		}

		public boolean add (AbstractTask task) {
			
			while (true) {
				
				TaskWindow window = find (tail.taskid);
				
				AbstractTask pred = window.pred;
				AbstractTask curr = window.curr;
				
				if (curr.taskid != tail.taskid)
					return false;
				else {
										
					task.next.set(curr, false); 					
					if (pred.next.compareAndSet(curr, task, false, false)) {
						return true; 
					}
				}
			}
		}
		
		public AbstractTask poll () {
			
			boolean snip;
			
			while (true) {
				
				TaskWindow window = find (head.taskid);
				
				AbstractTask pred = window.pred;
				AbstractTask curr = window.curr;
				
				/* Check if `curr` is not the tail of the queue */
				if (curr.taskid == tail.taskid) {
					return null;
				} else {
					/* Mark `curr` as logically removed */
					AbstractTask succ = curr.next.getReference();
					snip = curr.next.compareAndSet(succ, succ, false, true);
					if (!snip)
						continue;
					pred.next.compareAndSet(curr, succ, false, false); 
					/* Nodes are rewired */
					return curr;
				}
			}
		}
	}
