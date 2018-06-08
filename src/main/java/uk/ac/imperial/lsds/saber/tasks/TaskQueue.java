package uk.ac.imperial.lsds.saber.tasks;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicMarkableReference;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;
import uk.ac.imperial.lsds.saber.WindowBatch;

import sun.misc.Unsafe;
import java.lang.reflect.Field;


/* 
 * Based on the non-blocking queue of M. Herlihy and N. Shavit
 * ("The Art of Multiprocessor programming").
 */

public class TaskQueue {
		
	private AbstractTask head;//, tail;
	
	private AtomicIntegerArray [] count;
	
	//private ConcurrentLinkedQueue<AbstractTask> queue;
	
	public TaskQueue (int nqueries) {
		
		head = new Task ();
		AbstractTask tail = new Task (Integer.MAX_VALUE, null, null);
		while (! head.next.compareAndSet(null, tail, false, false));
		
		//queue = new ConcurrentLinkedQueue<AbstractTask>();
		
		
		count = new AtomicIntegerArray [2];
		for (int i = 0; i < 2; ++i)
			count[i] = new AtomicIntegerArray (nqueries);
	}
	
/*	  private static Unsafe unsafe;

	  private static final long tailOffset;
	  private static final long headOffset;

	  static {
	    try {
	      unsafe = getUnsafe();

	      tailOffset = unsafe.objectFieldOffset(TaskQueue.class.getDeclaredField("tail"));
	      headOffset = unsafe.objectFieldOffset(TaskQueue.class.getDeclaredField("head"));
	    } catch (Exception ex) {
	      throw new Error(ex);
	    }
	  }
	  
	  public static Unsafe getUnsafe() throws Exception {
		    if (unsafe == null) {
		        if (unsafe != null) {
		          return unsafe;
		        }

		        Field f = Unsafe.class.getDeclaredField("theUnsafe");
		        f.setAccessible(true);
		        unsafe = (Unsafe) f.get(null);
		      }
		    return unsafe;
	  }	  
	  
	   	
	  public boolean add(int taskid, WindowBatch batch1, WindowBatch batch2) {
		  AbstractTask node = new Task(taskid, batch1, batch2);
		  AbstractTask originTail;

		    do {
		      originTail = tail;		      
		    } while (!compareAndSetTail(originTail, node));

		    originTail.setNext(node);
		    size++;

		    return node;
		  }

	  
	  public AbstractTask poll() {
		AbstractTask originHead;
		AbstractTask nextHead;

	    do {
	      originHead = head;
	      nextHead = (AbstractTask) originHead.getNext();
	      if (nextHead == null) {
	        return null;
	      }
	    } while (!compareAndSetHead(originHead, nextHead));

	    nextHead.setPrev(null);
	    originHead.setNext(null); // help GC
	    size--;

	    return nextHead;
	  }

	  public int getSize() {
	    return size;
	  }

	  private boolean compareAndSetTail(LinkedNode current, LinkedNode replace) {
	    return unsafe.compareAndSwapObject(this, tailOffset, current, replace);
	  }

	  private boolean compareAndSetHead(LinkedNode current, LinkedNode replace) {
	    return unsafe.compareAndSwapObject(this, headOffset, current, replace);
	  }*/
	
	
/*	private TaskWindow find (int key) {
		
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

	public boolean add (int taskid, WindowBatch batch1, WindowBatch batch2) {
		
		while (true) {
			
			TaskWindow window = find (tail.taskid);
			
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			
			if (curr.taskid != tail.taskid)
				return false;
			else {
				AbstractTask task = new Task(taskid, batch1, batch2);				
				task.next = new AtomicMarkableReference<AbstractTask>(curr, false); 					
				if (pred.next.compareAndSet(curr, task, false, false)) {
					return true; 
				}
			}
		}
	}
	
	public AbstractTask poll (int[][] policy, int cid) {
		
		boolean snip;
		
		while (true) {
			
			TaskWindow window = find (head.taskid);
			
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			
			// Check if `curr` is not the tail of the queue 
			if (curr.taskid == tail.taskid) {
				return null;
			} else {
				// Mark `curr` as logically removed 
				AbstractTask succ = curr.next.getReference();
				snip = curr.next.compareAndSet(succ, succ, false, true);
				if (!snip)
					continue;
				pred.next.compareAndSet(curr, succ, false, false); 
				// Nodes are rewired 

				return curr;
			}
		}
	}

	
	public int getCount (int p, int q) {
		return count[p].get(q);
	}*/
	
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
	
	// Lock-free   Insert at the end of the queue 
	public boolean add (AbstractTask task) { //int taskid, WindowBatch batch1, WindowBatch batch2) {
		// int attempts = 0;
		
		//return queue.add(task);
		while (true) {
			TaskWindow window = TaskWindow.findTail(head);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			if (curr.taskid != Integer.MAX_VALUE) {
				return false;
			} else {
				//Task task = new Task (taskid, batch1, batch2);
				task.next.set(curr, false); //= new AtomicMarkableReference<AbstractTask>(curr, false);//.set(curr, false);
				if (pred.next.compareAndSet(curr, task, false, false)) {
					return true; 
				}
			}
		}
	}
	
	public AbstractTask getNextTask (int [][] policy, int p) {
		//return queue.poll();
		boolean snip;
		while (true) {
			TaskWindow window;
			if (SystemConf.SCHEDULING_POLICY == SystemConf.SchedulingPolicy.FIFO)
				window = TaskWindow.findHead(head);
			else
				window = TaskWindow.findNextSkipCost(head, policy, p, count);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			// Check if curr is not the tail of the queue 
			if (curr.taskid == Integer.MAX_VALUE) {
				return null;
			} else {
				// Mark curr as logically removed 
				AbstractTask succ = curr.next.getReference();
				snip = curr.next.compareAndSet(succ, succ, false, true);
				if (!snip) {
					continue;
				}
				pred.next.compareAndSet(curr, succ, false, false); 
				// Nodes are rewired; return curr 
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
		
		int q; // Query id 
		if (p == 0)
			q = 0;
		else
			q = 1;
		boolean snip;
		while (true) {
			TaskWindow window = TaskWindow.findQueryTask (head, q);
			AbstractTask pred = window.pred;
			AbstractTask curr = window.curr;
			// Check if curr is not the tail of the queue 
			if (curr.taskid == Integer.MAX_VALUE) {
				return null;
			} else {
				// Mark curr as logically removed 
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
	
	/*
	 * 	 Wait-free, but approximate queue size 
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
		return count;//queue.size();//count;
	}
	
	/*
	 * 	 Wait-free, but approximate print out (for debugging) 
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
