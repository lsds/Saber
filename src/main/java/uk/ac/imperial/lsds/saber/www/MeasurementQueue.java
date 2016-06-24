package uk.ac.imperial.lsds.saber.www;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MeasurementQueue {
	
	private static class Node {
		
		long timestamp;
		float cpuValue, gpuValue;
		Node next, prev;
		
		public Node () {
			clear ();
		}
		
		public void clear () {
			timestamp = 0L;
			cpuValue = gpuValue = 0F;
			next = prev = null;
		}
		
		public void set (long timestamp, float cpuValue, float gpuValue) {
			this.timestamp = timestamp;
			this.cpuValue = cpuValue;
			this.gpuValue = gpuValue;
		}
	}
	
	private static class NodeFactory {
		
		private int poolSize;
		
		private ConcurrentLinkedQueue<Node> pool = new ConcurrentLinkedQueue<Node>();
		
		public NodeFactory (int size) {
			poolSize = size;
			int i = poolSize;
			while (i-- > 0)
				pool.add(new Node());
		}
		
		public Node get () {
			Node p = pool.poll();
			if (p == null)
				return new Node ();
			return p;
		}
		
		public void free (Node p) {
			p.clear();
			pool.offer (p);
		}
	}
	
	private class NodeIterator {
		
		private Node current, last;
		private int index;
		
		public NodeIterator () {
			reset();
		}
		
		public void reset () {
			current = head.next;
			last = null;
			index = 0;
		}
		
		public boolean hasNext () {
			return (index < size);
		}
		
		public Node next () {
			Node result;
			if (! hasNext())
				throw new IllegalStateException ("error: invalid pointer access in node iterator");
			last = current;
			result = current;
			current = current.next;
			++index;
			return result;
		}
		
		@SuppressWarnings("unused")
		public void remove () {
			if (last == null)
				throw new IllegalStateException ("error: invalid pointer access in node iterator");
			Node f = last;
			Node q = last.prev;
			Node p = last.next;
			q.next = p;
			p.prev = q;
			--size;
			if (current == last)
				current = p;
			else
				--index;
			last = null;
			factory.free(f);
		}
	}
	
	Node head, tail;
	
	int size;
	int limit;
	
	NodeFactory factory;
	NodeIterator iterator;
	
	Lock lock;
	
	public MeasurementQueue (int max) {
		if (max < 2)
			throw new IllegalArgumentException ("error: measurement queue limit must be greater than 1");
		head = new Node (); 
		tail = new Node ();
		head.next = tail;
		tail.prev = head;
		size = 0;
		limit = max;
		factory = new NodeFactory (limit);
		iterator = new NodeIterator ();
		lock = new ReentrantLock ();
	}
	
	public boolean isEmpty () {
		return (size == 0);
	}
	
	public void add (long timestamp, float cpuValue, float gpuValue) { /* Thread-safe */
		Node p = factory.get();
		p.set (timestamp, cpuValue, gpuValue);
		lock.lock();
		Node last = tail.prev;
		p.next = tail;
		p.prev = last;
		tail.prev = p;
		last.next = p;
		++size;
		if (size >= limit)
			remove ();
		lock.unlock();
	}
	
	public void remove () { /* Thread-safe by default */
		Node p = head.next;
		Node q = p.next;
		head.next = q;
		q.prev = head;
		--size;
		factory.free(p);
	}
	
	public long getFirstTimestamp () { /* Thread-safe by default */
		if (isEmpty())
			return -1L;
		return head.next.timestamp;
		
	}
	
	public long getLastTimestamp () { /* Thread-safe by default */
		if (isEmpty())
			return -1L;
		return tail.prev.timestamp;
	}
	
	public List<Float> fill (String mode, long start, long end, long step) {
		
		List<Float> results = new ArrayList<Float> ();
		/* Iterate over the measurements queue, filling in the results array */
		lock.lock();
		/* Configure start and end timestamps */
		if (start < 0)
			start = getFirstTimestamp();
		if (end < 0)
			end = getLastTimestamp();
		iterator.reset();
		boolean skip = false;
		boolean exit = false;
		long _t = start;
		while (iterator.hasNext() && (! exit)) {
			Node p = iterator.next();
			if (p.timestamp < start)
				continue; // iterator.remove();
			else {
				/* Reset skip flag */
				if (p.timestamp >= _t) {
					skip = false;
					_t = _t + step;
				}	
				if (! skip) {
					if (mode.equals("cpu")) results.add(p.cpuValue);
					else
					if (mode.equals("gpu")) results.add(p.gpuValue);
					else
						results.add(p.cpuValue + p.gpuValue);
					
					/* Since a measurement has been added, we skip 
					 * subsequent ones until the next step. */
					skip = true;
				}
				if (p.timestamp > end)
					exit = true;
			}
		}
		lock.unlock();
		// System.out.println(results.toString());
		return results;
	}
}
