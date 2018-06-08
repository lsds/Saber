package uk.ac.imperial.lsds.saber.handlers;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableWrapper;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class PartialResultSlot {
	
	private static final boolean debug = false;
	
	int index;
	
	int freePointer1, freePointer2;
	
	int mark;
	int latch;
	
	boolean GPU = false;
	
	PartialResultSlot next;
	
	PartialWindowResults closingWindows, pendingWindows, openingWindows, completeWindows;
	
	ByteBuffer w3;
	
	WindowHashTableWrapper windowHashTable;
	WindowHashTableWrapper mergedHashTable;
	
	boolean [] b2found;
	boolean [] w3found;
	
	public PartialResultSlot (int index) {
		
		this.index = index;
		
		freePointer1 = freePointer2 = Integer.MIN_VALUE;
		
		mark = 0;
		latch = 0;
		
		GPU = false;
		
		next = null;
		
		/* Initialize windows */
		closingWindows = openingWindows = pendingWindows = completeWindows = null;
		
		w3 = ByteBuffer.allocate(SystemConf.HASH_TABLE_SIZE);
		
		windowHashTable = new WindowHashTableWrapper ();
		mergedHashTable = new WindowHashTableWrapper ();
		
		b2found = new boolean[1];
		b2found[0] = false;
		
		w3found = new boolean[1];
		w3found[0] = false;
	}
	
	public void connectTo (PartialResultSlot next) {
		this.next = next;
	}
	
	public void init (WindowBatch batch) {
		
		freePointer1 = batch.getFirstFreePointer();
		freePointer2 = batch.getSecondFreePointer();
		
		mark = batch.getLatencyMark();
		latch = 0;
		
		GPU = false;
		
		closingWindows  = batch.getClosingWindows ();
		openingWindows  = batch.getOpeningWindows ();
		pendingWindows  = batch.getPendingWindows ();
		completeWindows = batch.getCompleteWindows();
	}
	
	public void release () {
		
		if (closingWindows  != null)  
			closingWindows.release();
		
		if (openingWindows  != null)  
			openingWindows.release();
		
		if (pendingWindows  != null)  
			pendingWindows.release();
		
		if (completeWindows != null) 
			completeWindows.release();
	}
	
	/* 
	 * Aggregate this node's opening windows with node p's closing or pending windows. The output of this 
	 * operation will always produce complete or opening windows - never pending and never closing ones.
	 */
	public void aggregate (PartialResultSlot p, IAggregateOperator operator) {
		
		//System.out.println(this);
		//System.out.println(p);
		
		if (openingWindows.isEmpty()) { /* Nothing to aggregate */
			
			if ((! p.closingWindows.isEmpty()) || (! p.pendingWindows.isEmpty())) {
				
				throw new RuntimeException ("error: there are no opening windows but next slot has closing or pending windows");
			}
			
			openingWindows.nullify();
			
			p.closingWindows.nullify();
			p.pendingWindows.nullify();
			
			return;
		}
		
		if (p.closingWindows.isEmpty() && p.pendingWindows.isEmpty()) {
			
			throw new RuntimeException ("error: there are opening windows but next slot has neither closing nor pending windows");
		}
		
		if (operator.hasGroupBy())
			aggregateMultipleKeys (p, operator);
		else
			aggregateSingleKey (p, operator);
	}
	
	public void aggregateSingleKey (PartialResultSlot p, IAggregateOperator operator) {
		/* 
		 * Populate this node's complete windows or p's opening windows.
		 * 
		 * And, nullify this node's opening windows and node p's closing 
		 * and pending ones.
		 */
		
		/* this.openingWindows + p.closingWindows -> this.completeWindows 
		 * this.openingWindows + p.pendingWindows ->    p.openingWindows */
		
		int wid; /* Window index */
		IQueryBuffer b1 = openingWindows.getBuffer();
		IQueryBuffer b2;
		
		int pos1, pos2;
		int valueOffset1, countOffset1;
		int valueOffset2, countOffset2; 
		
		float value1, value2;
		int   count1, count2;
		
		if (debug) {
			System.out.println(String.format("[DBG] aggregate %10d bytes (%4d opening windows) with %10d bytes (%4d closing windows)",

				  openingWindows.getBuffer().position(),   openingWindows.numberOfWindows(), 
				p.closingWindows.getBuffer().position(), p.closingWindows.numberOfWindows())
			);
		}

		b2 = p.closingWindows.getBuffer();
		
		for (wid = 0; wid < p.closingWindows.numberOfWindows(); ++wid) {
			
			pos1 = p.closingWindows.getStartPointer(wid);
			/* Skip timestamp */
			pos1 += 8;
			
			countOffset1 = pos1 + (operator.numberOfValues() * 4);
			countOffset2 = countOffset1;
			count1 = b1.getInt(countOffset1);
			count2 = b2.getInt(countOffset2);
			/* Iterate over values */
			for (int i = 0; i < operator.numberOfValues(); ++i) {
				valueOffset1 = pos1 + (i * 4);
				valueOffset2 = valueOffset1;
				value1 = b1.getFloat(valueOffset1);
				value2 = b2.getFloat(valueOffset2);
				// System.out.println(String.format("[DBG] value1 = %5.1f value2 = %5.1f", value1, value2));
				AggregationType type = operator.getAggregationType(i);
				switch (type) {
				case CNT:
				case SUM:
					b2.putFloat(valueOffset2, value1 + value2);
					break;
				case AVG:
					/* Given <value1, count1> and <value2, count2>, then
					 * the average value is:
					 * 
					 * ((value1 x count2) + (value2 x count1)) / (count1 + count2)
					 */
					b2.putFloat(valueOffset2, ((value1 * count2) + (value2 * count1)) / ((float) (count1 + count2)));
					break;
				case MIN:
					if (value1 < value2)
						b2.putFloat(valueOffset2, value1);
					break;
				case MAX:
					if (value1 > value2)
						b2.putFloat(valueOffset2, value1);
					break;
				default:
					throw new IllegalArgumentException("error: invalid aggregation type");
				}
			}
			b2.putInt (countOffset2, (count1 + count2));
		}

		/* All closing windows of `p` are now complete. Append them to this node's complete windows */
		completeWindows.append(p.closingWindows);
		p.closingWindows.nullify();
		
		/* There may be some opening windows left, in which case they are aggregated with node p's pending one.
		 * The result will be stored (prepended) in p's opening windows */
		if (wid < openingWindows.numberOfWindows()) {

			if (p.pendingWindows.numberOfWindows() != 1) {

				throw new RuntimeException ("error: there are opening windows left but next slot has no pending windows");
			}
			
			if (debug) {
				System.out.println(String.format("[DBG] aggregate %4d remaining opening windows with pending", 
						openingWindows.numberOfWindows() - wid)); 
			}

			b2 = p.pendingWindows.getBuffer();

			int nextOpenWindow = wid;
			int count = 0;

			while (nextOpenWindow < openingWindows.numberOfWindows()) {

				pos1 =   openingWindows.getStartPointer(nextOpenWindow);
				pos2 = p.pendingWindows.getStartPointer(0);
				/* Skip timestamp */
				pos1 += 8;
				pos2 += 8;

				countOffset1 = pos1 + (operator.numberOfValues() * 4);
				countOffset2 = pos2 + (operator.numberOfValues() * 4);
				count1 = b1.getInt(countOffset1);
				count2 = b2.getInt(countOffset2);
				/* Iterate over values */
				for (int i = 0; i < operator.numberOfValues(); ++i) {
					valueOffset1 = pos1 + (i * 4);
					valueOffset2 = pos2 + (i * 4);
					value1 = b1.getFloat(valueOffset1);
					value2 = b2.getFloat(valueOffset2);
					AggregationType type = operator.getAggregationType(i);
					switch (type) {
					case CNT:
					case SUM:
						b1.putFloat(valueOffset1, value1 + value2);
						break;
					case AVG:
						/* Given <value1, count1> and <value2, count2>, then
						 * the average value is:
						 * 
						 * ((value1 x count2) + (value2 x count1)) / (count1 + count2)
						 */
						b1.putFloat(valueOffset1, ((value1 * count2) + (value2 * count1)) / ((float) (count1 + count2)));
						break;
					case MIN:
						if (value2 < value1)
							b1.putFloat(valueOffset1, value2);
						break;
					case MAX:
						if (value2 > value1)
							b1.putFloat(valueOffset1, value2);
						break;
					default:
						throw new IllegalArgumentException("error: invalid aggregation type");
					}
				}
				b1.putInt (countOffset1, (count1 + count2));

				++nextOpenWindow;
				++count;
			}

			/* Prepend this opening windows (starting from `wid`) to node p's opening windows.
			 * We have to shift the start pointers of p's opening windows down.
			 * 
			 * There are `count` new windows. The window size is:
			 */
			int windowSize = operator.getValueLength() + 12;
			
			p.openingWindows.prepend(openingWindows, wid, count, windowSize);
			
			p.pendingWindows.nullify();
		}
		
		openingWindows.nullify();
	}
		
	public void aggregateMultipleKeys (PartialResultSlot p, IAggregateOperator operator) {
		/* 
		 * Populate this node's complete windows or p's opening windows.
		 * 
		 * And, nullify this node's opening windows and node p's closing 
		 * and pending ones.
		 */
		
		/* this.openingWindows + p.closingWindows -> this.completeWindows 
		 * this.openingWindows + p.pendingWindows ->    p.openingWindows */
		
		int wid; /* Window index */
		IQueryBuffer b1 = openingWindows.getBuffer();
		IQueryBuffer b2;
		
		int start1, end1;
		int start2, end2;
		
		int edge1 =   openingWindows.numberOfWindows() - 1;
		int edge2 = p.closingWindows.numberOfWindows() - 1;
		
		if (debug) {
			System.out.println(String.format("[DBG] aggregate %10d bytes (%4d opening windows) with %10d bytes (%4d closing windows)",

				  openingWindows.getBuffer().position(),   openingWindows.numberOfWindows(), 
				p.closingWindows.getBuffer().position(), p.closingWindows.numberOfWindows())
			);
		}
		
		b2 = p.closingWindows.getBuffer();

		for (wid = 0; wid < p.closingWindows.numberOfWindows(); ++wid) {
			
			start1 =   openingWindows.getStartPointer(wid);
			start2 = p.closingWindows.getStartPointer(wid);
			
			end1 = (wid == edge1) ? b1.position() :   openingWindows.getStartPointer(wid + 1);
			end2 = (wid == edge2) ? b2.position() : p.closingWindows.getStartPointer(wid + 1);
			
			if (start1 == end1)
				throw new IllegalStateException ("error: empty opening window partial result");
			
			if (start2 == end2) {
				throw new IllegalStateException ("error: empty closing window partial result");
			}
			
			aggregateHashTables (b1, start1, end1, b2, start2, end2, operator, true);
			/* At this point, w3 contains a packed, complete window result.
			 * Append it directly to this node's complete windows. */
			if (w3 != null)
				completeWindows.append(w3);
		}
		
		p.closingWindows.nullify();
		
		/* There may be some opening windows left, in which case they are aggregated with node p's pending one.
		 * The result will be stored (prepended) in p's opening windows */
		if (wid < openingWindows.numberOfWindows()) {

			if (p.pendingWindows.numberOfWindows() != 1) {

				throw new RuntimeException ("error: there are opening windows left but next slot has no pending windows");
			}
			
			if (debug) {
				System.out.println(String.format("[DBG] aggregate %4d remaining opening windows with pending", 
						openingWindows.numberOfWindows() - wid)); 
			}
			
			b2 = p.pendingWindows.getBuffer();
			
			start2 = 0;
			end2 = b2.position();
			
			int nextOpenWindow = wid;
			int count = 0;
			
			while (nextOpenWindow < openingWindows.numberOfWindows()) {
				
				start1 = openingWindows.getStartPointer(nextOpenWindow);
				end1 = (nextOpenWindow == edge1) ? b1.position() : openingWindows.getStartPointer(nextOpenWindow + 1);
				
				if (start1 == end1)
					throw new IllegalStateException ("error: empty opening window partial result");
				
				aggregateHashTables (b1, start1, end1, b2, start2, end2, operator, false);
				
				/* At this point, w3 contains a hash table. Replace current window's hash table */
				openingWindows.getBuffer().position(start1);
				openingWindows.getBuffer().put(w3.array(), 0, w3.capacity());
				
				++nextOpenWindow;
				++count;
			}
			
			/* Prepend this opening windows (starting from `wid`) to node p's opening windows.
			 * We have to shift the start pointers of p's opening windows down.
			 * 
			 * There are `count` new windows. The window size equal the hash table size:
			 */
			int windowSize = w3.capacity();
			
			p.openingWindows.prepend(openingWindows, wid, count, windowSize);
			
			p.pendingWindows.nullify();
		}
		
		openingWindows.nullify();
	}
	
	private void aggregateHashTables (
		
		IQueryBuffer b1, int start1, int end1, 
		IQueryBuffer b2, int start2, int end2, 
		IAggregateOperator operator,
		boolean pack) {

		//System.out.println("Aggregate hash tables...pack " + pack);
		
		float value1, value2;
		int   count1, count2;
		
		int valueOffset1, valueOffset2;
		int countOffset1, countOffset2;
		
		int timestampOffset1, timestampOffset2;
		
/*		int tmp = 0;
		int j =0;
		while (tmp < b1.position()) {
			System.out.println((j++)*64 +", " + b1.getLong(tmp) +", " + b1.getLong(tmp + 8) +", " + b1.getLong(tmp+16) +" " + b1.getLong(tmp+24) +", " + b1.getFloat(tmp+32) +", " + b1.getFloat(tmp+36) + ", " + b1.getInt(tmp+40) +"! ");
			tmp+= 64;
		}
		System.out.println("New HashTABLE");
		tmp = 0;
		j =0;
		while (tmp < b2.position()) {
			System.out.println((j++)*64 +", " + b2.getLong(tmp) +", " + b2.getLong(tmp + 8) +", " + b2.getLong(tmp+16) +" " + b2.getLong(tmp+24) +", " + b2.getFloat(tmp+32) +", " + b2.getFloat(tmp+36) + ", " + b2.getInt(tmp+40) +"! ");
			tmp+= 64;
		}*/


		w3.clear();
		//if (pack) {
			/* Clear contents */
			for (int i = 0; i < w3.capacity(); i++)
				w3.put(i, (byte) 0);
			
			mergedHashTable.configure(w3, 0, w3.capacity(), 
					operator.getKeyLength(), operator.getValueLength());
			
		//}
		
		windowHashTable.configure (b2.getByteBuffer(), start2, end2, 
				operator.getKeyLength(), operator.getValueLength());
		
		int tupleSize = windowHashTable.getIntermediateTupleSize();


        WindowHashTableWrapper windowHashTable1 = new WindowHashTableWrapper();

        windowHashTable1.configure(b1.getByteBuffer(), start1, end1,
                operator.getKeyLength(), operator.getValueLength());

		int tmp = 0;

       /* System.out.println("--------------B1------------ ");
        System.out.println("Index, Timestamp, Occupancy, Key1 Key2, Value1, Value2, Count");
        for (int idx = start1; idx < end1; idx += tupleSize) {
            if (b1.getByteBuffer().get(idx) != 1) *//* Skip empty slot *//*
                continue;
            System.out.println(idx +", " + b1.getLong(windowHashTable1.getTimestampOffset(idx)) +", " + b1.getLong(idx) +", "
                    + b1.getLong(windowHashTable1.getKeyOffset(idx)) +" " + b1.getLong(windowHashTable1.getKeyOffset(idx)+8) +", " +
                    b1.getFloat(windowHashTable1.getValueOffset(idx)) +", " + b1.getFloat(windowHashTable1.getValueOffset(idx)+4) + ", "
                    + b1.getInt(windowHashTable1.getCountOffset(idx)) +"! ");
        }


        System.out.println("--------------B2------------ ");
        System.out.println("Index, Timestamp, Occupancy, Key1 Key2, Value1, Value2, Count");
        for (int idx = start2; idx < end2; idx += tupleSize) {
            if (b2.getByteBuffer().get(idx) != 1) *//* Skip empty slot *//*
                continue;
            System.out.println(idx +", " + b2.getLong(windowHashTable.getTimestampOffset(idx)) +", " + b2.getLong(idx) +", "
                    + b2.getLong(windowHashTable.getKeyOffset(idx)) +" " + b2.getLong(windowHashTable.getKeyOffset(idx)+8) +", " +
                    b2.getFloat(windowHashTable.getValueOffset(idx)) +", " + b2.getFloat(windowHashTable.getValueOffset(idx)+4) + ", "
                    + b2.getInt(windowHashTable.getCountOffset(idx)) +"! ");
        }*/



        /* Iterate over tuples in first table. Search for key in hash table.
		 * If found, merge the two entries. */
		for (int idx = start1; idx < end1; idx += tupleSize) {
			
			if (b1.getByteBuffer().get(idx) != 1) /* Skip empty slot */
				continue;
			

			b2found[0] = false;
			/* Search key in buffer b1 in the hash table stored in buffer b2.
			 * 
			 * Since the length of both tables is the same, `getKeyOffset()`
			 * should return the correct index.
			 */
			int b2pos = windowHashTable.findIndex(b1.array(),
					windowHashTable.getKeyOffset(idx), operator.getKeyLength(), b2found);



            if (b2pos < 0) {
				System.out.println("error: open-adress hash table is full");
				System.exit(1);
			}
			
			if (! b2found[0]) {
				
				if (pack) {
					/* Copy tuple based on output schema */

                    /*System.out.println(String.format("[DBG] Not found in B2 and pack."));
                    System.out.println(String.format("[DBG] B1 with index %d with key %d %d, writing in %d W3 ", idx, windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)),
                            windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)+8), idx));*/

                    /* Put timestamp */
					w3.putLong(b1.getLong(windowHashTable.getTimestampOffset(idx)));
					/* Put key */
					w3.put(b1.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength());
					/* Put value(s) */
					valueOffset1 = windowHashTable.getValueOffset(idx);
					for (int i = 0; i < operator.numberOfValues(); ++i) {
						
						valueOffset1 += (i * 4);
						value1 = b1.getFloat(valueOffset1);
						
						if (operator.getAggregationType(i) == AggregationType.AVG) {
							
							countOffset1 = windowHashTable.getCountOffset(idx);
							count1 = b1.getInt(countOffset1);
							
							value1 /= ((float) count1);
						}
						w3.putFloat(value1);
					}
					w3.put(operator.getOutputSchema().getPad());
					/* Do not put count */
				} else {
					/* Create a new hash table entry */
					w3found[0] = false;
					int w3pos = mergedHashTable.getIndex(b1.array(), mergedHashTable.getKeyOffset(idx), 
							operator.getKeyLength(), w3found);

                    //System.out.println(String.format("[DBG] w3pos is %6d and key is %6d %6d", w3pos, mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx)), mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx))));
                    /*System.out.println(String.format("[DBG] Not found in B2."));
                    System.out.println(String.format("[DBG] B1 with index %d with key %d %d, writing in %d W3 ", idx, windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)),
                            windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)+8), w3pos));*/

					/*if (w3found[0]) {
						//System.out.println("It already exists!");
						w3found[0] = false;
						w3pos = mergedHashTable.getIndex(b2.array(), mergedHashTable.getKeyOffset(idx), 
								operator.getKeyLength(), w3found, true);
					}*/
					
					if (w3pos < 0 || w3found[0])
						throw new IllegalStateException ("error: failed to insert new key in intermediate hash table");
					
					/* Store intermediate tuple is slot starting at `w3pos` */
					
					/* Mark occupancy */
					w3.put (w3pos, (byte) 1);
					timestampOffset1 = windowHashTable.getTimestampOffset (w3pos);
					w3.position (timestampOffset1);
					/* Put timestamp */
					w3.putLong(b1.getLong(windowHashTable.getTimestampOffset(idx)));
					/* Put key and value(s) */
					w3.put(b1.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength() + operator.getValueLength());
					/* Put count */
					w3.putInt(b1.getInt(windowHashTable.getCountOffset(idx)));
				}
				
			} else { /* Merge the two tuples */
				
				// System.out.println(String.format("[DBG] key found at %6d", b2pos));
				
				if (pack) {
					/* Copy tuple based on output schema */
                    /*System.out.println(String.format("[DBG] Found in B2 and pack."));
                    System.out.println(String.format("[DBG] B1 with index %d with key %d %d, found in %d B2 with key %d %d", idx, windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)),
                            windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)+8), b2pos, windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(b2pos)),
                            windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(b2pos)+8)));*/
                    /* Put timestamp */
					w3.putLong(b1.getLong(windowHashTable.getTimestampOffset(idx)));
					/* Put key */
					w3.put(b1.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength());
					/* Put value(s) */
					valueOffset1 = windowHashTable.getValueOffset(  idx);
					valueOffset2 = windowHashTable.getValueOffset(b2pos);
					for (int i = 0; i < operator.numberOfValues(); ++i) {
						
						valueOffset1 += (i * 4);
						value1 = b1.getFloat(valueOffset1);
						
						valueOffset2 += (i * 4);
						value2 = b2.getFloat(valueOffset2);
						
						AggregationType type = operator.getAggregationType(i);
						switch (type) {
						case CNT:
						case SUM:
							value1 += value2;
							break;
						case AVG:
							
							countOffset1 = windowHashTable.getCountOffset(  idx);
							count1 = b1.getInt(countOffset1);
							
							countOffset2 = windowHashTable.getCountOffset(b2pos);
							count2 = b2.getInt(countOffset2);
							
							value1 = ((value1 * count2) + (value2 * count1)) / ((float) (count1 + count2));
							break;
						case MIN:
							if (value2 < value1)
								value1 = value2;
							break;
						case MAX:
							if (value2 > value1)
								value1 = value2;
							break;
						default:
							throw new IllegalArgumentException("error: invalid aggregation type");
						}
						w3.putFloat(value1);
					}
					w3.put(operator.getOutputSchema().getPad());
					/* Do not put count */
				} else {
					/* Create a new hash table entry */
					w3found[0] = false;
					int w3pos = mergedHashTable.getIndex(b1.array(), mergedHashTable.getKeyOffset(idx), 
							operator.getKeyLength(), w3found);


                    //System.out.println(String.format("[DBG] w3pos is %6d and key is %6d %6d", w3pos, mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx)), mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx))));

                    /*System.out.println(String.format("[DBG] Found in B2."));
                    System.out.println(String.format("[DBG] B1 with index %d with key %d %d, found in %d B2 with key %d %d", idx, windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)),
                            windowHashTable1.getContent().getLong(windowHashTable1.getKeyOffset(idx)+8), b2pos,
                            windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(b2pos)),
                            windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(b2pos)+8)));
                    System.out.println(String.format("[DBG] Writing in w3pos at %6d and key is %6d %6d", w3pos, mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(w3pos)),
                            mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(w3pos)+8)));
*/

                    if (w3pos < 0 || w3found[0])
						throw new IllegalStateException ("error: failed to insert new key in intermediate hash table");
					
					/* Store intermediate tuple is slot starting at `w3pos` */
					
					/* Mark occupancy */
					w3.put (w3pos, (byte) 1);
					timestampOffset1 = windowHashTable.getTimestampOffset (w3pos);
					w3.position (timestampOffset1);
					/* Put timestamp */
					w3.putLong(b1.getLong(windowHashTable.getTimestampOffset(idx)));
					/* Put key */
					w3.put(b1.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength());
					/* Put value(s) */
					
					countOffset1 = windowHashTable.getCountOffset(  idx);
					count1 = b1.getInt(countOffset1);
					
					countOffset2 = windowHashTable.getCountOffset(b2pos);
					count2 = b2.getInt(countOffset2);
					
					valueOffset1 = windowHashTable.getValueOffset(  idx);
					valueOffset2 = windowHashTable.getValueOffset(b2pos);
					
					for (int i = 0; i < operator.numberOfValues(); ++i) {
						
						valueOffset1 += (i * 4);
						value1 = b1.getFloat(valueOffset1);
						
						valueOffset2 += (i * 4);
						value2 = b2.getFloat(valueOffset2);
						
						AggregationType type = operator.getAggregationType(i);
						switch (type) {
						case CNT:
						case SUM:
							value1 += value2;
							break;
						case AVG:	
							value1 = ((value1 * count2) + (value2 * count1)) / ((float) (count1 + count2));
							break;
						case MIN:
							if (value2 < value1)
								value1 = value2;
							break;
						case MAX:
							if (value2 > value1)
								value1 = value2;
							break;
						default:
							throw new IllegalArgumentException("error: invalid aggregation type");
						}
						w3.putFloat(value1);
					}
					/* Put count */
					w3.putInt(count1 + count2);
				}
				// Unmark occupancy in second buffer
				b2.getByteBuffer().put (b2pos, (byte) 0);
			}
		}


        /*System.out.println("--------------Remaining B2------------ ");
        System.out.println("Index, Timestamp, Occupancy, Key1 Key2, Value1, Value2, Count");
        for (int idx = start2; idx < end2; idx += tupleSize) {
            if (b2.getByteBuffer().get(idx) != 1) *//* Skip empty slot *//*
                continue;
            System.out.println(idx +", " + b2.getLong(windowHashTable.getTimestampOffset(idx)) +", " + b2.getLong(idx) +", "
                    + b2.getLong(windowHashTable.getKeyOffset(idx)) +" " + b2.getLong(windowHashTable.getKeyOffset(idx)+8) +", " +
                    b2.getFloat(windowHashTable.getValueOffset(idx)) +", " + b2.getFloat(windowHashTable.getValueOffset(idx)+4) + ", "
                    + b2.getInt(windowHashTable.getCountOffset(idx)) +"! ");
        }*/



		/* Iterate over the remaining tuples in the second table. */
		for (int idx = start2; idx < end2; idx += tupleSize) {
			
			if (b2.getByteBuffer().get(idx) != 1) /* Skip empty slot */
				continue;
			
			if (pack) {
				/* Copy tuple based on output schema */
                //System.out.println(String.format("[DBG] Skipping pack with B2."));

                /* Put timestamp */
				w3.putLong(b2.getLong(windowHashTable.getTimestampOffset(idx)));
				/* Put key */
				w3.put(b2.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength());
				/* Put value(s) */
				valueOffset2 = windowHashTable.getValueOffset(idx);
				for (int i = 0; i < operator.numberOfValues(); ++i) {
					
					valueOffset2 += (i * 4);
					value2 = b2.getFloat(valueOffset2);
					
					if (operator.getAggregationType(i) == AggregationType.AVG) {
						
						countOffset2 = windowHashTable.getCountOffset(idx);
						count2 = b1.getInt(countOffset2);
						
						value2 /= ((float) count2);
					}
					w3.putFloat(value2);
				}
				w3.put(operator.getOutputSchema().getPad());
				/* Do not put count */
			} else {
				/* Create a new hash table entry */
				w3found[0] = false;
				int w3pos = mergedHashTable.getIndex(b2.array(), mergedHashTable.getKeyOffset(idx), 
						operator.getKeyLength(), w3found);

                //System.out.println(String.format("[DBG] w3pos is %6d and key is %6d %6d", idx, windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(idx)), mergedHashTable.getContent().getLong(windowHashTable.getKeyOffset(idx))));

                //System.out.println(String.format("[DBG] w3pos is %6d and key is %6d %6d", w3pos, mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx)), mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(idx))));


                /*System.out.println(String.format("[DBG] Writing in W3 from B2."));
                System.out.println(String.format("[DBG] B2 with index %d with key %d %d, found in %d W3 with key %d %d", idx, windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(idx)),
                        windowHashTable.getContent().getLong(windowHashTable.getKeyOffset(idx)+8), w3pos, mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(w3pos)),
                        mergedHashTable.getContent().getLong(mergedHashTable.getKeyOffset(w3pos)+8)));*/

				/*tmp = 0;
				j =0;
				cont = mergedHashTable.getContent();
				while (tmp < cont.position()) {
					System.out.println((j++)*64+", " + cont.getLong(tmp) +", " + cont.getLong(tmp + 8) +", " + cont.getLong(tmp+16) +" " + cont.getLong(tmp+24) +", " + cont.getFloat(tmp+32) +", " + cont.getFloat(tmp+36) + ", " + cont.getInt(tmp+40) +"! ");
					tmp+= 64;
				}*/				
				
				/*if (w3found[0]) {
					//System.out.println("It already exists(!");
					w3found[0] = false;
					w3pos = mergedHashTable.getIndex(b2.array(), mergedHashTable.getKeyOffset(idx), 
							operator.getKeyLength(), w3found, true);
				}*/
				
				if (w3pos < 0 || w3found[0]) {
				//if (w3pos < 0) {

                        //throw new IllegalStateException("error: failed to insert new key in intermediate hash table at " + w3pos);
					System.err.println(String.format("George's error: position is %d and found is %s", w3pos, w3found[0]));
					System.exit(1);
				}
				
				/* Store intermediate tuple is slot starting at `w3pos` */
				
				/* Mark occupancy */
				w3.put (w3pos, (byte) 1);
				timestampOffset2 = windowHashTable.getTimestampOffset (w3pos);
				w3.position (timestampOffset2);
				/* Put timestamp */
				w3.putLong(b2.getLong(windowHashTable.getTimestampOffset(idx)));
				/* Put key and value(s) */
				w3.put(b2.array(), windowHashTable.getKeyOffset(idx), operator.getKeyLength() + operator.getValueLength());
				/* Put count */
				w3.putInt(b2.getInt(windowHashTable.getCountOffset(idx)));
			}
		}
		
/*		System.out.println("w3 hashtable");
		tmp = 0;
		j =0;
		while (tmp < w3.position()) {
			System.out.println((j++)*64+", " + w3.getLong(tmp) +", " + w3.getLong(tmp + 8) +", " + w3.getLong(tmp+16) +" " + w3.getLong(tmp+24) +", " + w3.getFloat(tmp+32) +", " + w3.getFloat(tmp+36) + ", " + w3.getInt(tmp+40) +"! ");
			tmp+= 64;
		}
		System.out.println("end of w3 hashtable");*/

	}
	
	public boolean isReady() {
		
		if (closingWindows.numberOfWindows() > 0)
			return false;
		if (openingWindows.numberOfWindows() > 0)
			return false;
		if (pendingWindows.numberOfWindows() > 0)
			return false;
		
		return true;
	}
	
	public String toString () {
		
		StringBuilder s = new StringBuilder();
		
		s.append(String.format("%4d", index));
		s.append(" [");
		s.append(String.format("%6d/", closingWindows.numberOfWindows()));
		s.append(String.format("%6d/",completeWindows.numberOfWindows()));
		s.append(String.format("%1d/", pendingWindows.numberOfWindows()));
		s.append(String.format("%6d" , openingWindows.numberOfWindows()));
		s.append("] ");
		
		s.append(String.format("free (%10d, %10d) ", freePointer1, freePointer2));
		s.append(String.format("GPU: %5s", GPU));
		
		return s.toString();
	}
}
