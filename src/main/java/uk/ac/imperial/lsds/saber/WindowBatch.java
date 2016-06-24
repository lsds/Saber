package uk.ac.imperial.lsds.saber;

import java.util.Arrays;

import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;

public class WindowBatch {
	
	private int batchSize;
	
	private int taskId;
	
	private int freePointer1;
	private int freePointer2;
	
	private Query query;
	
	private IQueryBuffer buffer;
	private WindowDefinition windowDefinition;
	private ITupleSchema schema;
	
	private int latencyMark;
	
	private int startPointer;
	private int   endPointer;
	
	private long streamStartPointer;
	private long   streamEndPointer;
	
	private long startTimestamp;
	private long   endTimestamp;
	
	private int [] windowStartPointers;
	private int []   windowEndPointers;
	
	private int lastWindowIndex;
	
	private boolean fragmentedWindows = false;
	private boolean hasPendingWindows = false;
	
	PartialWindowResults openingWindows, closingWindows, pendingWindows, completeWindows;
	
	private boolean initialised = false;
	
	public WindowBatch () {
		
		this (0, 0, Integer.MIN_VALUE, Integer.MIN_VALUE, null, null, null, null, 0);
	}
	
	public WindowBatch (
			int batchSize, 
			int taskId,
			int freePointer1,
			int freePointer2,
			Query query,
			IQueryBuffer buffer, 
			WindowDefinition windowDefinition, 
			ITupleSchema schema,
			int mark
		) {
		
		this.batchSize = batchSize;
		
		this.taskId = taskId;
		
		this.query = query;
		this.buffer = buffer;
		this.windowDefinition = windowDefinition;
		this.schema = schema;
		
		this.freePointer1 = freePointer1;
		this.freePointer2 = freePointer2;
		
		latencyMark = mark;
		
		startPointer = endPointer = -1;
		
		streamStartPointer = streamEndPointer = -1;
		
		startTimestamp = endTimestamp = -1;
		
		windowStartPointers = new int [SystemConf.PARTIAL_WINDOWS];
		windowEndPointers   = new int [SystemConf.PARTIAL_WINDOWS];
		
		lastWindowIndex = 0;
		
		fragmentedWindows = false;
		hasPendingWindows = false;
		
		closingWindows = pendingWindows = completeWindows = openingWindows = null;
		
		this.initialised = false;
	}
	
	public void set (
			int batchSize, 
			int taskId,
			int freePointer1,
			int freePointer2,
			Query query,
			IQueryBuffer buffer, 
			WindowDefinition windowDefinition, 
			ITupleSchema schema,
			int mark
		) {
		
		this.batchSize = batchSize;
		
		this.taskId = taskId;
		
		this.query = query;
		this.buffer = buffer;
		this.windowDefinition = windowDefinition;
		this.schema = schema;
		
		this.freePointer1 = freePointer1;
		this.freePointer2 = freePointer2;
		
		latencyMark = mark;
		
		startPointer = endPointer = -1;
		
		streamStartPointer = streamEndPointer = -1;
		
		startTimestamp = endTimestamp = -1;
		
		//
		// Note that the window start and end pointers are reset every 
		// time they are computed.
		//
		// (See `initPartialWindowPointers` method below)
		//
		// windowStartPointers = new int [SystemConf.PARTIAL_WINDOWS];
		// windowEndPointers   = new int [SystemConf.PARTIAL_WINDOWS];
		//
		lastWindowIndex = 0;
		
		fragmentedWindows = false;
		hasPendingWindows = false;
		
		closingWindows = pendingWindows = completeWindows = openingWindows = null;
		
		this.initialised = false;
	}
	
	public int getBatchSize () {
		return batchSize;
	}
	
	public int getTaskId () { 
		return taskId; 
	}
	
	public void setTaskId (int taskId) { 
		this.taskId = taskId;
	}
	
	public Query getQuery () { 
		return query;
	}
	
	public void setQuery (Query query) { 
		this.query = query; 
	}
	
	public IQueryBuffer getBuffer () {
		return buffer;
	}
	
	public void setBuffer (IQueryBuffer buffer) {
		this.buffer = buffer;
	}
	
	public ITupleSchema getSchema () {
		return schema;
	}
	
	public void setSchema (ITupleSchema schema) {
		this.schema = schema;
	}
		
	public WindowDefinition getWindowDefinition () {
		return windowDefinition;
	}
	
	public int getFreePointer () { 
		return freePointer1; 
	}
	
	public int getFirstFreePointer () {
		return getFreePointer();
	}
	
	public int getSecondFreePointer () {
		return freePointer2;
	}
	
	public int getBufferStartPointer () {
		return startPointer;
	}
	
	public int getBufferEndPointer () {
		return endPointer;
	}
	
	public void setBufferPointers (int start, int end) {
		startPointer = start;
		endPointer = end;
	}
	
	public long getStreamStartPointer () {
		return streamStartPointer;
	}
	
	public long getStreamEndPointer () {
		return streamEndPointer;
	}
	
	public void setStreamPointers (long start, long end) {
		streamStartPointer = start;
		streamEndPointer = end;
	}
	
	public long getBatchStartTimestamp () {
		return startTimestamp;
	}
	
	public long getBatchEndTimestamp () {
		return endTimestamp;
	}
	
	public void setBatchTimestamps (long start, long end) {
		startTimestamp = start;
		endTimestamp = end;
	}
	
	public int getLatencyMark () {
		return latencyMark;
	}
	
	public void setLatencyMark (int mark) {
		latencyMark  = mark;
	}
	
	public int [] getWindowStartPointers () {
		return windowStartPointers;
	}
	
	public int [] getWindowEndPointers () {
		return windowEndPointers;
	}
	
	public boolean containsFragmentedWindows () {
		return fragmentedWindows;
	}
	
	public boolean containsPendingWindows () {
		return hasPendingWindows;
	}
	
	public PartialWindowResults getOpeningWindows () {
		return openingWindows;
	}
	
	public void setOpeningWindows (PartialWindowResults results) {
		fragmentedWindows = true;
		openingWindows = results;
	}
	
	public PartialWindowResults getClosingWindows () {
		return closingWindows;
	}

	public void setClosingWindows (PartialWindowResults results) {
		fragmentedWindows = true;
		closingWindows = results;
	}
	
	public PartialWindowResults getPendingWindows () {
		return pendingWindows;
	}

	public void setPendingWindows (PartialWindowResults results) {
		fragmentedWindows = true;
		pendingWindows = results;
	}

	public PartialWindowResults getCompleteWindows () {
		return completeWindows;
	}

	public void setCompleteWindows (PartialWindowResults results) {
		fragmentedWindows = true;
		completeWindows = results;
	}
	
	public int getLastWindowIndex () {
		return lastWindowIndex;
	}
	
	public int getInt (int offset, int attribute) {
		int index = offset + schema.getAttributeOffset(attribute);
		return buffer.getInt (index);
	}
	
	public float getFloat (int offset, int attribute) {
		int index = offset + schema.getAttributeOffset(attribute);
		return buffer.getFloat (index);
	}
	
	public long getLong (int offset, int attribute) {
		int index = offset + schema.getAttributeOffset(attribute);
		return buffer.getLong (index);
	}
	
	public void clear () {
		initialised = false;
	}
	
	/*
	 * Normalize a pointer to a location of the underlying byte buffer.
	 * 
	 * Avoids "out of bounds" memory accesses, especially when we copy 
	 * memory via the Unsafe interface.
	 */
	public int normalise (int pointer) {
		return buffer.normalise((long) pointer);
	}
	
	public long getTimestamp (int index) {
		long value = buffer.getLong(index);
		if (SystemConf.LATENCY_ON)
			return (long) Utils.getTupleTimestamp(value);
		else 
			return value;
	}
	
	public void initPartialWindowPointers () {
		
		if (initialised)
			throw new IllegalStateException ("error: batch window pointers already initialised");
		
		if (windowDefinition.isRangeBased())
			initPartialRangeBasedWindowPointers ();
		else
			initPartialCountBasedWindowPointers ();
		
		initialised = true;
	}
	
	public void initPartialRangeBasedWindowPointers () {
		
		int tupleSize = schema.getTupleSize ();
		long paneSize = windowDefinition.getPaneSize ();
		
		Arrays.fill(windowStartPointers, -1);
		Arrays.fill(  windowEndPointers, -1);
		
		long streamPtr;
		int  bufferPtr;
		
		/* Previous, next, and current pane ids */
		long _pid, pid_, pid = 0;
		
		long pane;
		
		/* Current window */
		long wid;
		
		long offset = -1;
		
		int numberOfOpeningWindows = 0; /* Counters */
		int numberOfClosingWindows = 0;
		
		/* Set previous pane id */
		if (streamStartPointer == 0) {
			_pid = -1;
		} else {
			/* Check the last tuple of the previous batch */
			_pid = (getTimestamp(startPointer - schema.getTupleSize()) / paneSize);
		}
		
		/* Set offset */
		if (this.streamStartPointer == 0)
			offset = 0;
		
		for (streamPtr = streamStartPointer, bufferPtr = startPointer; streamPtr < streamEndPointer && bufferPtr < endPointer; 
			streamPtr += tupleSize, bufferPtr += tupleSize) {
			
			pid = getTimestamp(bufferPtr) / paneSize; /* Current pane */
			
			if (_pid < pid) {
				/* Pane `_pid` closed; pane `pid` opened; iterate over panes in between */
				while (_pid < pid) {
					
					pid_ = _pid + 1;
					
					/* Check if a window closes at this pane */
					pane = pid_ - windowDefinition.numberOfPanes();
					
					if (pane >= 0 && pane % windowDefinition.panesPerSlide() == 0) {
						
						wid = pane / windowDefinition.panesPerSlide();
						if (wid >= 0) {
							
							/* Calculate offset */
							if (offset < 0) {
								offset = wid;
							} else {
								/* The offset has already been set */
								if (numberOfClosingWindows == 0 && streamStartPointer != 0) {
									/* Shift down */
									int delta = (int) (offset - wid);
									for (int i = lastWindowIndex; i >= 0; i--) {
										windowStartPointers[i + delta] = windowStartPointers[i];
										windowEndPointers  [i + delta] = windowEndPointers  [i];
									}
									for (int i = 0; i < delta; i++) {
										windowStartPointers[i] = -1;
										windowEndPointers  [i] = -1;
									}
									/* Set last window index */
									lastWindowIndex += delta;
									/* Reset offset */
									offset = wid;
								}
							}
							
							int index = (int) (wid - offset);
							if (index < 0) {
								System.err.println("error: failed to close window " + wid);
								System.exit(1);
							}
							/* Store end pointer */
							windowEndPointers[index] = bufferPtr;
							
							numberOfClosingWindows += 1;
							/*
							 * Has this window been previously opened?
							 * 
							 * We characterise this window as "closing" and we expect to find its
							 * match in the opening set of the previous batch. But if this is the
							 * first batch, then there will be none.
							 */
							if (windowStartPointers[index] < 0 && streamStartPointer == 0)
								windowStartPointers[index] = 0;
							
							lastWindowIndex = (lastWindowIndex < index) ? index : lastWindowIndex;
						}
					}
					
					/* Check if a window opens at `pid_` */
					
					if (pid_ % windowDefinition.panesPerSlide() == 0) {
					
						wid = pid_ / windowDefinition.panesPerSlide();
						
						/* Calculate offset */
						if (offset < 0) {
							offset = wid;
						}
						/* Store start pointer */
						int index = (int) (wid - offset);
						windowStartPointers[index] = bufferPtr;
						
						numberOfOpeningWindows += 1;
						
						lastWindowIndex = (lastWindowIndex < index) ? index : lastWindowIndex;
					}
					
					_pid += 1;
				} /* End while */
				
				_pid = pid;
			} /* End if */
		} /* End for */
		
		if (numberOfOpeningWindows > 0 && numberOfClosingWindows == 0 && streamStartPointer != 0) {
			/* There are no closing windows. Therefore, windows that 
			 * have opened in a previous batch, should be considered
			 * as pending. */
			
			for (int i = lastWindowIndex; i >= 0; i--) {
				windowStartPointers[i + 1] = windowStartPointers[i];
				windowEndPointers  [i + 1] = windowEndPointers  [i];
			}
			/* Set pending window */
			windowStartPointers[0] = -1;
			windowEndPointers  [0] = -1;
			/* Increment last window index */
			lastWindowIndex ++;	
		} else if (numberOfOpeningWindows == 0 && numberOfClosingWindows == 0) {
			/* There are only pending windows in the batch */
			lastWindowIndex = 0;
		}
	}
	
	public void initPartialCountBasedWindowPointers () {
		
		int tupleSize = schema.getTupleSize ();
		long paneSize = windowDefinition.getPaneSize();
		
		Arrays.fill(windowStartPointers, -1);
		Arrays.fill(  windowEndPointers, -1);
		
		long streamPtr;
		int  bufferPtr;
		
		/* Previous, next, and current pane ids */
		long _pid, pid_, pid = 0;
		
		long pane; /* Normalised to panes/window */
		
		/* Current window */
		long wid;
		
		long offset = -1;
		
		int numberOfOpeningWindows = 0; /* Counters */
		int numberOfClosingWindows = 0;
		
		/* Set previous pane id */
		if (streamStartPointer == 0) {
			_pid = -1;
		} else {
			_pid = ((streamStartPointer / tupleSize) / paneSize) - 1;
		}
		
		/* Set offset */
		if (this.streamStartPointer == 0)
			offset = 0;
		
		for (streamPtr = streamStartPointer, bufferPtr = startPointer; streamPtr < streamEndPointer && bufferPtr < endPointer; 
			streamPtr += tupleSize, bufferPtr += tupleSize) {
			
			/* Current pane */
			pid = (streamPtr / tupleSize) / paneSize;
			
			if (_pid < pid) {
				/* Pane `_pid` closed; pane `pid` opened; iterate over panes in between... */
				while (_pid < pid) {
					
					pid_ = _pid + 1;
					
					/* Check if a window closes at this pane */
					pane = pid_ - windowDefinition.numberOfPanes();
					
					if (pane >= 0 && (pane % windowDefinition.panesPerSlide() == 0)) {
						
						wid = pane / windowDefinition.panesPerSlide();
						if (wid >= 0) {
							/* Calculate offset */
							if (offset < 0) {
								offset = wid;
							} else {
								/* The offset has already been set */
								if (numberOfClosingWindows == 0 && streamStartPointer != 0) {
									/* Shift down */
									int delta = (int) (offset - wid);
									for (int i = lastWindowIndex; i >= 0; i--) {
										windowStartPointers[i + delta] = windowStartPointers[i];
										windowEndPointers  [i + delta] = windowEndPointers  [i];
									}
									for (int i = 0; i < delta; i++) {
										windowStartPointers[i] = -1;
										windowEndPointers  [i] = -1;
									}
									/* Set last window index */
									lastWindowIndex += delta;
									/* Reset offset */
									offset = wid;
								}
							}
							int index = (int) (wid - offset);
							if (index < 0) {
								System.err.println("error: failed to close window " + wid);
								System.exit(1);
							}
							/* Store end pointer */
							windowEndPointers[index] = bufferPtr;
							numberOfClosingWindows += 1;
							/*
							 * Has this window been previously opened?
							 * 
							 * We characterise this window as "closing" and we expect to find its
							 * match in the opening set of the previous batch. But if this is the
							 * first batch, then there will be none.
							 */
							if (windowStartPointers[index] < 0 && streamStartPointer == 0)
								windowStartPointers[index] = 0;
							
							lastWindowIndex = (lastWindowIndex < index) ? index : lastWindowIndex;
						}
					}
					
					/* Check if a window opens at `pid_` */
					
					if (pid_ % windowDefinition.panesPerSlide() == 0) {
					
						wid = pid_ / windowDefinition.panesPerSlide();
						/* Calculate offset */
						if (offset < 0) {
							offset = wid;
						}
						/* Store start pointer */
						int index = (int) (wid - offset);
						windowStartPointers[index] = bufferPtr;
						
						numberOfOpeningWindows += 1;
						
						lastWindowIndex = (lastWindowIndex < index) ? index : lastWindowIndex;
					}
					
					_pid += 1;
				} /* End while */
				
				_pid = pid;
			} /* End if */
		} /* End for */
		
		if (numberOfOpeningWindows > 0 && numberOfClosingWindows == 0 && streamStartPointer != 0) {
			/* There are no closing windows. Therefore, windows that 
			 * have opened in a previous batch, should be considered
			 * as pending. */
			
			for (int i = lastWindowIndex; i >= 0; i--) {
				windowStartPointers[i + 1] = windowStartPointers[i];
				windowEndPointers  [i + 1] = windowEndPointers  [i];
			}
			
			/* Set pending window */
			windowStartPointers[0] = -1;
			windowEndPointers  [0] = -1;
			
			/* Increment last window index */
			lastWindowIndex ++;
			
		} else if (numberOfOpeningWindows == 0 && numberOfClosingWindows == 0) {
			/* There are only pending windows in the batch */
			lastWindowIndex = 0;
		}
	}
}
