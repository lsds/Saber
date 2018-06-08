package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class Aggregation implements IOperatorCode, IAggregateOperator {
	
	private static final boolean debug = false;
	WindowDefinition windowDefinition;
	
	private AggregationType [] aggregationTypes;

	private FloatColumnReference [] aggregationAttributes;
	
	private LongColumnReference timestampReference;
	
	private Expression [] groupByAttributes;
	private boolean groupBy = false;
	
	private boolean processIncremental;
	
	ITupleSchema outputSchema;
	
	private int keyLength, valueLength;
	
	/* Thread local variables */
	private ThreadLocal<float   []> tl_values;
	private ThreadLocal<int     []> tl_counts;
	private ThreadLocal<byte    []> tl_tuplekey;
	private ThreadLocal<boolean []> tl_found;
	
	public Aggregation (WindowDefinition windowDefinition) {
		
		this.windowDefinition = windowDefinition;
		
		aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.CNT;
		
		aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(1);
		
		timestampReference = new LongColumnReference(0);
		
		groupByAttributes = null;
		groupBy = false;
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[3]; /* +1 for count */
		
		outputAttributes[0] = timestampReference;
		outputAttributes[1] = new FloatColumnReference(1);
		outputAttributes[2] = new IntColumnReference(2); /* count */
		
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
		
		keyLength = 0;
		valueLength = 4;
		
		tl_values = new ThreadLocal<float []> () {
			@Override protected float [] initialValue () {
				return new float [numberOfValues()];
		    }
		};
		
		tl_counts = new ThreadLocal<int []> () {
			@Override protected int [] initialValue () {
				return new int [numberOfValues()];
		    }
		};
	}
	
	public Aggregation (WindowDefinition windowDefinition, 
		
		AggregationType aggregationType, FloatColumnReference aggregationAttribute) {
		
		this.windowDefinition = windowDefinition;
		
		aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = aggregationType;
		
		aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = aggregationAttribute;
		
		timestampReference = new LongColumnReference(0);
		
		groupByAttributes = null;
		groupBy = false;
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[3]; /* +1 for count */
		
		outputAttributes[0] = timestampReference;
		outputAttributes[1] = new FloatColumnReference(1);
		outputAttributes[2] = new IntColumnReference(2); /* count */
		
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		if (
			aggregationTypes[0] == AggregationType.CNT || 
			aggregationTypes[0] == AggregationType.SUM || 
			aggregationTypes[0] == AggregationType.AVG) {
			
			processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
		}
		
		keyLength = 0;
		valueLength = 4;
		
		tl_values = new ThreadLocal<float []> () {
			@Override protected float [] initialValue () {
				return new float [numberOfValues()];
		    }
		};
		
		tl_counts = new ThreadLocal<int []> () {
			@Override protected int [] initialValue () {
				return new int [numberOfValues()];
		    }
		};
	}
	
	public Aggregation (WindowDefinition windowDefinition, 

		AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes) {

		this.windowDefinition = windowDefinition;
		this.aggregationTypes = aggregationTypes;
		this.aggregationAttributes = aggregationAttributes;
		
		groupByAttributes = null;
		groupBy = false;
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
		Expression [] outputAttributes = new Expression[1 + aggregationAttributes.length + 1]; /* +1 for count */

		outputAttributes[0] = timestampReference;
		for (int i = 1; i < outputAttributes.length - 1; ++i)
			outputAttributes[i] = new FloatColumnReference(i);
		outputAttributes[outputAttributes.length - 1] = new IntColumnReference(outputAttributes.length - 1); /* count */

		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		boolean containsIncrementalAggregationType = true;
		for (int i = 0; i < aggregationTypes.length; ++i) {
			if (
				aggregationTypes[i] != AggregationType.CNT && 
				aggregationTypes[i] != AggregationType.SUM && 
				aggregationTypes[i] != AggregationType.AVG) {
				
				containsIncrementalAggregationType = false;
				break;
			}
		}
		
		if (containsIncrementalAggregationType)
			processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
		else
			processIncremental = false;

		keyLength = 0;
		valueLength = 4 * aggregationAttributes.length;
		
		tl_values = new ThreadLocal<float []> () {
			@Override protected float [] initialValue () {
				return new float [numberOfValues()];
		    }
		};
		
		tl_counts = new ThreadLocal<int []> () {
			@Override protected int [] initialValue () {
				return new int [numberOfValues()];
		    }
		};
	}
	
	public Aggregation (WindowDefinition windowDefinition, 
		
		AggregationType [] aggregationTypes, FloatColumnReference [] aggregationAttributes, Expression [] groupByAttributes) {
		
		this.windowDefinition = windowDefinition;
		this.aggregationTypes = aggregationTypes;
		this.aggregationAttributes = aggregationAttributes;
		this.groupByAttributes = groupByAttributes;
		
		if (groupByAttributes != null)
			groupBy = true;
		else
			groupBy = false;
		
		timestampReference = new LongColumnReference(0);
		
		/* Create output schema */
		
		int numberOfKeyAttributes;
		if (groupByAttributes != null)
			numberOfKeyAttributes = groupByAttributes.length;
		else
			numberOfKeyAttributes = 0;
		
		int n = 1 + numberOfKeyAttributes + aggregationAttributes.length;
		
		int numberOfOutputAttributes = n;
		if (groupByAttributes == null)
			numberOfOutputAttributes += 1; /* +1 for count */
		
		Expression [] outputAttributes = new Expression[numberOfOutputAttributes];
		
		/* The first attribute is the timestamp */
		outputAttributes[0] = timestampReference;
		
		keyLength = 0;
		
		if (numberOfKeyAttributes > 0) {
			
			for (int i = 1; i <= numberOfKeyAttributes; ++i) {
				
				Expression e = groupByAttributes[i - 1];
				     if (e instanceof      IntExpression) { outputAttributes[i] = new      IntColumnReference(i); keyLength += 4; }
				else if (e instanceof  	  LongExpression) { outputAttributes[i] = new     LongColumnReference(i); keyLength += 8; }
				else if (e instanceof 	 FloatExpression) { outputAttributes[i] = new    FloatColumnReference(i); keyLength += 4; }
				else if (e instanceof LongLongExpression) { outputAttributes[i] = new LongLongColumnReference(i); keyLength += 16; }
				else
					throw new IllegalArgumentException("error: invalid group-by attribute");
			}
		}
		
		for (int i = numberOfKeyAttributes + 1; i < n; ++i)
			outputAttributes[i] = new FloatColumnReference(i);
		
		/* Set count attribute */
		if (groupByAttributes == null)
			outputAttributes[numberOfOutputAttributes - 1] = new IntColumnReference(numberOfOutputAttributes - 1);
		
		this.outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		boolean containsIncrementalAggregationType = true;
		for (int i = 0; i < aggregationTypes.length; ++i) {
			if (
				aggregationTypes[i] != AggregationType.CNT && 
				aggregationTypes[i] != AggregationType.SUM && 
				aggregationTypes[i] != AggregationType.AVG) {
				
				containsIncrementalAggregationType = false;
				break;
			}
		}
		
		if (containsIncrementalAggregationType) {
			System.out.println("[DBG] operator contains incremental aggregation type");
			processIncremental = (windowDefinition.getSlide() < windowDefinition.getSize() / 2);
		} else {
			processIncremental = false;
		}
		
		valueLength = 4 * aggregationTypes.length;
				
		tl_values = new ThreadLocal<float []> () {
			@Override protected float [] initialValue () {
				return new float [numberOfValues()];
		    }
		};
		
		tl_counts = new ThreadLocal<int []> () {
			@Override protected int [] initialValue () {
				return new int [numberOfValues()];
		    }
		};
		
		if (groupByAttributes != null) {
			tl_tuplekey = new ThreadLocal <byte []> () {
				@Override protected byte [] initialValue () {
					return new byte [keyLength];
				}
			};
		
			tl_found = new ThreadLocal<boolean []> () {
				@Override protected boolean [] initialValue () {
					return new boolean [1];
				}
			};
		}
	}
	
	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		s.append("[Partial window u-aggregation] ");
		for (int i = 0; i < aggregationTypes.length; ++i)
			s.append(aggregationTypes[i].asString(aggregationAttributes[i].toString())).append(" ");
		s.append("(group-by ?").append(" ").append(groupBy).append(") ");
		s.append("(incremental ?").append(" ").append(processIncremental).append(") ");
		return s.toString();
	}
	
	public boolean hasGroupBy () {
		return groupBy;
	}
	
	public ITupleSchema getOutputSchema () {
		return outputSchema;
	}
	
	public int getKeyLength () {
		return keyLength;
	}

	public int getValueLength () {
		return valueLength;
	}

	public int numberOfValues () {
		return aggregationAttributes.length;
	}
	
	public AggregationType getAggregationType () {
		return getAggregationType (0);
	}
	
	public AggregationType getAggregationType (int idx) {
		if (idx < 0 || idx > aggregationTypes.length - 1)
			throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
		return aggregationTypes[idx];
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		batch.initPartialWindowPointers();
		
		if (debug) {
			System.out.println(
				String.format("[DBG] aggregation task %6d: batch starts at %10d (%10d) ends at %10d (%10d)", batch.getTaskId(),
				batch.getBufferStartPointer(),
				batch.getStreamStartPointer(),
				batch.getBufferEndPointer(),
				batch.getStreamEndPointer()
				)
			);
		}
		
		if (! this.groupBy) {
			if (processIncremental) { 
				processDataPerWindowIncrementally (batch, api);
			} else {
				processDataPerWindow (batch, api);
			}
		} else {
			if (processIncremental) { 
				processDataPerWindowIncrementallyWithGroupBy (batch, api);
			} else {
				processDataPerWindowWithGroupBy (batch, api);
			}
		}
		
		if (debug) {
			System.out.println(
				String.format("[DBG] aggregation task %6d: %4d closing %4d pending %4d complete and %4d opening windows]", batch.getTaskId(),
					batch.getClosingWindows ().numberOfWindows(),
					batch.getPendingWindows ().numberOfWindows(),
					batch.getCompleteWindows().numberOfWindows(),
					batch.getOpeningWindows ().numberOfWindows()
				)
			);
		}
		
		batch.getBuffer().release();
		batch.setSchema(outputSchema);
		
		api.outputWindowBatchResult(batch);
	}
	
	private void processDataPerWindow (WindowBatch batch, IWindowAPI api) {
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();
		
		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;
		
		/* Current window start and end pointers */
		int start, end;
		
		long timestampValue;
		float value;
		
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();		
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
				// System.out.println(String.format("[DBG] task %6d closing window: [%10d, %10d]", batch.getTaskId(), start, end));
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			if (start == end) {
				/* Store "null" (zero-valued) tuple in output buffer */
				outputBuffer.putLong(0L);
				for (int i = 0; i < numberOfValues(); ++i) {
					outputBuffer.putFloat(0);
				}
				outputBuffer.putInt(0);
				/* Move to next window */
				continue;
			}
			
			float [] values = tl_values.get(); 
			int [] counts = tl_counts.get();
			
			/* Process first tuple */
			timestampValue = timestampReference.eval(inputBuffer, inputSchema, start);
			for (int i = 0; i < numberOfValues(); ++i) {
				if (aggregationTypes[i] == AggregationType.CNT)
					values[i] = 1;
				else
					values[i] = aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
				counts[i] = 1;
			}
			/* Move pointer to second tuple */
			start += inputTupleSize;
			/* For all remaining tuples... */
			while (start < end) {
				for (int i = 0; i < numberOfValues(); ++i) {
					if (aggregationTypes[i] == AggregationType.CNT) {
						values[i] += 1;
					} else {
						value = aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
						switch (aggregationTypes[i]) {
						case MAX: values[i] = (value <= values[i]) ? values[i] : value;
						case MIN: values[i] = (value <= values[i]) ? values[i] : value;
						case SUM:
						case AVG: values[i] += value; break;
						default:
							throw new IllegalArgumentException("error: invalid aggregation type");
						}
					}
					counts[i] += 1;
				}
				start += inputTupleSize;
			}
			/* Compute average, if any */
			for (int i = 0; i < numberOfValues(); ++i) {
				if (aggregationTypes[i] == AggregationType.AVG) {
					values[i] /= ((float) counts[i]);
				}
			}
			/* Store window result in output buffer */
			outputBuffer.putLong(timestampValue);
			for (int i = 0; i < numberOfValues(); ++i) {
				outputBuffer.putFloat(values[i]);
			}
			outputBuffer.putInt(counts[0]);
		}
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}
	
	private void processDataPerWindowIncrementally (WindowBatch batch, IWindowAPI api) {
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();
		
		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;
		
		/* Current window start and end pointers */
		int start, end;
		/* Previous window start and end pointers */
		int _start = -1;
		int   _end = -1;
		
		long timestampValue;
		
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();					
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			if (start == end) {
				/* Store "null" (zero-valued) tuple in output buffer */
				outputBuffer.putLong(0L);
				for (int i = 0; i < numberOfValues(); ++i) {
					outputBuffer.putFloat(0);
				}
				outputBuffer.putInt(0);
				/* Move to next window */
				continue;
			}
			
			float [] values = tl_values.get(); 
			int [] counts = tl_counts.get();
			
			if (_start >= 0) {
				/* Process tuples in current window that have not been in the previous window */
				for (int p = _end; p < end; p += inputTupleSize) {
					for (int i = 0; i < numberOfValues(); ++i) {
						if (aggregationTypes[i] == AggregationType.CNT)
							values[i] += 1;
						else
							values[i] += aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
						counts[i] += 1;
					}
				}
			} else {
				/* Process tuples in current window */
				for (int i = 0; i < numberOfValues(); ++i) {
					if (aggregationTypes[i] == AggregationType.CNT)
						values[i] = 1;
					else
						values[i] = aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
					counts[i] = 1;
				}
				/* Move pointer to second tuple */
				int p = start + inputTupleSize;
				/* For all remaining tuples... */
				while (p < end) {
					for (int i = 0; i < numberOfValues(); ++i) {
						if (aggregationTypes[i] == AggregationType.CNT)
							values[i] += 1;
						else
							values[i] += aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
						counts[i] += 1;
					}
					p += inputTupleSize;
				}
			}
			
			/* Process tuples in previous window that are not in current window */
			if (_start >= 0) {
				for (int p = _start; p < start; p += inputTupleSize) {
					for (int i = 0; i < numberOfValues(); ++i) {
						if (aggregationTypes[i] == AggregationType.CNT)
							values[i] -= 1;
						else
							values[i] -= aggregationAttributes[i].eval(inputBuffer, inputSchema, start);
						counts[i] -= 1;
					}
				}
			}
			/* Compute average, if any */
			for (int i = 0; i < numberOfValues(); ++i) {
				if (aggregationTypes[i] == AggregationType.AVG) {
					values[i] /= ((float) counts[i]);
				}
			}
			timestampValue = this.timestampReference.eval(inputBuffer, inputSchema, start);
			/* Store window result in output buffer */
			outputBuffer.putLong(timestampValue);
			for (int i = 0; i < numberOfValues(); ++i) {
				outputBuffer.putFloat(values[i]);
			}
			outputBuffer.putInt(counts[0]);
			
			/* Continue with the next window */
			_start = start;
			_end = end;
		}
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}
	
	private void setGroupByKey (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		int pivot = 0;
		for (int i = 0; i < groupByAttributes.length; i++) {
			pivot = groupByAttributes[i].evalAsByteArray (buffer, schema, offset, bytes, pivot);
		}
	}
	
	private void processDataPerWindowWithGroupBy (WindowBatch batch, IWindowAPI api) {
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();
		
		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;
		
		/* Current window start and end pointers */
		int start, end;
		
		WindowHashTable windowHashTable;
		byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
		boolean [] found = (boolean []) tl_found.get(); // new boolean[1];
		boolean pack = false;
		
		float [] values = tl_values.get(); 
		
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;
			
			pack = false;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
				pack = true;
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			windowHashTable = WindowHashTableFactory.newInstance(workerId);
			windowHashTable.setTupleLength(keyLength, valueLength);
			
			while (start < end) {
				/* Get the group-by key */
				setGroupByKey (inputBuffer, inputSchema, start, tupleKey);
				/* Get values */
				for (int i = 0; i < numberOfValues(); ++i) {
					if (aggregationTypes[i] == AggregationType.CNT)
						values[i] = 1;
					else
						values[i] = aggregationAttributes[i].eval (inputBuffer, inputSchema, start);
				}
				
				/* Check whether there is already an entry in the hash table for this key. 
				 * If not, create a new entry */
				found[0] = false;
				int idx = windowHashTable.getIndex (tupleKey, found);
				if (idx < 0) {
					System.out.println("error: open-adress hash table is full");
					System.exit(1);
				}
				
				ByteBuffer theTable = windowHashTable.getBuffer();
				if (! found[0]) {
					theTable.put (idx, (byte) 1);
					int timestampOffset = windowHashTable.getTimestampOffset (idx);
					theTable.position (timestampOffset);
					/* Store timestamp */
					theTable.putLong (inputBuffer.getLong(start));
					/* Store key and value(s) */
					theTable.put (tupleKey);
					for (int i = 0; i < numberOfValues(); ++i)
						theTable.putFloat(values[i]);
					/* Store count */
					theTable.putInt(1);
				} else {
					/* Update existing entry */
					int valueOffset = windowHashTable.getValueOffset (idx);
					int countOffset = windowHashTable.getCountOffset (idx);
					/* Store value(s) */
					float v;
					int p;
					for (int i = 0; i < numberOfValues(); ++i) {
						p = valueOffset + i * 4;
						switch (aggregationTypes[i]) {
						case CNT:
							theTable.putFloat(p, (theTable.getFloat(p) + 1));
							break;
						case SUM:
						case AVG:
							theTable.putFloat(p, (theTable.getFloat(p) + values[i]));
						case MIN:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v > values[i]) ? values[i] : v));
							break;
						case MAX:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v < values[i]) ? values[i] : v));
							break;
						default:
							throw new IllegalArgumentException ("error: invalid aggregation type");
						}
					}
					/* Increment tuple count */
					theTable.putInt(countOffset, theTable.getInt(countOffset) + 1);
				}
				/* Move to next tuple in window */
				start += inputTupleSize;
			}
			/* Store window result and move to next window */
			evaluateWindow (windowHashTable, outputBuffer, pack);
			/* Release hash maps */
			windowHashTable.release();
		}
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}
	
	private void evaluateWindow (WindowHashTable windowHashTable, IQueryBuffer buffer, boolean pack) {
		
		/* Write current window results to output buffer; copy the entire hash table */
		if (! pack) {
			buffer.put(windowHashTable.getBuffer().array());
			return;
		}
		
		/* Set complete windows */
		//System.out.println("Complete windows start at " + buffer.position());
		
		ByteBuffer theTable = windowHashTable.getBuffer();
		int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
		/* Pack the elements of the table */
		/*int tupleIndex = 0;
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
			if (theTable.get(idx) == 1) {
				int mark = theTable.get(idx);
				long timestamp = theTable.getLong(idx + 8);
				int key = theTable.getInt(idx + 16);
				float val1 = theTable.getFloat(idx + 20);
				float val2 = theTable.getFloat(idx + 24);
				int count = theTable.getInt(idx + 28);
				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d", 
						tupleIndex, 
						mark,
						timestamp,
						key,
						val1,
						val2,
						count
						));
			}
			tupleIndex ++;
		}*/
		/*System.exit(1);*/
//		int tupleIndex = 0;
//		for (int idx = offset; idx < (offset + SystemConf.HASH_TABLE_SIZE); idx += 32) {
//			int mark = buffer.getInt(idx + 0);
//			if (mark > 0) {
//				long timestamp = buffer.getLong(idx + 8);
//				//
//				// int key_1
//				// float value1
//				// float value2
//				// int count
//				//
//				int key = buffer.getInt(idx + 16);
//				float val1 = buffer.getFloat(idx + 20);
//				float val2 = buffer.getFloat(idx + 24);
//				int count = buffer.getInt(idx + 28);
//				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d", 
//					tupleIndex, 
//					Integer.reverseBytes(mark),
//					Long.reverseBytes(timestamp),
//					Integer.reverseBytes(key),
//					0F,
//					0F,
//					Integer.reverseBytes(count)
//				));
//			}
//			tupleIndex ++;
//		}
		
		// ByteBuffer theTable = windowHashTable.getBuffer();
		// int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
		/* Pack the elements of the table */
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
			
			if (theTable.get(idx) != 1) /* Skip empty slots */
				continue;
			
			/* Store timestamp, and key */
			int timestampOffset = windowHashTable.getTimestampOffset (idx);
			buffer.put(theTable.array(), timestampOffset, (8 + keyLength));
			
			int valueOffset = windowHashTable.getValueOffset (idx);
			int countOffset = windowHashTable.getCountOffset (idx);
			
			int count = theTable.getInt(countOffset);
			int p;
			for (int i = 0; i < numberOfValues(); ++i) {
				p = valueOffset + i * 4;
				if (aggregationTypes[i] == AggregationType.AVG) {
					buffer.putFloat(theTable.getFloat(p) / (float) count);
				} else {
					buffer.putFloat(theTable.getFloat(p));
				}
			}
			buffer.put(outputSchema.getPad());
		}
	}
	
	private void processDataPerWindowIncrementallyWithGroupBy (WindowBatch batch, IWindowAPI api) {
		
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();
		
		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;
		
		/* Current window start and end pointers */
		int start, end;
		/* Previous window start and end pointers */
		int _start = -1;
		int   _end = -1;
		
		WindowHashTable windowHashTable = WindowHashTableFactory.newInstance(workerId);
		windowHashTable.setTupleLength(keyLength, valueLength);
		byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
		boolean [] found = (boolean []) tl_found.get(); // new boolean[1];
		boolean pack = false;
		
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;
			
			pack = false;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();					
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
				pack = true;
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			/* Process tuples in current window that have not been in the previous window */
			if (_start != -1) {
				for (int i = _end; i < end; i += inputTupleSize) {
					enterWindow (workerId, inputBuffer, inputSchema, i, windowHashTable, tupleKey, found);
				}
			} else {
				for (int i = start; i < end; i += inputTupleSize) {
					enterWindow (workerId, inputBuffer, inputSchema, i, windowHashTable, tupleKey, found);
				}
			}
			
			/* Process tuples in previous window that are not in current window */
			if (_start != -1) {
				for (int i = _start; i < start; i += inputTupleSize) {
					exitWindow (inputBuffer, inputSchema, i, windowHashTable, tupleKey, found);
				}
			}
			
			/* Store window result and move to next window */
			evaluateWindow (windowHashTable, outputBuffer, pack);
			
			_start = start;
			_end = end;
		}
		
		/* Release hash maps */
		windowHashTable.release();
		
		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}
	
	private void exitWindow (IQueryBuffer buffer, ITupleSchema schema, int tupleOffset, WindowHashTable windowHashTable, 
		
		byte [] tupleKey, boolean [] found) {
		
		setGroupByKey (buffer, schema, tupleOffset, tupleKey);
		
		found[0] = false;
		int idx = windowHashTable.getIndex(tupleKey, found);
		
		if (idx < 0 || ! found[0])
			throw new IllegalStateException ("error: attempt to remove an invalid tuple from window");
		
		ByteBuffer theTable = windowHashTable.getBuffer();
		
		/* Update existing entry */
		int valueOffset = windowHashTable.getValueOffset (idx);
		int countOffset = windowHashTable.getCountOffset (idx);
		/* Decrement tuple count */
		int count = theTable.getInt(countOffset);
		count--;
		/*
		 * If we are linearly probing the hash table to find an
		 * entry, we cannot simply remove elements because this
		 * might brake a chain.
		 * 
		 * We should rather rely on a `tombstone mark` (2).
		 */
		if (count <= 0) {
			theTable.put(idx, (byte) 2);
		} else {
			/* Update value(s) */
			int p;
			float value;
			for (int i = 0; i < numberOfValues(); ++i) {
				p = valueOffset + i * 4;
				value = theTable.getFloat(p); 
				if (this.aggregationTypes[i] != AggregationType.CNT) {
					value -= this.aggregationAttributes[i].eval (buffer, schema, tupleOffset);
				} else {
					value -= 1; 
				}
				theTable.putFloat(valueOffset, value);
			}
			/* Update count */
			theTable.putInt(countOffset, count);
		}
	}
	
	private void enterWindow (int workerId, IQueryBuffer buffer, ITupleSchema schema, int tupleOffset, 
		
		WindowHashTable windowHashTable, byte [] tupleKey, boolean [] found) {
		
		if (tupleOffset < 0) {
			System.err.println("[DBG] Negative tuple offset?!");
			System.exit(1);
		}
		
		setGroupByKey (buffer, schema, tupleOffset, tupleKey);
		
		float [] values = tl_values.get(); 
		
		for (int i = 0; i < numberOfValues(); ++i) {
			if (aggregationTypes[i] == AggregationType.CNT)
				values[i] = 1;
			else
				values[i] = aggregationAttributes[0].eval(buffer, schema, tupleOffset);
		}
		
		/* Check whether there is already an entry in the hash table for this key. 
		 * If not, create a new entry */
		found[0] = false;
		int idx = windowHashTable.getIndex (tupleKey, found);
		if (idx < 0) {
			System.out.println("error: open-adress hash table is full");
			System.exit(1);
		}
		
		ByteBuffer theTable = windowHashTable.getBuffer();
		
		int timestampOffset = windowHashTable.getTimestampOffset (idx);
		
		int valueOffset = windowHashTable.getValueOffset (idx);
		int countOffset = windowHashTable.getCountOffset (idx);
		
		float v;
		int p;
		
		/* Store value(s) */
		if (! found[0]) {
			theTable.put (idx, (byte) 1);
			theTable.position(timestampOffset);
			/* Store timestamp */
			theTable.putLong (buffer.getLong(tupleOffset));
			/* Store key and value(s) */
			theTable.put (tupleKey);
			for (int i = 0; i < numberOfValues(); ++i) {
				theTable.putFloat(values[i]);
			}
			/* Store count */
			theTable.putInt(1);
		} else {
			/* Update existing entry */
			for (int i = 0; i < numberOfValues(); ++i) {
				p = valueOffset + i * 4;
				switch (aggregationTypes[i]) {
				case CNT:
					theTable.putFloat(p, (theTable.getFloat(p) + 1));
					break;
				case SUM:
				case AVG:
					theTable.putFloat(p, (theTable.getFloat(p) + values[i]));
				case MIN:
					v = theTable.getFloat(p);
					theTable.putFloat(p, ((v > values[i]) ? values[i] : v));
					break;
				case MAX:
					v = theTable.getFloat(p);
					theTable.putFloat(p, ((v < values[i]) ? values[i] : v));
					break;
				default:
					throw new IllegalArgumentException ("error: invalid aggregation type");
				}
			}
			/* Increment tuple count */
			theTable.putInt(countOffset, theTable.getInt(countOffset) + 1);
		}
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}

	public void configureOutput (int queryId) {
		
		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	public void processOutput (int queryId, WindowBatch batch) {
		
		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}
	
	public void setup() {
		
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}
}
