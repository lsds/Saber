inline int gatherInt (__local uchar *data, int index) {
	int i = data [index] & 0xFF;
	i |= (data[++index] & 0xFF) << 8;
	i |= (data[++index] & 0xFF) << 16;
	i |= (data[++index] << 24);
	return i;
}

inline int gatherPartialInt (__local uchar *data, int index, int available) {
	int i = data[index] & 0xFF;
	if (available > 1) {
		i |= (data[++index] & 0xFF) << 8;
		if (available > 2) {
			i |= (data[++index] & 0xFF) << 16;
		}
	}
	return i;
}

#define rotateInt(x,k) (((x)>>(k)) | ((x)<<(32-(k))))

#define mix(a,b,c) \
{ \
a -= c;  a ^= rotateInt(c, 4);  c += b; \
b -= a;  b ^= rotateInt(a, 6);  a += c; \
c -= b;  c ^= rotateInt(b, 8);  b += a; \
a -= c;  a ^= rotateInt(c,16);  c += b; \
b -= a;  b ^= rotateInt(a,19);  a += c; \
c -= b;  c ^= rotateInt(b, 4);  b += a; \
}

#define final(a,b,c) \
{ \
c ^= b; c -= rotateInt(b,14); \
a ^= c; a -= rotateInt(c,11); \
b ^= a; b -= rotateInt(a,25); \
c ^= b; c -= rotateInt(b,16); \
a ^= c; a -= rotateInt(c, 4); \
b ^= a; b -= rotateInt(a,14); \
c ^= b; c -= rotateInt(b,24); \
}

inline int jenkinsHash (__local uchar *key, int length, int initValue) {
	int a, b, c;
	a = b = c = (0xdeadbeef + (length << 2) + initValue);
	int L = length;
	__local int *k = (__local int *) key;
	int i = 0;
	while (L >= 12) {
		a += k[0];
		b += k[1];
		c += k[2];
		mix(a, b, c);
		L -= 12;
		k +=  3;
		i += 12;
	}
	/* Handle the last few bytes */
	c += L;
	if (L > 0) {
		if (L >= 4) {
			a += gatherInt(key, i);
			if (L >= 8) {
				b += gatherInt(key, i + 4);
				if (L > 8) {
					c += (gatherPartialInt(key, i + 8, L - 8) << 8);
				}
			} else if (L > 4) {
				b += gatherPartialInt(key, i + 4, L - 4);
			}
		} else {
			a += gatherPartialInt(key, i, L);
		}
	}
	final(a,b,c);
	return c;
}

__kernel void countWindowsKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);
	int lid = (int) get_local_id   (0);
	int gid = (int) get_group_id   (0);
	int lgs = (int) get_local_size (0); /* Local group size */
	int nlg = (int) get_num_groups (0);

	__local int num_windows;
	if (lid == 0)
		num_windows = convert_int_sat(offset[1]);

	barrier(CLK_LOCAL_MEM_FENCE);

	int wid = tid;

	if (wid > num_windows)
		return;

	/* A group may process more than one windows */
	while (wid <= num_windows) {

		int  offset_ =  window_ptrs_ [wid]; /* Window start and end pointers */
		int _offset  = _window_ptrs  [wid];

		/* Check if a window is closing, opening, pending, or complete. */
		if (offset_ < 0 && _offset >= 0) {
			/* A closing window; set start offset */
			offset_ = 0;
			atomic_inc(&windowCounts[0]);
		} else
		if (offset_ >= 0 && _offset < 0) {
			/* An opening window; set end offset */
			_offset = inputBytes;
			atomic_inc(&windowCounts[3]);
		} else
		if (offset_ < 0 && _offset < 0) {
			/* A pending window */
			_offset  = 0;
			offset_ = inputBytes;
			atomic_inc(&windowCounts[1]);
		} else
		if (offset_ == 0 && _offset == 0) {
			/* A closing window */
			atomic_inc(&windowCounts[0]);
		} else {
			/* A complete window */
			atomic_inc(&windowCounts[2]);
		}

		wid += nlg * lgs; /* try next window */
	}
	return ;
}

__kernel void aggregateClosingWindowsKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);
	int lid = (int) get_local_id   (0);
	int gid = (int) get_group_id   (0);
	int lgs = (int) get_local_size (0); /* Local group size */
	int nlg = (int) get_num_groups (0);
	

	__local int num_windows;
	if (lid == 0)
		num_windows = windowCounts[0];

	barrier(CLK_LOCAL_MEM_FENCE);

	int group_offset = lgs * sizeof(input_t);

	int table_capacity = _table_ / sizeof(intermediate_t);

	int wid = gid;

	if (wid >= num_windows)
		return;

	// A group may process more than one closing windows
	while (wid < num_windows) {
		//
		// The offset of closing windows should start from 0.
		//
		int table_offset = wid * (_table_);

		int  offset_ =  window_ptrs_ [wid]; // Window start and end pointers
		int _offset  = _window_ptrs  [wid];

		if (offset_ == _offset) {
		 	wid += nlg;
			continue;
		}

		int idx = lid * sizeof(input_t) + offset_;

		while (idx < _offset) {

			int lidx = lid * sizeof(key_t);

			__global input_t *p = (__global input_t *)   &input[ idx];
			__local key_t    *k = (__local  key_t   *) &scratch[lidx];

			pack_key (k, p);

			int h = jenkinsHash(&scratch[lidx], sizeof(key_t), 1) & (table_capacity - 1);

			int tableIndex = h * sizeof(intermediate_tuple_t) + table_offset;

			// Insert
			bool success = false;

			for (int attempt = 1; attempt <= table_capacity; ++attempt) {

				__global intermediate_t *t = (__global intermediate_t *) &closingContents[tableIndex];

				int old = atomic_cmpxchg((global int *) &(t->tuple.mark), -1, idx);
				if (old == -1) {

					// Insert new tuple
					storef (t, p);

					success = true;
					break;

				} else {

					// Check tuple at position `old`
					__global input_t *theOther = (__global input_t *) &input[old];

					// Compare keys
					if (comparef(k, theOther) == 1) {
						// Update value and count
						updatef(t, p);

						success = true;
						break;

					}
				}
				// Conflict; try next slot
				++h;
				tableIndex = (h & (table_capacity - 1)) * sizeof(intermediate_tuple_t) + table_offset;
			}

			if (! success) {
				atomic_inc(&failed[0]);
			}

			idx += group_offset;
		}

		wid += nlg; // try next window
	}

	return ;
}

__kernel void aggregateCompleteWindowsKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);
	int lid = (int) get_local_id   (0);
	int gid = (int) get_group_id   (0);
	int lgs = (int) get_local_size (0); /* Local group size */
	int nlg = (int) get_num_groups (0);

	__local int num_windows;

	__local int nclosing;
	__local int npending;

	if (lid == 0) {
		nclosing = windowCounts[0];
		npending = windowCounts[1];

		num_windows = windowCounts[2];
	}

	barrier(CLK_LOCAL_MEM_FENCE);

	if (npending > 0)
		return;

	int windowIndexOffset = nclosing;

	int group_offset = lgs * sizeof(input_t);

	int table_capacity = _table_ / sizeof(intermediate_t);

	int wid = gid + windowIndexOffset;

	if (wid >= num_windows + windowIndexOffset)
		return;

	/* A group may process more than one closing windows */
	while (wid < num_windows + windowIndexOffset) {

		/*
		 * The offset of closing windows should start from 0.
		 */
		int table_offset = (wid - windowIndexOffset) * (_table_);

		int  offset_ =  window_ptrs_ [wid]; /* Window start and end pointers */
		int _offset  = _window_ptrs  [wid];

		if (offset_ == _offset) {
		 	wid += nlg;
			continue;
		}

		int idx = lid * sizeof(input_t) + offset_;

		while (idx < _offset) {

			int lidx = lid * sizeof(key_t);

			__global input_t *p = (__global input_t *)   &input[ idx];
			__local key_t    *k = (__local  key_t   *) &scratch[lidx];

			pack_key (k, p);

			int h = jenkinsHash(&scratch[lidx], sizeof(key_t), 1) & (table_capacity - 1);

			int tableIndex = h * sizeof(intermediate_tuple_t) + table_offset;

			if (tableIndex >= outputBytes)
				break;

			/* Insert */
			bool success = false;

			for (int attempt = 1; attempt <= table_capacity; ++attempt) {

				__global intermediate_t *t = (__global intermediate_t *) &completeContents[tableIndex];

				int old = atomic_cmpxchg((global int *) &(t->tuple.mark), -1, idx);
				if (old == -1) {

					/* Insert new tuple */
					storef (t, p);

					success = true;
					break;

				} else {

					/* Check tuple at position `old` */
					__global input_t *theOther = (__global input_t *) &input[old];

					/* Compare keys */
					if (comparef(k, theOther) == 1) {
						/* Update value and count */
						updatef(t, p);

						success = true;
						break;

					}
				}
				/* Conflict; try next slot */
				++h;
				tableIndex = (h & (table_capacity - 1)) * sizeof(intermediate_tuple_t) + table_offset;
			}

			if (! success) {
				atomic_inc(&failed[0]);
			}

			idx += group_offset;
		}

		wid += nlg; /* try next window */
	}
	return ;
}

__kernel void aggregateOpeningWindowsKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);
	int lid = (int) get_local_id   (0);
	int gid = (int) get_group_id   (0);
	int lgs = (int) get_local_size (0); /* Local group size */
	int nlg = (int) get_num_groups (0);

	__local int num_windows;

	__local int nclosing;
	__local int npending;
	__local int ncomplete;

	if (lid == 0) {
		nclosing = windowCounts[0];
		npending = windowCounts[1];
		ncomplete= windowCounts[2];

		num_windows = windowCounts[3];
	}

	barrier(CLK_LOCAL_MEM_FENCE);

	int windowIndexOffset = nclosing + npending + ncomplete;

	int group_offset = lgs * sizeof(input_t);

	int table_capacity = _table_ / sizeof(intermediate_t);

	int wid = gid + windowIndexOffset;

	if (wid >= num_windows + windowIndexOffset)
		return;

	/* A group may process more than one closing windows */
	while (wid < num_windows + windowIndexOffset) {
		/*
		 * The offset of closing windows should start from 0.
		 */
		int table_offset = (wid - windowIndexOffset) * (_table_);

		int  offset_ =  window_ptrs_ [wid]; /* Window start and end pointers */
		int _offset  = _window_ptrs  [wid];

		if (offset_ == _offset) {
		 	wid += nlg;
			continue;
		}

		int idx = lid * sizeof(input_t) + offset_;

		while (idx < _offset) {

			int lidx = lid * sizeof(key_t);

			__global input_t *p = (__global input_t *)   &input[ idx];
			__local key_t    *k = (__local  key_t   *) &scratch[lidx];

			pack_key (k, p);

			int h = jenkinsHash(&scratch[lidx], sizeof(key_t), 1) & (table_capacity - 1);

			int tableIndex = h * sizeof(intermediate_tuple_t) + table_offset;

			/* Insert */
			bool success = false;

			for (int attempt = 1; attempt <= table_capacity; ++attempt) {

				__global intermediate_t *t = (__global intermediate_t *) &completeContents[tableIndex];

				int old = atomic_cmpxchg((global int *) &(t->tuple.mark), -1, idx);
				if (old == -1) {

					/* Insert new tuple */
					storef (t, p);

					success = true;
					break;

				} else {

					/* Check tuple at position `old` */
					__global input_t *theOther = (__global input_t *) &input[old];

					/* Compare keys */
					if (comparef(k, theOther) == 1) {
						/* Update value and count */
						updatef(t, p);

						success = true;
						break;

					}
				}
				/* Conflict; try next slot */
				++h;
				tableIndex = (h & (table_capacity - 1)) * sizeof(intermediate_tuple_t) + table_offset;
			}

			if (! success) {
				atomic_inc(&failed[0]);
			}

			idx += group_offset;
		}

		wid += nlg; /* try next window */
	}
	return ;
}

__kernel void aggregatePendingWindowsKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);
	int lid = (int) get_local_id   (0);
	int gid = (int) get_group_id   (0);
	int lgs = (int) get_local_size (0); /* Local group size */
	int nlg = (int) get_num_groups (0);

	__local int num_windows;

	__local int nclosing;

	if (lid == 0) {
		nclosing = windowCounts[0];
		num_windows = windowCounts[1];
	}

	barrier(CLK_LOCAL_MEM_FENCE);

	int windowIndexOffset = nclosing;

	if (num_windows <= 0)
		return;

	int table_capacity = _table_ / sizeof(intermediate_t);

	int wid = windowIndexOffset;

	//  There is only one window
	int  offset_ =  window_ptrs_ [wid]; // Window start and end pointers
	int _offset  = _window_ptrs  [wid];

	int idx = tid * sizeof(input_t) + offset_;

	int lidx = lid * sizeof(key_t);

	__global input_t *p = (__global input_t *)   &input[ idx];
	__local key_t    *k = (__local  key_t   *) &scratch[lidx];

	pack_key (k, p);

	int h = jenkinsHash(&scratch[lidx], sizeof(key_t), 1) & (table_capacity - 1);

	int tableIndex = h * sizeof(intermediate_tuple_t);

	// Insert
	bool success = false;

	for (int attempt = 1; attempt <= table_capacity; ++attempt) {

		__global intermediate_t *t = (__global intermediate_t *) &completeContents[tableIndex];

		int old = atomic_cmpxchg((global int *) &(t->tuple.mark), -1, idx);
		if (old == -1) {

			// Insert new tuple
			storef (t, p);

			success = true;
			break;

		} else {

			// Check tuple at position `old`
			__global input_t *theOther = (__global input_t *) &input[old];

			// Compare keys
			if (comparef(k, theOther) == 1) {
				// Update value and count
				updatef(t, p);

				success = true;
				break;

			}
		}

		// Conflict; try next slot
		++h;
		tableIndex = (h & (table_capacity - 1)) * sizeof(intermediate_tuple_t);
	}

	if (! success) {
		atomic_inc(&failed[0]);
	}

	return ;
}

__kernel void clearKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {

	int tid = (int) get_global_id (0);

	int outputIndex = tid * sizeof(intermediate_tuple_t);
	if (outputIndex >= outputBytes)
		return;

	__global intermediate_t *t1 = (__global intermediate_t *) &closingContents[outputIndex];
	t1->vectors[0] =  0;
	t1->vectors[1] =  0;
	t1->tuple.mark = -1;

	__global intermediate_t *t2 = (__global intermediate_t *) &pendingContents[outputIndex];
	t2->vectors[0] =  0;
	t2->vectors[1] =  0;
	t2->tuple.mark = -1;

	__global intermediate_t *t3 = (__global intermediate_t *) &completeContents[outputIndex];
	t3->vectors[0] =  0;
	t3->vectors[1] =  0;
	t3->tuple.mark = -1;

	__global intermediate_t *t4 = (__global intermediate_t *) &completeContents[outputIndex];
	t4->vectors[0] =  0;
	t4->vectors[1] =  0;
	t4->tuple.mark = -1;

	if (tid < tuples) {

		/* The maximum number of window pointers is ... */
		if (tid < maxWindows) {
			window_ptrs_[tid] = -1;
			_window_ptrs [tid] = -1;
		}

		failed  [tid] = 0;
		attempts[tid] = 0;

		if (tid < 5) {

			/* Initialise window counters: closing, pending, complete, opening.
			 *
			 * The last slot is reserved for output size */
			windowCounts[tid] = 0;

			if (tid == 0) {
				offset[0] = LONG_MAX;
				offset[1] = 0;
			}
		}
	}

	return;
}

__kernel void computeOffsetKernel (
		const int tuples,
		const int inputBytes,
		const int outputBytes,
		const int _table_,
		const int maxWindows,
		const long previousPaneId,
		const long batchOffset,
		__global const uchar* input,
		__global int* window_ptrs_,
		__global int* _window_ptrs,
		__global int *failed,
		__global int *attempts,
		__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
		__global int *windowCounts,
		__global uchar* closingContents,
		__global uchar* pendingContents,
		__global uchar* completeContents,
		__global uchar* openingContents,
		__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);

	long wid;
	long paneId, normalisedPaneId;

	long currPaneId;
	long prevPaneId;

	if (batchOffset == 0) {
		offset[0] = 0;
		return;
	}

	/* Every thread is assigned a tuple */
#ifdef RANGE_BASED
	__global input_t *curr = (__global input_t *) &input[tid * sizeof(input_t)];
	currPaneId = __bswap64(curr->tuple.t) / PANE_SIZE;
#else
	currPaneId = ((batchOffset + (tid * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif
	if (tid > 0) {
#ifdef RANGE_BASED
		__global input_t *prev = (__global input_t *) &input[(tid - 1) * sizeof(input_t)];
		prevPaneId = __bswap64(prev->tuple.t) / PANE_SIZE;
#else
		prevPaneId = ((batchOffset + ((tid - 1) * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif
	} else {
		prevPaneId = previousPaneId;
	}

	if (prevPaneId < currPaneId) {
		/* Compute offset based on the first closing window */
		while (prevPaneId < currPaneId) {
			paneId = prevPaneId + 1;
			normalisedPaneId = paneId - PANES_PER_WINDOW;
			if (normalisedPaneId >= 0 && normalisedPaneId % PANES_PER_SLIDE == 0) {
				wid = normalisedPaneId / PANES_PER_SLIDE;
				if (wid >= 0) {
					atom_min(&offset[0], wid);
					break;
				}
			}
			prevPaneId += 1;
		}
	}

//	if (tid == 0) {
//		if (offset[0] == LONG_MAX)
//			offset[0] = 0;
//	}

	return ;
}

__kernel void computePointersKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {
	int tid = (int) get_global_id  (0);

	long wid;
	long paneId, normalisedPaneId;

	long currPaneId;
	long prevPaneId;
	
	// Every thread is assigned a tuple
#ifdef RANGE_BASED
	__global input_t *curr = (__global input_t *) &input[tid * sizeof(input_t)];
	currPaneId = __bswap64(curr->tuple.t) / PANE_SIZE;
#else
	currPaneId = ((batchOffset + (tid * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif
	if (tid > 0) {
#ifdef RANGE_BASED
		__global input_t *prev = (__global input_t *) &input[(tid - 1) * sizeof(input_t)];
		prevPaneId = __bswap64(prev->tuple.t) / PANE_SIZE;
#else
		prevPaneId = ((batchOffset + ((tid - 1) * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif
	} else {
		prevPaneId = previousPaneId;
	}

	long windowOffset = offset[0];
	int index;

	if (prevPaneId < currPaneId) {
		while (prevPaneId < currPaneId) {
			paneId = prevPaneId + 1;
			normalisedPaneId = paneId - PANES_PER_WINDOW;
			// Check closing windows
			if (normalisedPaneId >= 0 && normalisedPaneId % PANES_PER_SLIDE == 0) {
				wid = normalisedPaneId / PANES_PER_SLIDE;
				if (wid >= 0) {
					index = convert_int_sat(wid - windowOffset);
					atom_max(&offset[1], (wid - windowOffset));
					_window_ptrs[index] = tid * sizeof(input_t);
				}
			}
			// Check opening windows
			if (paneId % PANES_PER_SLIDE == 0) {
				wid = paneId / PANES_PER_SLIDE;
				index = convert_int_sat(wid - windowOffset);
				atom_max(&offset[1], wid - windowOffset);
				window_ptrs_[index] = tid * sizeof(input_t);
			}
			prevPaneId += 1;
		}
	}

	return ;
}

__kernel void packKernel (
	const int tuples,
	const int inputBytes,
	const int outputBytes,
	const int _table_,
	const int maxWindows,
	const long previousPaneId,
	const long batchOffset,
	__global const uchar* input,
	__global int* window_ptrs_,
	__global int* _window_ptrs,
	__global int *failed,
	__global int *attempts,
	__global long *offset, /* Temp. variable holding the window pointer offset and window counts */
	__global int *windowCounts,
	__global uchar* closingContents,
	__global uchar* pendingContents,
	__global uchar* completeContents,
	__global uchar* openingContents,
	__local uchar *scratch
) {

	int tid = (int) get_global_id (0);

//	int outputIndex = tid * sizeof(intermediate_tuple_t);
//	if (outputIndex >= outputBytes)
//		return;
//
//	__global intermediate_t *t1 = (__global intermediate_t *) &closingContents[outputIndex];
//
//	if (t1->tuple.mark == -1)
//		return;
//
//	/* Set mark's first byte to 1 */
//	t1->tuple.mark = 0;
//	t1->tuple.pad0 = 0;
//	closingContents[outputIndex] = 1;

	/* No need to update the timestamp or the key */

	// t1->tuple.value1 = __bswap32(t1->tuple.value1);
	// t1->tuple.value1 = __bswapfp(t1->tuple.value1);
	// t1->tuple.count = __bswap32(t1->tuple.count);

	return;
}
