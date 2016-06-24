/* Scan based on the implementation of [...] */

/*
 * Up-sweep (reduce) on a local array `data` of length `length`.
 * `length` must be a power of two.
 */
inline void upsweep (__local int *data, int length) {

	int lid  = get_local_id (0);
	int b = (lid * 2) + 1;
	int depth = 1 + (int) log2 ((float) length);

	for (int d = 0; d < depth; d++) {

		barrier(CLK_LOCAL_MEM_FENCE);
		int mask = (0x1 << d) - 1;
		if ((lid & mask) == mask) {

			int offset = (0x1 << d);
			int a = b - offset;
			data[b] += data[a];
		}
	}
}

/*
 * Down-sweep on a local array `data` of length `length`.
 * `length` must be a power of two.
 */
inline void downsweep (__local int *data, int length) {

	int lid = get_local_id (0);
	int b = (lid * 2) + 1;
	int depth = (int) log2 ((float) length);
	for (int d = depth; d >= 0; d--) {

		barrier(CLK_LOCAL_MEM_FENCE);
		int mask = (0x1 << d) - 1;
		if ((lid & mask) == mask) {

			int offset = (0x1 << d);
			int a = b - offset;
			int t = data[a];
			data[a] = data[b];
			data[b] += t;
		}
	}
}

/* Scan */
inline void scan (__local int *data, int length) {

	int lid = get_local_id (0);
	int lane = (lid * 2) + 1;

	upsweep (data, length);

	if (lane == (length - 1))
		data[lane] = 0;

	downsweep (data, length);
	return ;
}

/*
 * Assumes:
 *
 * - N tuples
 * - Every thread handles two tuples, so #threads = N / 2
 * - L threads/group
 * - Every thread group handles (2 * L) tuples
 * - Let, W be the number of groups
 * - N = 2L * W => W = N / 2L
 *
 */
__kernel void scanKernel (
	const int s1_tuples,
	const int s2_tuples,
	const int output_size,
	__global const uchar *s1_input,
	__global const uchar *s2_input,
	__global const int *window_start_pointers,
	__global const int *window_end_pointers,
	__global int *counts,
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local int *x
)
{
	int lgs = get_local_size (0);
	int tid = get_global_id (0);

	int  left = (2 * tid);
	int right = (2 * tid) + 1;

	int lid = get_local_id(0);

	/* Local memory indices */
	int cached_left  = (2 * lid);
	int cached_right = (2 * lid) + 1;

	int gid = get_group_id (0);
	/* A thread group processes twice as many tuples */
	int L = 2 * lgs;

	/* Copy count to local memory */
	x[cached_left]  = (left  < s1_tuples) ? counts[ left] : 0;
	x[cached_right] = (right < s1_tuples) ? counts[right] : 0;

	upsweep(x, L);

	if (lid == (lgs - 1)) {
		partitions[gid] = x[cached_right];
		x[cached_right] = 0;
	}

	downsweep(x, L);

	/* Write results to global memory */
	offsets[ left] = ( left < s1_tuples) ? x[cached_left]  : -1;
	offsets[right] = (right < s1_tuples) ? x[cached_right] : -1;
}

__kernel void compactKernel (
	const int s1_tuples,
	const int s2_tuples,
	const int output_size,
	__global const uchar *s1_input,
	__global const uchar *s2_input,
	__global const int *window_start_pointers,
	__global const int *window_end_pointers,
	__global int *counts,
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local int *x
) {

	int tid = get_global_id (0);

	int  left = (2 * tid);
	int right = (2 * tid) + 1;

	int lid = get_local_id (0);

	int gid = get_group_id (0);

	/* Compute pivot value */
	__local int pivot;
	if (lid == 0) {
		pivot = 0;
		if (gid > 0) {
			for (int i = 0; i < gid; i++) {
				pivot += (partitions[i]);
			}
		}
	}
	barrier(CLK_LOCAL_MEM_FENCE);

	offsets[ left] = offsets[ left] + pivot;
	offsets[right] = offsets[right] + pivot;
}

__kernel void countKernel (
	const int s1_tuples,
	const int s2_tuples,
	const int output_size,
	__global const uchar *s1_input,
	__global const uchar *s2_input,
	__global const int *window_start_pointers,
	__global const int *window_end_pointers,
	__global int *counts,
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local int *x
) {
	int tid = get_global_id (0);
	if (tid >= s1_tuples)
		return ;
	int count = 0;
	/* For every tuple in stream S1, scan stream S2 */
	const int start_offset = window_start_pointers [tid];
	const int end_offset   = window_end_pointers   [tid];

	/* If this is not a valid tuple, set count to zero */
	if (start_offset < 0 || end_offset < 0) {
		counts[tid] = 0;
		return ;
	}

	const int lidx = tid * sizeof(s1_input_t);
	__global s1_input_t *lp = (__global s1_input_t *) &s1_input[lidx];

	int i;
	for (i = 0; i < s2_tuples; i++) {

		const int ridx = i * sizeof(s2_input_t);

		/* Is the last tuple of the window included or not? */
		if (ridx < start_offset || ridx > end_offset) {
			counts[tid] = count;
			continue;
		}

		/* Read tuple from second stream */
		__global s2_input_t *rp = (__global s2_input_t *) &s2_input[ridx];
		/* selectf returns either 0 or 1 */
		count += selectf (lp, rp);
	}

	counts[tid] = count;
	// counts[tid] = start_offset;
	// counts[tid] = sizeof(output_t);
	return ;
}

__kernel void joinKernel (
	const int s1_tuples,
	const int s2_tuples,
	const int output_size,
	__global const uchar *s1_input,
	__global const uchar *s2_input,
	__global const int *window_start_pointers,
	__global const int *window_end_pointers,
	__global int *counts,
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local int *x
) {

	int tid = get_global_id (0);
	if (tid >= s1_tuples)
		return ;

	int count = 0;

	/* For every tuple in stream S1, scan stream S2 */
	const int start_offset = window_start_pointers [tid];
	const int end_offset   = window_end_pointers   [tid];

	/* If this is not a valid tuple, set count to zero */
	if (start_offset < 0 || end_offset < 0) {
		return ;
	}

	const int lidx = tid * sizeof(s1_input_t);
	__global s1_input_t *lp = (__global s1_input_t *) &s1_input[lidx];

	int i;
	for (i = 0; i < s2_tuples; i++) {

		const int ridx = i * sizeof(s2_input_t);

		/* Is the last tuple of the window included or not? */
		if (ridx < start_offset || ridx > end_offset)
			continue;

		/* Read tuple from second stream */
		__global s2_input_t *rp = (__global s2_input_t *) &s2_input[ridx];

		/* `selectf` returns either 0 or 1 */
		const int result = selectf (lp, rp);

		if (result == 1) {
			/* Compute write offset */
			const int rq = (offsets[tid] + count) * sizeof(output_t);
			if (rq < output_size) {
				__global output_t *out = (__global output_t *) &output[rq];
				copyf (lp, rp, out);
			}
			/* Store mark */
			counts[tid] = rq + sizeof(output_t);
			count += 1;
		}
	}
	// counts[tid] = offsets[tid];
	return ;
}
