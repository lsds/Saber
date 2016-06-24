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

__kernel void selectKernel (
	const int size,
	const int tuples,
	__global const uchar *input,
	__global int *flags, /* The output of select (0 or 1) */
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local  int *x
)
{
	int lgs = get_local_size (0);
	int tid = get_global_id (0);

	int  left = (2 * tid);
	int right = (2 * tid) + 1;

	int lid = get_local_id(0);

	/* Local memory indices */
	int  _left = (2 * lid);
	int _right = (2 * lid) + 1;

	int gid = get_group_id (0);
	/* A thread group processes twice as many tuples */
	int L = 2 * lgs;

	/* Fetch tuple and apply selection filter */
	const int lidx =  left * sizeof(input_t);
	const int ridx = right * sizeof(input_t);

	__global input_t *lp = (__global input_t *) &input[lidx];
	__global input_t *rp = (__global input_t *) &input[ridx];

	flags[ left] = selectf (lp);
	flags[right] = selectf (rp);

	/* Copy flag to local memory */
	x[ _left] = (left  < tuples) ? flags[ left] : 0;
	x[_right] = (right < tuples) ? flags[right] : 0;

	upsweep(x, L);

	if (lid == (lgs - 1)) {
		partitions[gid] = x[_right];
		x[_right] = 0;
	}

	downsweep(x, L);

	/* Write results to global memory */
	offsets[ left] = ( left < tuples) ? x[ _left] : -1;
	offsets[right] = (right < tuples) ? x[_right] : -1;
}

__kernel void compactKernel (
	const int size,
	const int tuples,
	__global const uchar *input,
	__global int *flags,
	__global int *offsets,
	__global int *partitions,
	__global uchar *output,
	__local  int *x
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

	/* Compact left and right */
	if (flags[left] == 1) {

		const int lq = (offsets[left] + pivot) * sizeof(output_t);
		const int lp = left * sizeof(input_t);
		flags[left] = lq + sizeof(output_t);
		 __global  input_t *lx = (__global  input_t *) &  input[lp];
		 __global output_t *ly = (__global output_t *) & output[lq];

		 /* TODO: replace with generic function */

		 ly->vectors[0] = lx->vectors[0];
		 ly->vectors[1] = lx->vectors[1];
		//  ly->vectors[2] = lx->vectors[2];
		//  ly->vectors[3] = lx->vectors[3];
	}

	if (flags[right] == 1) {

		const int rq = (offsets[right] + pivot) * sizeof(output_t);
		const int rp = right * sizeof(input_t);
		flags[right] = rq + sizeof(output_t);
		 __global  input_t *rx = (__global  input_t *) &  input[rp];
		 __global output_t *ry = (__global output_t *) & output[rq];

		 /* TODO: replace with generic function */

		 ry->vectors[0] = rx->vectors[0];
		 ry->vectors[1] = rx->vectors[1];
		 // ry->vectors[2] = rx->vectors[2];
		 // ry->vectors[3] = rx->vectors[3];

	}
}

