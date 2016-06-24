__kernel void projectKernel (
	const int tuples,
	const int bytes,
	__global const uchar *input,
	__global uchar *output,
	__local uchar *_input,
	__local uchar *_output
) {

	int lid = (int) get_local_id  (0);
	int gid = (int) get_group_id  (0);
	int lgs = (int) get_local_size(0); /* Local group size */

	int input_idx  = gid * lgs * sizeof( input_t);
	int output_idx = gid * lgs * sizeof(output_t);

	/* Cache data into local memory */
	barrier (CLK_LOCAL_MEM_FENCE);
	event_t e = (event_t) 0;
	e = async_work_group_copy (
		(__local uchar *) &_input[0],
		(const __global uchar *) &input[input_idx],
		sizeof(input_t) * lgs,
		e);
	/* wait_group_events (1, &e); */

	__local  input_t* p = (__local  input_t*) &_input [lid * sizeof( input_t)];
	__local output_t* q = (__local output_t*) &_output[lid * sizeof(output_t)];

	projectf (p, q);

	/* Write results in main memory */
	barrier (CLK_LOCAL_MEM_FENCE);
	e = async_work_group_copy (
			(__global uchar *) &output[output_idx],
			(__local uchar *) &_output[0],
			sizeof(output_t) * lgs,
			e);
	wait_group_events (1, &e);

	return ;
}
