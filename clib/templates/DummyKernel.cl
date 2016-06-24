__kernel void dummyKernel
	(
	__global const uchar *input,
	__global uchar *output
	) {

	int tid = get_global_id (0);

	__global  input_t *p = (__global  input_t *)  &input[tid * sizeof( input_t)];
	__global output_t *q = (__global output_t *) &output[tid * sizeof(output_t)];

	/* Copy p into q */
	copyf (p, q);
	return;
}
