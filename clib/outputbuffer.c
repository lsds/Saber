#include "outputbuffer.h"

#include "openclerrorcode.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdint.h>

outputBufferP getOutputBuffer (cl_context context, cl_command_queue queue, int size,

	int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {

	outputBufferP p = malloc(sizeof(output_buffer_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit (1);
	}
	p->size = size;

	p->writeOnly = (unsigned char) writeOnly;
	p->doNotMove = (unsigned char) doNotMove;
	p->bearsMark = (unsigned char) bearsMark;
	p->readEvent = (unsigned char) readEvent;
	p->ignoreMark= (unsigned char) ignoreMark;

	int error;
	cl_mem_flags flags;
	if (writeOnly)
		flags = CL_MEM_WRITE_ONLY;
	else
		flags = CL_MEM_READ_WRITE;
	/* Set p->device_buffer */
	p->device_buffer = clCreateBuffer (
		context,
		flags,
		p->size,
		NULL,
		&error);
	if (! p->device_buffer) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	/* Allocate buffers */
	p->pinned_buffer = clCreateBuffer (
		context,
		CL_MEM_READ_WRITE | CL_MEM_ALLOC_HOST_PTR,
		p->size,
		NULL,
		&error);
	if (! p->pinned_buffer) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	p->mapped_buffer = (void *) clEnqueueMapBuffer (
		queue,
		p->pinned_buffer,
		CL_TRUE,
		CL_MAP_READ,
		0,
		p->size,
		0, NULL, NULL,
		&error);
	if (! p->mapped_buffer) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return p;
}

int getOutputBufferSize (outputBufferP b) {
	return b->size;
}

void freeOutputBuffer (outputBufferP b, cl_command_queue queue) {
	if (b) {
		if (b->mapped_buffer)
			clEnqueueUnmapMemObject (
				queue,
				b->pinned_buffer,
				(void *) b->mapped_buffer,
				0, NULL, NULL); /* Zero dependencies */

		if (b->pinned_buffer)
			clReleaseMemObject(b->pinned_buffer);

		if (b->device_buffer)
			clReleaseMemObject(b->device_buffer);

		free (b);
	}
}
