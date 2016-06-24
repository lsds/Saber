#ifndef __OUTPUT_BUFFER_H_
#define __OUTPUT_BUFFER_H_

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <jni.h>

typedef struct output_buffer *outputBufferP;
typedef struct output_buffer {
	int size;
	unsigned char writeOnly;
	unsigned char doNotMove;
	unsigned char bearsMark; /* The last integer is the mark */
	unsigned char readEvent;
	unsigned char ignoreMark;
	cl_mem device_buffer;
	cl_mem pinned_buffer;
	void  *mapped_buffer;
} output_buffer_t;

outputBufferP getOutputBuffer (cl_context, cl_command_queue, int, int, int, int, int, int);

void freeOutputBuffer (outputBufferP, cl_command_queue);

int getOutputBufferSize (outputBufferP);

#endif /* __OUTPUT_BUFFER_H_ */

