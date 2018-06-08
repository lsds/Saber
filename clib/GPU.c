#include "GPU.h"

#include "uk_ac_imperial_lsds_saber_devices_TheGPU.h"
#include <jni.h>

#include "utils.h"
#include "debug.h"

#include "gpuquery.h"
#include "resulthandler.h"
#include "openclerrorcode.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;

static int Q; /* Number of queries */
static int freeIndex;
static gpuQueryP queries [MAX_QUERIES];

static int D;
static gpuContextP pipeline [MAX_DEPTH];

static jclass class;
static jmethodID writeMethod, readMethod;

static resultHandlerP resultHandler = NULL;

/* Forward method declarations (callbacks) */

void callback_setKernelDummy     (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelProject   (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelSelect    (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelCompact   (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelThetaJoin (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelReduce    (cl_kernel, gpuContextP, int *, long *);
void callback_setKernelAggregate (cl_kernel, gpuContextP, int *, long *);

void callback_configureReduce    (cl_kernel, gpuContextP, int *, long *);
void callback_configureAggregate (cl_kernel, gpuContextP, int *, long *);

void callback_writeInput (gpuContextP, JNIEnv *, jobject, int, int);
void callback_readOutput (gpuContextP, JNIEnv *, jobject, int, int, int);

/* Get previous execution context and set current one */
gpuContextP callback_execKernel (gpuContextP);

static void setPlatform () {
	int error = 0;
	cl_uint count = 0;
	error = clGetPlatformIDs (1, &platform, &count);
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	dbg("Obtained 1/%u platforms available\n", count);
	return;
}

static void setDevice () {
	int error = 0;
	cl_uint count = 0;
	error = clGetDeviceIDs (platform, CL_DEVICE_TYPE_GPU, 2, &device, &count);
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	dbg("Obtained 1/%u devices available\n", count);
	return;
}

static void setContext () {
	int error = 0;
	context = clCreateContext (
		0,
		1,
		&device,
		NULL,
		NULL,
		&error);
	if (! context) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return ;
}

static void getDeviceInfo () {

	cl_int error = 0;

	cl_uint value = 0;
	char extensions [2048];
	char name [256];

	error = 0;
	error |= clGetDeviceInfo (device, CL_DEVICE_MEM_BASE_ADDR_ALIGN, sizeof (cl_uint), &value,         NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_EXTENSIONS,                      2048, &extensions[0], NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_NAME,                            2048, &name[0],       NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	fprintf(stdout, "GPU name: %s\n", name);
	fprintf(stdout, "GPU supported extensions are: %s\n", extensions);
	fprintf(stdout, "GPU memory addresses are %u bits aligned\n", value);

	return ;
}

void gpu_init (JNIEnv *env, int _queries, int _depth) {

	int i;
	(void) env;

	setPlatform ();
	setDevice ();
	setContext ();
	getDeviceInfo ();

	Q = _queries; /* Number of queries */
	freeIndex = 0;
	for (i = 0; i < MAX_QUERIES; i++)
		queries[i] = NULL;

	D = _depth; /* Pipeline depth */
	for (i = 0; i < MAX_DEPTH; i++)
		pipeline[i] = NULL;

	#ifdef GPU_HANDLER
	/* Create result handler */
	resultHandler = result_handler_init (env);
	#else
	resultHandler = NULL;
	#endif

	return;
}

void gpu_free () {
	int i;
	int error = 0;
	for (i = 0; i < MAX_QUERIES; i++)
		if (queries[i])
			gpu_query_free (queries[i]);
	if (context)
		error = clReleaseContext (context);
	if (error != CL_SUCCESS)
		fprintf(stderr, "error: failed to free context\n");
	
	return;
}

int gpu_getQuery (const char *source, int _kernels, int _inputs, int _outputs) {
	
	int ndx = freeIndex++;
	if (ndx < 0 || ndx >= Q) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", ndx);
		exit (1);
	}
	queries[ndx] = gpu_query_new (ndx, device, context, source, _kernels, _inputs, _outputs);
	/* Set result handler */
	gpu_query_setResultHandler (queries[ndx], resultHandler);
	
	return ndx;
}

int gpu_setInput  (int qid, int ndx, int size) {
	if (qid < 0 || qid >= Q) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpuQueryP p = queries[qid];
	return gpu_query_setInput (p, ndx, size);
}

int gpu_setOutput (int qid, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {
	if (qid < 0 || qid >= Q) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpuQueryP p = queries[qid];
	return gpu_query_setOutput (p, ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
}

int gpu_setKernel (int qid, int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpuContextP, int *, long *),
	int *args1, long *args2) {

	if (qid < 0 || qid >= Q) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpuQueryP p = queries[qid];
	return gpu_query_setKernel (p, ndx, name, callback, args1, args2);
}

int gpu_exec (int qid,
	size_t *threads, size_t *threadsPerGroup,
	queryOperatorP operator,
	JNIEnv *env, jobject obj) {

	if (qid < 0 || qid >= Q) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpuQueryP p = queries[qid];
	return gpu_query_exec (p, threads, threadsPerGroup, operator, env, obj);
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_init
	(JNIEnv *env, jobject obj, jint N, jint D) {
	
	class = (*env)->GetObjectClass (env, obj);

	writeMethod = (*env)->GetMethodID (env, class,
		"inputDataMovementCallback",  "(IIJI)V");
	if (! writeMethod) {
		fprintf(stderr, "JNI error: failed to acquire write method pointer\n");
		exit(1);
	}

	readMethod = (*env)->GetMethodID (env, class, "outputDataMovementCallback",  "(IIJI)V");
	if (! readMethod) {
		fprintf(stderr, "error: failed to acquire read method pointer\n");
		exit(1);
	}

	// Check NCONTEXTS and D

	gpu_init (env, N, D);

	return 0;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_free
	(JNIEnv *env, jobject obj) {

	(void) env;
	(void) obj;

	gpu_free ();

	return 0;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_getQuery
	(JNIEnv *env, jobject obj, jstring source, jint _kernels, jint _inputs, jint _outputs) {

	(void) obj;

	const char *_source = (*env)->GetStringUTFChars (env, source, NULL);

	int qid = gpu_getQuery (_source, _kernels, _inputs, _outputs);

	(*env)->ReleaseStringUTFChars (env, source, _source);

	return qid;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setInput
	(JNIEnv *env, jobject obj, jint qid, jint ndx, jint size) {

	(void) env;
	(void) obj;
	
	return gpu_setInput (qid, ndx, size);
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setOutput
	(JNIEnv *env, jobject obj, jint qid, jint ndx, jint size,
	jint writeOnly, jint doNotMove, jint bearsMark, jint readEvent, jint ignoreMark) {

	(void) env;
	(void) obj;

	return gpu_setOutput (qid, ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelDummy
	(JNIEnv *env, jobject obj, jint qid, jintArray _args) {

	(void) env;
	(void) obj;

	(void) _args;

	gpu_setKernel (qid, 0, "dummyKernel", &callback_setKernelDummy, NULL, NULL);

	return 0;
}

void callback_setKernelDummy (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) args1;
	(void) args2;

	int error = 0;
	error |= clSetKernelArg (
		kernel,
		0, /* First argument */
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		1, /* Second argument */
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelProject
	(JNIEnv *env, jobject obj, jint qid, jintArray _args) {

	(void) obj;

	jsize argc = (*env)->GetArrayLength(env, _args);

	if (argc != 4) { /* # projection kernel constants */
		fprintf(stderr, "error: invalid number of kernel arguments (expected 4, found %d)\n", argc);
		exit (1);
	}

	jint *args = (*env)->GetIntArrayElements(env, _args, 0);
	/* Object `int []` pinned */
	gpu_setKernel (qid, 0, "projectKernel", &callback_setKernelProject, args, NULL);

	(*env)->ReleaseIntArrayElements (env, _args, args, JNI_ABORT);
	return 0;
}

void callback_setKernelProject (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) args2;

	/* Get all constants */
	int     numberOfTuples = args1[0];
	int      numberOfBytes = args1[1];
	int  cached_input_size = args1[2]; /* Local buffer memory sizes */
	int cached_output_size = args1[3];

	int error = 0;
	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int), (void *) &numberOfTuples);
	error |= clSetKernelArg (kernel, 1, sizeof(int), (void *)  &numberOfBytes);
	/* Set I/O byte buffers */
	error |= clSetKernelArg (
		kernel,
		2,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		3,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	/* Set local memory */
	error |= clSetKernelArg (kernel, 4, (size_t)  cached_input_size, (void *) NULL);
	error |= clSetKernelArg (kernel, 5, (size_t) cached_output_size, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelSelect
	(JNIEnv *env, jobject obj, jint qid, jintArray _args) {

	(void) obj;

	jsize argc = (*env)->GetArrayLength(env, _args);
	if (argc != 3) {
		fprintf(stderr, "error: invalid number of kernel arguments (expected 3, found %d)\n", argc);
		exit (1);
	}
	jint *args = (*env)->GetIntArrayElements(env, _args, 0);
	/* Object `int []` pinned */

	gpu_setKernel (qid, 0,  "selectKernel",  &callback_setKernelSelect, args, NULL);
	gpu_setKernel (qid, 1, "compactKernel", &callback_setKernelCompact, args, NULL);

	(*env)->ReleaseIntArrayElements(env, _args, args, JNI_ABORT);
	return 0;
}

void callback_setKernelSelect (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) args2;

	/* Get all constants */
	int numberOfBytes  = args1[0];
	int numberOfTuples = args1[1];
	int cache_size     = args1[2]; /* Local buffer size */

	int error = 0;
	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int), (void *)   &numberOfBytes);
	error |= clSetKernelArg (kernel, 1, sizeof(int), (void *)  &numberOfTuples);
	/* Set I/O byte buffers */
	error |= clSetKernelArg (
		kernel,
		2,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		3,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		4,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[1]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		5,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[2]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		6,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[3]->device_buffer));
	/* Set local memory */
	error |= clSetKernelArg (kernel, 7, (size_t) cache_size, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return;
}

void callback_setKernelCompact (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	/* The configuration of this kernel is identical to the select kernel. */
	callback_setKernelSelect (kernel, context, args1, args2);
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelThetaJoin
	(JNIEnv *env, jobject obj, jint qid, jintArray _args) {

	(void) obj;

	jsize argc = (*env)->GetArrayLength(env, _args);
	if (argc != 4) { /* # theta-join kernel constants */
		fprintf(stderr, "error: invalid number of kernel arguments (expected 4, found %d)\n", argc);
		exit (1);
	}

	jint *args = (*env)->GetIntArrayElements(env, _args, 0);
	/* Object `int []` pinned */

	gpu_setKernel (qid, 0, "countKernel",   &callback_setKernelThetaJoin, args, NULL);
	gpu_setKernel (qid, 1, "scanKernel",    &callback_setKernelThetaJoin, args, NULL);
	gpu_setKernel (qid, 2, "compactKernel", &callback_setKernelThetaJoin, args, NULL);
	gpu_setKernel (qid, 3, "joinKernel",    &callback_setKernelThetaJoin, args, NULL);

	(*env)->ReleaseIntArrayElements(env, _args, args, 0);
	/* Object `int []` released */
	return 0;
}

void callback_setKernelThetaJoin (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) args2;

	/* Get all constants */
	int tuplesInStream1 = args1[0];
	int tuplesInStream2 = args1[1];

	int outputSize = args1[2];
	int  cacheSize = args1[3]; /* Local buffer memory size */

	int error = 0;
	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int), (void *) &tuplesInStream1);
	error |= clSetKernelArg (kernel, 1, sizeof(int), (void *) &tuplesInStream2);
	error |= clSetKernelArg (kernel, 2, sizeof(int), (void *) &outputSize);
	/* Set I/O byte buffers */
	error |= clSetKernelArg (
		kernel,
		3,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		4,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[1]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		5,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[2]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		6,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[3]->device_buffer));

	error |= clSetKernelArg (
		kernel,
		7,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		8,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[1]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		9,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[2]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		10,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[3]->device_buffer));
	/* Set local memory */
	error |= clSetKernelArg (kernel, 11, (size_t)  cacheSize, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelReduce
	(JNIEnv *env, jobject obj, jint qid, jintArray _args1, jlongArray _args2) {

	(void) obj;
	(void) qid;

	jsize argc1 = (*env)->GetArrayLength(env, _args1);
	jsize argc2 = (*env)->GetArrayLength(env, _args2);

	if (argc1 != 4 && argc2 != 2) {
		fprintf(stderr, "error: invalid number of kernel arguments (expected [4, 2], found [%d, %d])\n",
			argc1, argc2);
		exit (1);
	}

	jint  *args1 = (*env)->GetIntArrayElements (env, _args1, 0);
	jlong *args2 = (*env)->GetLongArrayElements(env, _args2, 0);

	/* Set kernel(s) */
	gpu_setKernel (qid, 0, "clearKernel",           &callback_setKernelReduce, args1, args2);
	gpu_setKernel (qid, 1, "computeOffsetKernel",   &callback_setKernelReduce, args1, args2);
	gpu_setKernel (qid, 2, "computePointersKernel", &callback_setKernelReduce, args1, args2);
	gpu_setKernel (qid, 3, "reduceKernel",          &callback_setKernelReduce, args1, args2);

	(*env)->ReleaseIntArrayElements (env, _args1, args1, JNI_ABORT);
	(*env)->ReleaseLongArrayElements(env, _args2, args2, JNI_ABORT);

	return 0;
}

void callback_setKernelReduce (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	int numberOfTuples = args1[0];
	int numberOfBytes  = args1[1];

	int maxNumberOfWindows = args1[2];

	int cache_size = args1[3]; /* Local buffer memory size */

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int),  (void *)     &numberOfTuples);
	error |= clSetKernelArg (kernel, 1, sizeof(int),  (void *)      &numberOfBytes);

	error |= clSetKernelArg (kernel, 2, sizeof(int),  (void *) &maxNumberOfWindows);

	error |= clSetKernelArg (kernel, 3, sizeof(long), (void *)     &previousPaneId);
	error |= clSetKernelArg (kernel, 4, sizeof(long), (void *)        &startOffset);

	/* Set I/O byte buffers */
	error |= clSetKernelArg (
			kernel,
			5,
			sizeof(cl_mem),
			(void *) &(context->kernelInput.inputs[0]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			6,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[0]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			7,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[1]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			8,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[2]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			9,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[3]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			10,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[4]->device_buffer));

	/* Set local memory */
	error |= clSetKernelArg (kernel, 11, (size_t)  cache_size, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_setKernelAggregate
	(JNIEnv *env, jobject obj, jint qid, jintArray _args1, jlongArray _args2) {

	(void) obj;
	(void) qid;
	
	jsize argc1 = (*env)->GetArrayLength(env, _args1);
	jsize argc2 = (*env)->GetArrayLength(env, _args2);
	
	if (argc1 != 6 && argc2 != 2) {
		fprintf(stderr, "error: invalid number of kernel arguments (expected [6, 2], found [%d, %d])\n",
			argc1, argc2);
		exit (1);
	}

	jint  *args1 = (*env)->GetIntArrayElements (env, _args1, 0);
	jlong *args2 = (*env)->GetLongArrayElements(env, _args2, 0);
	
	/* Set kernel(s) */
	gpu_setKernel (qid, 0, "clearKernel",                    &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 1, "computeOffsetKernel",            &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 2, "computePointersKernel",          &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 3, "countWindowsKernel",             &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 4, "aggregateClosingWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 5, "aggregateCompleteWindowsKernel", &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 6, "aggregateOpeningWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 7, "aggregatePendingWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_setKernel (qid, 8, "packKernel",                     &callback_setKernelAggregate, args1, args2);
	
	(*env)->ReleaseIntArrayElements (env, _args1, args1, JNI_ABORT);
	(*env)->ReleaseLongArrayElements(env, _args2, args2, JNI_ABORT);
	
	return 0;
}

void callback_setKernelAggregate (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	int numberOfTuples = args1[0];

	int numberOfInputBytes  = args1[1];
	int numberOfOutputBytes = args1[2];

	int hashTableSize = args1[3];

	int maxNumberOfWindows = args1[4];

	int cache_size = args1[5];
	
	long previousPaneId = args2[0];
	long startOffset    = args2[1];
	
	int error = 0;
	
	error |= clSetKernelArg (kernel, 0, sizeof(int),  (void *)      &numberOfTuples);

	error |= clSetKernelArg (kernel, 1, sizeof(int),  (void *)  &numberOfInputBytes);
	error |= clSetKernelArg (kernel, 2, sizeof(int),  (void *) &numberOfOutputBytes);

	error |= clSetKernelArg (kernel, 3, sizeof(int),  (void *)       &hashTableSize);
	error |= clSetKernelArg (kernel, 4, sizeof(int),  (void *)  &maxNumberOfWindows);

	error |= clSetKernelArg (kernel, 5, sizeof(long), (void *)      &previousPaneId);
	error |= clSetKernelArg (kernel, 6, sizeof(long), (void *)         &startOffset);
	
	/* Set input buffers */
	error |= clSetKernelArg (
			kernel,
			7,
			sizeof(cl_mem),
			(void *) &(context->kernelInput.inputs[0]->device_buffer));
	
	
	/* Set output buffers */
	error |= clSetKernelArg (
			kernel,
			8,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			9,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[1]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			10,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[2]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			11,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[3]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			12,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[4]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			13,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[5]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			14,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[6]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			15,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[7]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			16,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[8]->device_buffer));
	
	/* Set local memory */
	error |= clSetKernelArg (kernel, 17, (size_t) cache_size, (void *) NULL);
	
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	
	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_execute
	(JNIEnv *env, jobject obj, jint qid, jintArray _t1, jintArray _t2) {

	jsize argc = (*env)->GetArrayLength(env, _t1);
	
	jint *t1 = (*env)->GetIntArrayElements(env, _t1, 0);
	jint *t2 = (*env)->GetIntArrayElements(env, _t2, 0);

	size_t *threads         = (size_t *) malloc (argc * sizeof(size_t));
	size_t *threadsPerGroup = (size_t *) malloc (argc * sizeof(size_t));

	int i;
	for (i = 0; i < argc; i++) {
		threads[i]         = (size_t) t1[i];
		threadsPerGroup[i] = (size_t) t2[i];
		dbg("[DBG] kernel %d: %10zu threads %10zu threads/group\n", i, threads[i], threadsPerGroup[i]);
	}
	
	/* Create operator */
	queryOperatorP operator = (queryOperatorP) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	/* Currently, we assume the same execution pattern for all queries */

	operator->args1 = NULL;
	operator->args2 = NULL;
	
	operator->configure = NULL;

	operator->writeInput = callback_writeInput;
	operator->readOutput = callback_readOutput;
	operator->execKernel = callback_execKernel;

	gpu_exec (qid, threads, threadsPerGroup, operator, env, obj);

	/* Free operator */
	if (operator)
		free (operator);

	/* Release Java arrays */
	(*env)->ReleaseIntArrayElements (env, _t1, t1, JNI_ABORT);
	(*env)->ReleaseIntArrayElements (env, _t2, t2, JNI_ABORT);

	/* Free copied memory */
	if (threads)
		free (threads);
	if (threadsPerGroup)
		free (threadsPerGroup);

	return 0;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_executeReduce
	(JNIEnv *env, jobject obj, jint qid, jintArray _t1, jintArray _t2, jlongArray _args2) {

	jsize argc = (*env)->GetArrayLength(env, _t1);

	jint *t1 = (*env)->GetIntArrayElements(env, _t1, 0);
	jint *t2 = (*env)->GetIntArrayElements(env, _t2, 0);

	size_t *threads         = (size_t *) malloc (argc * sizeof(size_t));
	size_t *threadsPerGroup = (size_t *) malloc (argc * sizeof(size_t));

	int i;
	for (i = 0; i < argc; i++) {
		threads[i]         = (size_t) t1[i];
		threadsPerGroup[i] = (size_t) t2[i];
		dbg("[DBG] kernel %d: %10zu threads %10zu threads/group\n", i, threads[i], threadsPerGroup[i]);
	}

	jsize argc2 = (*env)->GetArrayLength(env, _args2);

	if (argc2 != 2) {
		fprintf(stderr, "error: invalid number of kernel arguments (expected 4, found %d)\n", argc2);
		exit (1);
	}

	jlong *args2 = (*env)->GetLongArrayElements(env, _args2, 0);

	/* Create operator */
	queryOperatorP operator = (queryOperatorP) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	/* Currently, we assume the same execution pattern for all queries */

	operator->args1 = NULL;
	operator->args2 = args2;

	operator->configure = callback_configureReduce;

	operator->writeInput = callback_writeInput;
	operator->readOutput = callback_readOutput;
	operator->execKernel = callback_execKernel;

	gpu_exec (qid, threads, threadsPerGroup, operator, env, obj);

	/* Free operator */
	if (operator)
		free (operator);

	/* Release Java arrays */
	(*env)->ReleaseIntArrayElements (env, _t1, t1, JNI_ABORT);
	(*env)->ReleaseIntArrayElements (env, _t2, t2, JNI_ABORT);

	(*env)->ReleaseLongArrayElements(env, _args2, args2, JNI_ABORT);

	/* Free copied memory */
	if (threads)
		free (threads);
	if (threadsPerGroup)
		free (threadsPerGroup);

	return 0;
}

void callback_configureReduce (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) context;
	(void)   args1;

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 3, sizeof(long), (void *) &previousPaneId);
	error |= clSetKernelArg (kernel, 4, sizeof(long), (void *)    &startOffset);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheGPU_executeAggregate
	(JNIEnv *env, jobject obj, jint qid, jintArray _t1, jintArray _t2, jlongArray _args2) {

	jsize argc = (*env)->GetArrayLength(env, _t1);

	jint *t1 = (*env)->GetIntArrayElements(env, _t1, 0);
	jint *t2 = (*env)->GetIntArrayElements(env, _t2, 0);

	size_t *threads         = (size_t *) malloc (argc * sizeof(size_t));
	size_t *threadsPerGroup = (size_t *) malloc (argc * sizeof(size_t));

	int i;
	for (i = 0; i < argc; i++) {
		threads[i]         = (size_t) t1[i];
		threadsPerGroup[i] = (size_t) t2[i];
		dbg("[DBG] kernel %d: %10zu threads %10zu threads/group\n", i, threads[i], threadsPerGroup[i]);
	}

	jsize argc2 = (*env)->GetArrayLength(env, _args2);

	if (argc2 != 2) {
		fprintf(stderr, "error: invalid number of kernel arguments (expected 4, found %d)\n", argc2);
		exit (1);
	}

	jlong *args2 = (*env)->GetLongArrayElements(env, _args2, 0);

	/* Create operator */
	queryOperatorP operator = (queryOperatorP) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	/* Currently, we assume the same execution pattern for all queries */

	operator->args1 = NULL;
	operator->args2 = args2;

	operator->configure = callback_configureAggregate;

	operator->writeInput = callback_writeInput;
	operator->readOutput = callback_readOutput;
	operator->execKernel = callback_execKernel;

	gpu_exec (qid, threads, threadsPerGroup, operator, env, obj);

	/* Free operator */
	if (operator)
		free (operator);

	/* Release Java arrays */
	(*env)->ReleaseIntArrayElements (env, _t1, t1, JNI_ABORT);
	(*env)->ReleaseIntArrayElements (env, _t2, t2, JNI_ABORT);

	(*env)->ReleaseLongArrayElements(env, _args2, args2, JNI_ABORT);

	/* Free copied memory */
	if (threads)
		free (threads);
	if (threadsPerGroup)
		free (threadsPerGroup);

	return 0;
}

void callback_configureAggregate (cl_kernel kernel, gpuContextP context, int *args1, long *args2) {

	(void) context;
	(void)   args1;

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 5, sizeof(long), (void *) &previousPaneId);
	error |= clSetKernelArg (kernel, 6, sizeof(long), (void *)    &startOffset);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

/* Data movement callbacks */

void callback_writeInput (gpuContextP context, JNIEnv *env, jobject obj, int qid, int ndx) {

	(*env)->CallVoidMethod (
			env, obj, writeMethod,
			qid,
			ndx,
			(long) (context->kernelInput.inputs[ndx]->mapped_buffer),
			context->kernelInput.inputs[ndx]->size);

	return ;
}

void callback_readOutput (gpuContextP context, JNIEnv *env, jobject obj, int qid, int ndx, int mark) {
	
	if (context->kernelOutput.outputs[ndx]->doNotMove)
		return;
	
	/* Use the mark */
	int size;
	if (mark >= 0 && (! context->kernelOutput.outputs[ndx]->ignoreMark))
		size = mark;
	 else
		size = context->kernelOutput.outputs[ndx]->size;
	
	if (size > context->kernelOutput.outputs[ndx]->size) {
		fprintf(stderr, "error: invalid mark for query's %d output buffer %d (marked %d bytes; size is %d bytes)\n",
			qid, ndx, size, context->kernelOutput.outputs[ndx]->size);
		exit(1);
	}
	
	/* Copy data across the JNI boundary */
	(*env)->CallVoidMethod (
			env, obj, readMethod,
			qid,
			ndx,
			(long) (context->kernelOutput.outputs[ndx]->mapped_buffer),
			size);

	return;
}

gpuContextP callback_execKernel (gpuContextP context) {
	int i;
	gpuContextP p = pipeline[0];
	#ifdef GPU_VERBOSE
	if (! p)
		dbg("[DBG] (null) callback_execKernel(%p) \n", context);
	else
		dbg("[DBG] %p callback_execKernel(%p)\n", p, context);
	#endif
	/* Shift */
	for (i = 0; i < D - 1; i++) {
		pipeline[i] = pipeline [i + 1];
	}
	pipeline[D - 1] = context;
	return p;
}
