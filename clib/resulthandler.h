#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include "gpucontext.h"

#include <jni.h>

#include <pthread.h>

typedef struct result_handler *resultHandlerP;
typedef struct result_handler {
	JavaVM *jvm;
	JNIEnv *env;

	int qid;
	gpuContextP context;
	void (*readOutput) (gpuContextP, JNIEnv *, jobject, int, int, int);
	jobject obj;

	pthread_mutex_t *mutex;
	pthread_cond_t *reading, *waiting;

	volatile unsigned count;
	volatile unsigned start;

} result_handler_t;

resultHandlerP result_handler_init (JNIEnv *);

void result_handler_readOutput (resultHandlerP, int, gpuContextP,
		void (*callback)(gpuContextP, JNIEnv *, jobject, int, int, int), jobject);

void result_handler_waitForReadEvent (resultHandlerP);

#endif /* __RESULT_HANDLER_H_ */
