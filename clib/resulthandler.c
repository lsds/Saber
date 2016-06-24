#include "resulthandler.h"

#include <pthread.h>

static pthread_t thr = NULL;

static void *output_handler (void *args) {

	resultHandlerP p = (resultHandlerP) args;

	JNIEnv *env = p->env;
	JavaVM *jvm = p->jvm;

	(*jvm)->AttachCurrentThread(jvm, (void **) &env, NULL);

#ifndef __APPLE__
	/* Pin this thread to a particular core: 0 is the dispatcher, 1 is the GPU */
	int core = 2;
	cpu_set_t set;
	CPU_ZERO (&set);
	CPU_SET (core, &set);
	sched_setaffinity (0, sizeof(set), &set);
#endif
	fprintf(stdout, "[DBG] result handler attached to core 2\n");
	fflush (stdout);

	/* Unblocks thread waiting for this thread to attach to the JVM */
	p->start = 1;

	while (1) {

		pthread_mutex_lock (p->mutex);
		while (p->count != 1)
			pthread_cond_wait (p->waiting, p->mutex);

		gpu_context_readOutput (p->context, p->readOutput, env, p->obj, p->qid);

		/* Block again */
		p->count -= 1;
		pthread_mutex_unlock (p->mutex);
		pthread_cond_signal  (p->reading);
	}

	(*jvm)->DetachCurrentThread(jvm);
	return (args) ? NULL : args;
}

resultHandlerP result_handler_init (JNIEnv *env) {

	resultHandlerP p = (resultHandlerP) malloc (sizeof(result_handler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	(*env)->GetJavaVM(env, &(p->jvm));
	p->env = env;

	p->qid = -1;
	p->context = NULL;
	p->readOutput  = NULL;
	p->obj = NULL;

	p->start = 0;
	p->count = 0;

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->reading = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->reading, NULL);

	p->waiting = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->waiting, NULL);

	/* Initialise thread */
	if (pthread_create(&thr, NULL, output_handler, (void *) p)) {
		fprintf(stderr, "error: failed to create result handler thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;
}

void result_handler_readOutput (resultHandlerP p,
	int qid,
	gpuContextP context,
	void (*callback)(gpuContextP, JNIEnv *, jobject, int, int, int),
	jobject obj) {

	p->qid = qid;
	p->context = context;
	p->readOutput = callback;
	p->obj = obj;

	/* Notify output handler */
	pthread_mutex_lock (p->mutex);
	p->count = 1;
	pthread_mutex_unlock (p->mutex);
	pthread_cond_signal (p->waiting);
}

void result_handler_waitForReadEvent (resultHandlerP p) {
	pthread_mutex_lock (p->mutex);
	while (p->count == 1) {
		pthread_cond_wait (p->reading, p->mutex);
	}
	pthread_mutex_unlock (p->mutex);
}
