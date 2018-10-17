#include "uk_ac_imperial_lsds_saber_devices_TheCPU.h"
#include <jni.h>

#include <unistd.h>
#include <sched.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>

/* Thread affinity library calls */

#ifndef __APPLE__
static cpu_set_t fullSet;

static cpu_set_t *getFullSet (void) {
	static int init = 0;
	if (init == 0) {
		int i;
		int ncores = sysconf(_SC_NPROCESSORS_ONLN);
		CPU_ZERO (&fullSet);
		for (i = 0; i < ncores; i++)
			CPU_SET (i, &fullSet);
		init = 1;
	}
	return &fullSet;
}
#endif

typedef union {
    uint64_t x;
    char *y;
} un_int_helper;


/****************** rdtsc() related ******************/

// the well-known rdtsc(), in 32 and 64 bits versions
// has to be used with a uint_64t
#ifdef __x86_64__
#define rdtsc(val) { \
    unsigned int __a,__d;                                        \
    asm volatile("rdtsc" : "=a" (__a), "=d" (__d));              \
    (val) = ((unsigned long)__a) | (((unsigned long)__d)<<32);   \
}

#else
#define rdtsc(val) __asm__ __volatile__("rdtsc" : "=A" (val))
#endif




JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_getNumCores
	(JNIEnv *env, jobject obj) {
	(void) env;
	(void) obj;
#ifndef __APPLE__
	int ncores = 0;
	ncores = sysconf(_SC_NPROCESSORS_ONLN);
	return ncores;
#else
	return 0;
#endif
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_bind
	(JNIEnv *env, jobject obj, jint core) {
	(void) env;
	(void) obj;
#ifndef __APPLE__
	cpu_set_t set;
	CPU_ZERO (&set);
	CPU_SET  (core, &set);
	return sched_setaffinity (0, sizeof(set), &set);
#else
	(void) core;
	return 0;
#endif
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_unbind
	(JNIEnv *env, jobject obj) {
	(void) env;
	(void) obj;
#ifndef __APPLE__
	return sched_setaffinity (0, sizeof (cpu_set_t), getFullSet());
#else
	return 0;
#endif
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_getCpuId
	(JNIEnv *env, jobject obj) {
	(void) env;
	(void) obj;
#ifndef __APPLE__
	int core = -1;
	cpu_set_t set;
	int error = sched_getaffinity (0, sizeof (set), &set);
	if (error < 0)
		return core; /* -1 */
	for (core = 0; core < CPU_SETSIZE; core++) {
		if (CPU_ISSET (core, &set))
			break;
	}
	return core;
#else
	return 0;
#endif
}


JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_init_1clock
    (JNIEnv * env, jobject obj, jobject buffer) {

    char *result = (char *) env->GetDirectBufferAddress(buffer);

    uint64_t clock_mhz; // clock frequency in MHz (number of instructions per microseconds)

    struct timeval t0, t1;
    uint64_t c0, c1;

    rdtsc(c0);
    gettimeofday(&t0, 0);
    sleep(1);
    rdtsc(c1);
    gettimeofday(&t1, 0);

    //clock_mhz = number of instructions per microseconds
    clock_mhz = (c1 - c0) / ((t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec
      - t0.tv_usec);

    printf("Number of instructions per microsecond: %lu \n", (clock_mhz));
    for ( size_t j = 0; j < sizeof( long ); j++ ) {
        // my characters are 8 bit
        result[j] = (( clock_mhz >> ( j << 3 )) & 0xff );
        //printf("result[%d]: %lu \n", j, result[j]);
        fflush(stdout);
    }

    return 0;
}