#include "uk_ac_imperial_lsds_saber_devices_TheCPU.h"
#include <jni.h>

#include <unistd.h>
#include <sched.h>

#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <float.h>
#include <unistd.h>
#include <mm_malloc.h>

#include "AggregateStacksSIMD.h"

#include "immintrin.h"
#include "emmintrin.h"


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

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_byteBufferMethod
  (JNIEnv * env, jobject obj, jobject buffer, jint startPointer, jint endPointer, jobject resultBuffer,
  jint resultsPointer, jint windowSize, jint windowSlide, jint aggrType) {

    int *data= (int *) env->GetDirectBufferAddress(buffer);
    //int len = env->GetDirectBufferCapacity(buffer);

    int *results= (int *) env->GetDirectBufferAddress(resultBuffer);

    //int windowSize = 1024;
    //int windowSlide = 64;
    AggregationType type = (AggregationType) aggrType;//MIN;
    TimeGranularity granularity = sec;
    int inputSize = (endPointer-startPointer);

    //printf("Input size is: %d", inputSize);
    int sum = 0;
    AggregateStacksSIMD *aggrStacksSIMD;
    aggrStacksSIMD = newAggregateStacksSIMD((int) windowSize, windowSlide, type, true, false, granularity);

    //for (int k = 0; k < 2048; k++)
    //    printf("%d \n", data[startPointer+k]);

    //printf(" %d \n ", startPointer);
    //printf(" %d \n ", endPointer);
    //printf(" %d \n ", inputSize);
    //printf(" %d \n ", resultsPointer);
    //printf(" %d \n ", windowSize);
    //printf(" %d \n ", windowSlide);

    sum += processStacksSIMD(aggrStacksSIMD, data+startPointer, inputSize, results+resultsPointer);

    deleteStacks(aggrStacksSIMD);

    return sum;

    /*int start = (int) startPointer;
    int end = (int) endPointer;
    int diff = end - start;

    int mod = diff%8;

    int pointer = start;
    float min = FLT_MAX;

    if (diff > 16) {
        //__m256i *f4 = (__m256i *) input;
        __m256 minVal1 = _mm256_set1_ps(FLT_MAX);
        int counter;
        //float a_toload[8];
        end = (mod==0) ? end : end-=8;
        for (counter = start; counter < end; counter+=8) {
            float a_toload[8] = {data[counter], data[counter+1],
                                 data[counter+2], data[counter+3],
                                 data[counter+4], data[counter+5],
                                 data[counter+6], data[counter+7]
            };

            //memcpy(a_toload, &data[counter], 8*sizeof(float));
            __m256 loaded_a = _mm256_loadu_ps((const float *) a_toload);
            minVal1 = _mm256_min_ps(minVal1, loaded_a);
        }

        const U256 ins = {minVal1};
        for (int i = 0; i < 8; i++) {
            min = (ins.a[i] < min) ? ins.a[i] : min;
        }
        pointer = (mod==0) ? counter : counter-=8;
    }

    for (int i = pointer;  i <= endPointer; i++)
        min = (data[i] < min) ? data[i] : min;


    return min;*/
 }

 //g++ -std=c++14 -shared -fPIC -O3 -mavx2 -I/usr/include -I$JAVA_HOME/include -I$JAVA_HOME/include/linux  CPU.c CircularQueue.h Utils.h TwoStackCircular.h AggregateStacksSIMD.cpp -o libCPU.so