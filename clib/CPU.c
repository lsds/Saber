#include "uk_ac_imperial_lsds_saber_devices_TheCPU.h"
#include <jni.h>

#include <unistd.h>
#include <sched.h>


#include <stdlib.h>
#include "hashTable.h"
#include "LRB_Utils.h"

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

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_optimisedDistinct
  (JNIEnv * env, jobject obj, jobject buffer, jint bufferSize, jint bufferStartPointer, jint bufferEndPointer,
   jobject openingWindowsBuffer, jobject closingWindowsBuffer, jobject pendingWindowsBuffer, jobject completeWindowsBuffer,
   jobject openingWindowsStartPointers, jobject closingWindowsStartPointers,
   jobject pendingWindowsStartPointers, jobject completeWindowsStartPointers,
   jlong streamStartPointer, jlong windowSize, jlong windowSlide, jlong windowPaneSize,
   jint openingWindowsPointer, jint closingWindowsPointer,
   jint pendingWindowsPointer, jint completeWindowsPointer,
   jobject arrayHelperBuffer,
   jint mapSize) {

    (void) obj;

    // Input Buffer
    PosSpeedStr *data= (PosSpeedStr *) env->GetDirectBufferAddress(buffer);
    //int len = env->GetDirectBufferCapacity(buffer); // TODO: cache this or pass it as a parameter
    //const int inputSize = len/32; // 32 is the size of the tuple here

    // Output Buffers
    // TODO: FIX the pointers of the result buffers!!!
    ht_node *openingWindowsResults = (ht_node *) env->GetDirectBufferAddress(openingWindowsBuffer); // the results here are in the
    ht_node *closingWindowsResults = (ht_node *) env->GetDirectBufferAddress(closingWindowsBuffer); // form of the hashtable
    ht_node *pendingWindowsResults = (ht_node *) env->GetDirectBufferAddress(pendingWindowsBuffer);
    DistinctRes *completeWindowsResults = (DistinctRes *) env->GetDirectBufferAddress(completeWindowsBuffer); // the results here are packed
    int * arrayHelper = (int *) env->GetDirectBufferAddress(arrayHelperBuffer);
    int *openingWindowsPointers = (int *) env->GetDirectBufferAddress(openingWindowsStartPointers);
    int *closingWindowsPointers = (int *) env->GetDirectBufferAddress(closingWindowsStartPointers);
    int *pendingWindowsPointers = (int *) env->GetDirectBufferAddress(pendingWindowsStartPointers);
    int *completeWindowsPointers = (int *) env->GetDirectBufferAddress(completeWindowsStartPointers);

    // Set the first pointer for all types of windows
    openingWindowsPointers[0] = openingWindowsPointer;
    closingWindowsPointers[0] = closingWindowsPointer;
    pendingWindowsPointers[0] = pendingWindowsPointer;
    completeWindowsPointers[0] = completeWindowsPointer;

    const long panesPerWindow = windowSize / windowPaneSize;
    const long panesPerSlide = windowSlide / windowPaneSize;

    /*printf("---- \n");
    printf("bufferStartPointer %d \n", bufferStartPointer);
    printf("bufferEndPointer %d \n", bufferEndPointer);
    printf("streamStartPointer %d \n", streamStartPointer);
    printf("data[bufferStartPointer].timestamp %lu, vehicle %d \n", data[bufferStartPointer].timestamp, data[bufferStartPointer].vehicle);
    printf("data[bufferEndPointer-1].timestamp %lu, vehicle %d \n", data[bufferEndPointer-1].timestamp, data[bufferEndPointer].vehicle);
    fflush(stdout);
    printf("windowSize %d \n", windowSize);
    printf("windowSlide %d \n", windowSlide);
    printf("openingWindowsPointer %d \n", openingWindowsPointer);
    printf("closingWindowsPointer %d \n", closingWindowsPointer);
    printf("pendingWindowsPointer %d \n", pendingWindowsPointer);
    printf("completeWindowsPointer %d \n", completeWindowsPointer);
    printf("panesPerWindow %ld \n", panesPerWindow);
    printf("panesPerSlide %ld \n", panesPerSlide);
    printf("windowPaneSize %ld \n", windowPaneSize);
    printf("---- \n");
    fflush(stdout);
*/

    int openingWindows = 0;
    int closingWindows = 0;
    int pendingWindows = 0;
    int completeWindows = 0;

    // Query specific variables
    const int startPositionsSize = (int) ((bufferEndPointer-bufferStartPointer) / windowSlide); // TODO: Fix the way this is defined
    const int endPositionsSize = startPositionsSize; //- windowSize/1024 + 1;
    int *startPositions = (int *) malloc(startPositionsSize* 2 * sizeof(int));
    int *endPositions = (int *) malloc(endPositionsSize* 2 * sizeof(int));
    //int startPositions [startPositionsSize*2];
    //int endPositions [startPositionsSize*2];

    for (int i = 0; i < startPositionsSize*2; i++) {
        startPositions[i] = -1;
        endPositions[i] = -1;
    }

    // TODO: store properly the timestamp of the aggregated values...
    // TODO: add exceptions in the hashtable...
    //const int mapSize = 16;//1024; // TODO: the size of the hashtable is hard-coded at the moment!
    hashtable *map = ht_create(mapSize);
    ht_node *hTable = map->table;

    long tempPane;
    long tempCompletePane = (data[bufferStartPointer].timestamp%windowSlide==0) ?
                            (data[bufferStartPointer].timestamp / windowPaneSize) :
                            (data[bufferStartPointer].timestamp / windowPaneSize) + 1;
    long tempOpenPane = (data[bufferStartPointer].timestamp%windowSlide==0) ?
                        (data[bufferStartPointer].timestamp / windowPaneSize) - 1 :
                        (data[bufferStartPointer].timestamp / windowPaneSize);
    startPositions[0] = bufferStartPointer;
    int currentSlide = 1;
    int currentWindow = 0;
    int currPos = bufferStartPointer;

    if (data[bufferEndPointer-1].timestamp < data[bufferStartPointer].timestamp) {
        printf("The input is messed up...");
        exit(-1);
    }

    // TODO: first compute the boundaries and then add...!!!!

    // TODO: deal with equality by +1 ???
    // TODO: check the next curr position for the definition of window boundaries to deal with same values???
    // TODO: fix the hasComplete
    long activePane;
    bool hasComplete = ((data[bufferEndPointer - 1].timestamp - data[bufferStartPointer].timestamp) / windowPaneSize) >=
                       panesPerWindow;
    // the beginning of the stream. Check if we have at least one complete window so far!
    if (streamStartPointer == 0) {
        tempPane = data[bufferStartPointer].timestamp / windowPaneSize;
        // compute the first window and check if it is complete!
        while (currPos < bufferEndPointer) {
            ht_insert_and_increment(hTable, data[currPos].vehicle, 1, data[currPos].timestamp, mapSize);
            activePane = data[currPos].timestamp / windowPaneSize;
            if (activePane - tempPane >= panesPerSlide) {
                tempPane = activePane;
                startPositions[currentSlide] = currPos;
                currentSlide++;
            }
            if (activePane - tempCompletePane >= panesPerWindow) {
                endPositions[currentWindow] = currPos;
                currentWindow++;
                currPos++;
                completeWindows++;
                break;
            }
            currPos++;
        }

    } else if ((data[bufferEndPointer - 1].timestamp / windowPaneSize) <
               panesPerWindow) { //we still have a pending window until the first full window is closed.
        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / windowPaneSize :
                   data[bufferSize / 32 - 1].timestamp / windowPaneSize;
        while (currPos < bufferEndPointer) {
            ht_insert_and_increment(hTable, data[currPos].vehicle, 1, data[currPos].timestamp, mapSize);
            activePane = data[currPos].timestamp / windowPaneSize;
            if (activePane - tempPane >= panesPerSlide) { // there may be still opening windows
                tempPane = activePane;
                startPositions[currentSlide] = currPos;
                currentSlide++;
            }
            currPos++;
        }
    } else { // this is not the first batch, so we get the previous panes for the closing and opening windows
        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / windowPaneSize :
                   data[bufferSize / 32 - 1].timestamp / windowPaneSize; // TODO: fix this!!
        // compute the closing windows util we reach the first complete window. After this point we start to remove slides!
        // There are two discrete cases depending on the starting timestamp of this batch. In the first we don't count the last closing window, as it is complete.
        //printf("data[bufferStartPointer].timestamp %ld \n", data[bufferStartPointer].timestamp);

        //startPositions[currentSlide] = currPos;
        //currentSlide++;
        while (currPos < bufferEndPointer) {
            ht_insert_and_increment(hTable, data[currPos].vehicle, 1, data[currPos].timestamp, mapSize);

            activePane = data[currPos].timestamp / windowPaneSize;
            if (activePane - tempOpenPane >= panesPerSlide) { // new slide and possible opening windows
                tempOpenPane = activePane;
                // count here and not with the closing windows the starting points of slides!!
                startPositions[currentSlide] = currPos;
                currentSlide++;
                //if (!hasComplete) { // we don't have complete windows, so everything is opening
                // write result to the opening windows
                //    std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                //    openingWindowsPointer += mapSize;
                //    openingWindowsPointers[openingWindows] = openingWindowsPointer - 1;
                //    openingWindows++;
                //}
            }
            if (activePane - tempCompletePane >= panesPerWindow) { // complete window
                if (startPositions[currentSlide - 1] !=
                    currPos) { // check if I have already added this position in the previous step!
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                }
                endPositions[currentWindow] = currPos;
                currentWindow++;
                currPos++;
                completeWindows++;
                break;
            }
            // Does the second check stand???
            if (activePane - tempPane >= panesPerSlide &&
                activePane >= panesPerWindow) {//activePane - tempPane < panesPerWindow) { // closing window
                tempPane = activePane;

                // write result to the closing windows
                std::memcpy(closingWindowsResults + closingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                closingWindowsPointer += mapSize;
                closingWindows++;
                closingWindowsPointers[closingWindows] = closingWindowsPointer; //- 1;
                //printf("activePane %d \n", activePane);
            }
            currPos++;
        }

    }


    int tempStartPosition;
    int tempEndPosition;
    // Check if we have one pending window
    if (streamStartPointer != 0 && !hasComplete) {  // (completeWindows == 0 && closingWindows == 0) {
        // write results
        std::memcpy(pendingWindowsResults + pendingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        pendingWindowsPointer += mapSize;
        pendingWindows++;
        pendingWindowsPointers[pendingWindows] = pendingWindowsPointer; // - 1;
    }

    if (completeWindows == 0  && streamStartPointer == 0) { // We only have one opening window, so we write it and return...
        // write results
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        openingWindowsPointer += mapSize;
        openingWindows++;
        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;
        // We only have one opening window if we start from a valid point in time (the last check in the next if statement)
    } else if (completeWindows == 0  && currentSlide > 1 && data[startPositions[0]].timestamp%windowSlide==0) {
        // write results
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        openingWindowsPointer += mapSize;
        openingWindows++;
        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;
        // todo: fix this!!
        currentWindow++; // in order to skip the check later
        //} else if (completeWindows == 0 && closingWindows > 0) { // We have only opening and closing windows which are already written
    } else if (completeWindows > 0) { // we have at least one complete window...

        //auto resultTimestamp = data[currPos-1].timestamp;
        // write results and pack them for the first complete window in the batch
        for (int i = 0; i < mapSize; i++) {
            completeWindowsResults[completeWindowsPointer].timestamp = hTable[i].timestamp;//resultTimestamp;
            completeWindowsResults[completeWindowsPointer].vehicle = hTable[i].key;
            completeWindowsResults[completeWindowsPointer].count = hTable[i].counter;
            completeWindowsPointer += hTable[i].status; // pack items!!!
        }
        // write in the correct slot, as the value has already been incremented!
        completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;
        //completeWindows++;

        // compute the rest windows
        currPos = endPositions[0];
        tempPane = data[currPos].timestamp/windowPaneSize; //currStartPos = data[currPos].timestamp; //startPositions[currentWindow];
        while (currPos < bufferEndPointer) {
            // remove previous slide
            tempStartPosition = startPositions[currentWindow - 1];
            tempEndPosition = startPositions[currentWindow];
            for (int i = tempStartPosition; i < tempEndPosition; i++) {
                ht_update_counter(hTable, data[i].vehicle, -1, mapSize);
            }
            // add elements from the next slide
            currPos = endPositions[currentWindow - 1] + 1; // take the next position, as we have already computed this value
            while (true) {
                ht_insert_and_increment(hTable, data[currPos].vehicle, 1, data[currPos].timestamp, mapSize);

                activePane = data[currPos].timestamp/windowPaneSize;
                // complete windows
                if (activePane - tempPane >= panesPerSlide ) { //&& (data[bufferEndPointer-1].timestamp/windowPaneSize) - activePane>= panesPerWindow-1
                    tempPane = data[currPos].timestamp/windowPaneSize;
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                    endPositions[currentWindow] = currPos;
                    currentWindow++;
                    // write and pack the complete window result
                    //resultTimestamp = data[currPos].timestamp;
                    for (int i = 0; i < mapSize; i++) {
                        completeWindowsResults[completeWindowsPointer].timestamp = hTable[i].timestamp;//resultTimestamp;
                        completeWindowsResults[completeWindowsPointer].vehicle = hTable[i].key;
                        completeWindowsResults[completeWindowsPointer].count = hTable[i].counter;
                        completeWindowsPointer += hTable[i].status; // pack items!!!
                    }
                    completeWindows++;
                    completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;
                    // increment before exiting for a complete window
                    currPos++;
                    break;
                }
                currPos++;
                if (currPos >= bufferEndPointer) { // we have reached the first open window after all the complete ones
                    // write the first open window if we have already computed the result, otherwise remove the respective tuples
                    if ((data[bufferEndPointer-1].timestamp/windowPaneSize) -
                                (data[tempEndPosition].timestamp/windowPaneSize) < panesPerWindow) {
                        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                        openingWindowsPointer += mapSize;
                        openingWindows++;
                        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;
                    }
                    break;
                }
            }
        }
    }


    // compute the rest opening windows
    //currentWindow += (openingWindows==1); // if opening windows are already one, we have to compute one less
    while (currentWindow < currentSlide - 1) {
        // remove previous slide
        tempStartPosition = startPositions[currentWindow];
        tempEndPosition = startPositions[currentWindow + 1];
        for (int i = tempStartPosition; i < tempEndPosition; i++) {
            ht_update_counter(hTable, data[i].vehicle, -1, mapSize);
        }
        // write result to the opening windows
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        openingWindowsPointer += mapSize;
        openingWindows++;
        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;

        currentWindow++;
    }


    // free resources!!!
    free(startPositions);
    free(endPositions);
    ht_free(map);


    // return the variables required for consistent logic with the Java part
    arrayHelper[0] = openingWindowsPointer * sizeof(ht_node);
    arrayHelper[1] = closingWindowsPointer * sizeof(ht_node);
    arrayHelper[2] = pendingWindowsPointer * sizeof(ht_node);
    arrayHelper[3] = completeWindowsPointer * sizeof(DistinctRes);
    arrayHelper[4] = openingWindows;
    arrayHelper[5] = closingWindows;
    arrayHelper[6] = pendingWindows;
    arrayHelper[7] = completeWindows;

/*
printf("bufferStartPointer %d \n", bufferStartPointer);
printf("bufferEndPointer %d \n", bufferEndPointer);
printf("streamStartPointer %d \n", streamStartPointer);
printf("data[bufferStartPointer].timestamp %lu, vehicle %d \n", data[bufferStartPointer].timestamp, data[bufferStartPointer].vehicle);
printf("data[bufferEndPointer-1].timestamp %lu, vehicle %d \n", data[bufferEndPointer-1].timestamp, data[bufferEndPointer].vehicle);
printf("openingWindowsPointer %d \n", openingWindowsPointer);
printf("closingWindowsPointer %d \n", closingWindowsPointer);
printf("pendingWindowsPointer %d \n", pendingWindowsPointer);
printf("completeWindowsPointer %d \n", completeWindowsPointer);
printf("openingWindows %d \n", openingWindows);
printf("closingWindows %d \n", closingWindows);
printf("pendingWindows %d \n", pendingWindows);
printf("completeWindows %d \n", completeWindows);
printf("---- \n");
fflush(stdout);
*/

/*
printf("bufferStartPointer %d \n", bufferStartPointer);
printf("bufferEndPointer %d \n", bufferEndPointer);

printf("streamStartPointer %d \n", streamStartPointer);
printf("first timestamp %lu \n", data[bufferStartPointer].timestamp);
printf("second timestamp %lu \n", data[bufferEndPointer-1].timestamp);
printf("streamStartPointer %d \n", streamStartPointer);*/
/*printf("openingWindows %d \n", openingWindows);
if (openingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < openingWindows; i++) {
        int base = i * mapSize;
        for (int j = 0; j < mapSize ; j++) {
            printf(" %d, %ld, %d, %d \n", openingWindowsResults[base + j].status, openingWindowsResults[base + j].timestamp,
            openingWindowsResults[base + j].key, openingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}*/

/*printf("closingWindows %d \n", closingWindows);
if (closingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < closingWindows; i++) {
        int base = i * mapSize;
        for (int j = 0; j < mapSize ; j++) {
            printf(" %d, %ld, %d, %d \n", closingWindowsResults[base + j].status, closingWindowsResults[base + j].timestamp,
            closingWindowsResults[base + j].key, closingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}*/

/*printf("pendingWindows %d \n", pendingWindows);
if (pendingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < pendingWindows; i++) {
        int base = i * mapSize;
        for (int j = 0; j < mapSize; j++) {
            printf(" %d, %ld, %d, %d \n", pendingWindowsResults[base + j].status, pendingWindowsResults[base + j].timestamp,
            pendingWindowsResults[base + j].key, pendingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}*/

//printf("completeWindows %d \n", completeWindows);
//fflush(stdout);

/*if (completeWindows > 0) {
    printf("timestamp, key, value \n");
    for (int i = 0;  i < completeWindows; i++) {
        int base = i * mapSize;
        for (int j = 0; j < mapSize; j++) {
            printf("%ld, %d, %d \n", data[bufferStartPointer].timestamp,
            completeWindowsResults[base + j].vehicle, completeWindowsResults[base + j].count);
        }
        printf("------ \n");
    }
}

printf("----xxx---- \n");
fflush(stdout);
*/

    return 0;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_optimisedAggregateHashTables
  (JNIEnv * env, jobject obj,
  jobject buff1, jint start1, jint end1,
  jobject buff2, jint start2, jint end2,
  jint keyLength, jint valueLength, jint intermediateTupleSize, jint mapSize,
  jint numberOfValues,
  jint outputSchemaPad, jboolean pack,
  jobject openingWindowsBuffer, jobject completeWindowsBuffer, jint resultBufferPosition) {

    (void) obj;

    // Input Buffers
    ht_node *buffer1= (ht_node *) env->GetDirectBufferAddress(buff1);
    ht_node *buffer2= (ht_node *) env->GetDirectBufferAddress(buff2);
    //int len = env->GetDirectBufferCapacity(buffer);
    //const int inputSize = len/32; // 32 is the size of the tuple here

    // temp variables for the merging
    int posInB2;
    bool isFound;
    int resultIndex = (pack) ? resultBufferPosition/sizeof(DistinctRes) : resultBufferPosition; //sizeof(ht_node);
    int posInRes = 0;
    int *pendingValidPos; // keep the valid items from the pending window, as they will be reused by other opening windows!
    int pendingIndex = 0;

    // Output Buffers
    ht_node *openingWindowsResults;
    DistinctRes *completeWindowsResults; // the results here are packed

    if (!pack) {
        openingWindowsResults = (ht_node *) env->GetDirectBufferAddress(openingWindowsBuffer);
        pendingValidPos = (int *) malloc(mapSize * sizeof(int));

/*                        printf("--------------buffer1-------------- \n");
                        printf("start1: %d, end1: %d \n", start1, end1);
                        for (int i = start1; i < end1; i++)
                            printf ("idx: %d, status: %d, timestamp: %ld, key: %d, counter: %d \n", i, buffer1[i].status,
                            buffer1[i].timestamp, buffer1[i].key, buffer1[i].counter);

                        fflush(stdout);*/
    } else {
        completeWindowsResults = (DistinctRes *) env->GetDirectBufferAddress(completeWindowsBuffer);
    }

    // Normalise start and end pointers, as we have the struct ???
    /*start1 = start1/sizeof(ht_node);
    end1 = end1/sizeof(ht_node);
    start2 = start2/sizeof(ht_node);
    end2 = end2/sizeof(ht_node);*/



    /*printf("fist hashtable \n");
    printf("start1 %d, end1 %d \n", start1, end1);
    printf("status, timestamp, key, value \n");
    for (int j = start1; j < end1; j++) {
        printf("%d, %ld, %d, %d \n", buffer1[j].status,
               buffer1[j].timestamp, buffer1[j].key, buffer1[j].counter);
    }
    printf("--------- \n");

    printf("second hashtable \n");
    printf("start2 %d, end2 %d \n", start2, end2);
    printf("status, timestamp, key, value \n");
    for (int j = start2; j < end2; j++) {
        printf("%d, %ld, %d, %d \n", buffer2[j].status,
               buffer2[j].timestamp, buffer2[j].key, buffer2[j].counter);
    }
    printf("--------- \n");
    fflush(stdout);*/

    /* Iterate over tuples in first table. Search for key in the hash table.
     * If found, merge the two entries. */
    for (int idx = start1; idx < end1; idx++) {

        if (buffer1[idx].status != 1) /* Skip empty slot */
            continue;

        // search in the correct hashtable by moving the respective pointer
        isFound = ht_get_index(&buffer2[start2], buffer1[idx].key, mapSize, posInB2);
        if (posInB2 < 0) {
            printf ("error in C: open-adress hash table is full \n");
            exit(1);
        }
        posInB2+=start2; // get the correct index;

        if (!isFound) {
            if (pack) {
                /* Copy tuple based on output schema */

                /* Put timestamp */
                completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;
                /* Put key */
                completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;
                /* TODO: Put value(s) */
                for (int i = 0; i < numberOfValues; i++) {
                    completeWindowsResults[resultIndex].count = buffer1[idx].counter;
                }
                // Do I need padding here ???

                resultIndex++;
            } else {
                // we operating already on the hashtable,
                // as b1 and openingWindowsResults are the same, so we don't need to copy anything!

                /* Create a new hash table entry */
                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, mapSize, posInRes);
                if (posInRes < 0 || isFound) {
                    printf ("error in C: failed to insert new key in intermediate hash table \n");
                    exit(1);
                }*/
                /* Mark occupancy */
                //openingWindowsResults[posInRes + resultIndex].status = 1;
                /* Put timestamp */
                //openingWindowsResults[posInRes + resultIndex].timestamp = buffer1[idx].timestamp;
                /* Put key and TODO: value(s) */
                /*openingWindowsResults[posInRes + resultIndex].key = buffer1[idx].key;
                for (int i = 0; i < numberOfValues; i++) {
                    openingWindowsResults[posInRes + resultIndex].counter = buffer1[idx].counter;
                }*/
                /* Put count */
            }
        } else { // merge values based on the number of aggregated values and their types!
            // TODO: now is working only for count!
            if (pack) {
                /* Copy tuple based on output schema */

                /* Put timestamp */
                completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;
                /* Put key */
                completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;
                /* TODO: Put value(s) */
                for (int i = 0; i < numberOfValues; i++) {
                    // TODO: check for types

                    completeWindowsResults[resultIndex].count = buffer1[idx].counter + buffer2[posInB2].counter;
                }
                // Do I need padding here ???

                resultIndex++;
            } else {
                /* Create a new hash table entry */
                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, mapSize, posInRes);

                if (posInRes < 0 || isFound) {
                    printf ("error in C: failed to insert new key in intermediate hash table \n");
                    exit(1);
                }*/

                /* Mark occupancy */
                //openingWindowsResults[idx].status = 1;
                /* Put timestamp */
                //openingWindowsResults[idx].timestamp = buffer1[idx].timestamp;
                /* Put key and TODO: value(s) */
                //openingWindowsResults[idx].key = buffer1[idx].key;
                for (int i = 0; i < numberOfValues; i++) {
                    // TODO: check for types

                    openingWindowsResults[idx].counter += buffer2[posInB2].counter; //buffer1[idx].counter ;
                }
                /* Put count */
            }
            // Unmark occupancy in second buffer
            buffer2[posInB2].status = 0;
            // if it is pending, keep the position in order to restore it later
            if (!pack) {
                pendingValidPos[pendingIndex++] = posInB2;
            }
        }
    }

    /* Iterate over the remaining tuples in the second table. */
    for (int idx = start2; idx < end2; idx ++) {

        if (buffer2[idx].status != 1) /* Skip empty slot */
            continue;

        if (pack) {
            /* Copy tuple based on output schema */

            /* Put timestamp */
            completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp;
            /* Put key */
            completeWindowsResults[resultIndex].vehicle = buffer1[idx].key;
            /* TODO: Put value(s) */
            for (int i = 0; i < numberOfValues; i++) {
                completeWindowsResults[resultIndex].count = buffer1[idx].counter;
            }
            // Do I need padding here ???

            resultIndex++;
        } else {
            /* Create a new hash table entry */
            isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer2[idx].key, mapSize, posInRes);

            if (posInRes < 0 || isFound) {
                printf ("idx: %d, key: %d, posInRes %d, resultIndex %d \n", idx, buffer2[idx].key, posInRes, resultIndex);

                /*for (int i = resultIndex; i < resultIndex+mapSize; i++)
                    printf ("idx: %d, status: %d, timestamp: %ld, key: %d, counter: %d \n", i, openingWindowsResults[i].status,
                    openingWindowsResults[i].timestamp, openingWindowsResults[i].key, openingWindowsResults[i].counter);

                printf("--------------buffer2-------------- \n");
                for (int i = start2; i < end2; i++)
                    printf ("idx: %d, status: %d, timestamp: %ld, key: %d, counter: %d \n", i, buffer2[i].status,
                    buffer2[i].timestamp, buffer2[i].key, buffer2[i].counter);

                printf("--------------buffer1-------------- \n");
                printf("start1: %d, end1: %d \n", start1, end1);
                for (int i = start1; i < end1; i++)
                    printf ("idx: %d, status: %d, timestamp: %ld, key: %d, counter: %d \n", i, buffer1[i].status,
                    buffer1[i].timestamp, buffer1[i].key, buffer1[i].counter);

                fflush(stdout);*/

                printf ("error in C: failed to insert new key in intermediate hash table \n");
                exit(1);
            }

            /* Mark occupancy */
            openingWindowsResults[posInRes + resultIndex].status = 1;
            /* Put timestamp */
            openingWindowsResults[posInRes + resultIndex].timestamp = buffer2[idx].timestamp;
            /* Put key and TODO: value(s) */
            openingWindowsResults[posInRes + resultIndex].key = buffer2[idx].key;
            for (int i = 0; i < numberOfValues; i++) {
                openingWindowsResults[posInRes + resultIndex].counter = buffer2[idx].counter;
            }
            /* Put count */
        }
    }


    if (!pack) {

        resultIndex += mapSize;
        // Remark occupancy in second buffer if it is a pending window
        for (int i = 0; i < pendingIndex; i++) {
            buffer2[pendingValidPos[i]].status = 1;
        }
        free(pendingValidPos);
    }

    /*printf("result \n");
    int startIndex = (pack) ? resultBufferPosition/sizeof(DistinctRes) : resultBufferPosition;
    printf("startIndex %d, resultIndex %d \n", startIndex, resultIndex);
    if (!pack) {
        printf("status, timestamp, key, value \n");
        for (int j = startIndex; j < resultIndex; j++) {
            printf("%d, %ld, %d, %d \n", openingWindowsResults[j].status,
                   openingWindowsResults[j].timestamp, openingWindowsResults[j].key, openingWindowsResults[j].counter);
        }
    } else {
        printf("timestamp, key, value \n");
        for (int j = startIndex; j < resultIndex; j++) {
            printf("%ld, %d, %d \n",
                   completeWindowsResults[j].timestamp, completeWindowsResults[j].vehicle, completeWindowsResults[j].count);
        }
    }
    printf("--------- \n");
    fflush(stdout);*/


    // return the variables required for consistent logic with the Java part
    return (pack) ? resultIndex*sizeof(DistinctRes) : (resultIndex+mapSize)*sizeof(ht_node) ;
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

    printf("Number of instructions per microseconds: %lu \n", (clock_mhz));
    for ( size_t j = 0; j < sizeof( long ); j++ ) {
        // my characters are 8 bit
        result[j] = (( clock_mhz >> ( j << 3 )) & 0xff );
        //printf("result[%d]: %lu \n", j, result[j]);
        fflush(stdout);
    }

    return 0;
}

JNIEXPORT jlong JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_changeTimestamps
  (JNIEnv *env, jobject obj, jobject buffer, jint startPos, jint endPos, jint dataLength, jlong timestamp) {

    (void) obj;

    // Input Buffer
    PosSpeedStr *inputBuffer= (PosSpeedStr *) env->GetDirectBufferAddress(buffer);

    int start = startPos/sizeof(PosSpeedStr);
    int end = endPos/sizeof(PosSpeedStr);
    int changeOffset = dataLength/sizeof(PosSpeedStr);

    int temp = 0;
    //timestamp++;

    //printf("%d, %d, %ld \n", start, end, timestamp);
    //fflush(stdout);
    for (int i = start; i < end; i++) {
        if (temp%changeOffset==0)
            timestamp++;
        inputBuffer[i].timestamp = timestamp;
        temp++;
    }

    return timestamp;

}