#include "uk_ac_imperial_lsds_saber_devices_TheCPU.h"
#include <jni.h>

#include <unistd.h>
#include <sched.h>


#include <stdlib.h>
#include "hashTable.h"
#include "LRB_Utils.h"

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

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_optimisedDistinct
  (JNIEnv * env, jobject obj, jobject buffer, jint bufferStartPointer, jint bufferEndPointer,
   jobject openingWindowsBuffer, jobject closingWindowsBuffer, jobject pendingWindowsBuffer, jobject completeWindowsBuffer,
   jobject openingWindowsStartPointers, jobject closingWindowsStartPointers,
   jobject pendingWindowsStartPointers, jobject completeWindowsStartPointers,
   jlong streamStartPointer, jlong windowSize, jlong windowSlide, jlong windowPaneSize,
   jint openingWindowsPointer, jint closingWindowsPointer,
   jint pendingWindowsPointer, jint completeWindowsPointer,
   jobject arrayHelperBuffer) {

    (void) obj;

    // Input Buffer
    PosSpeedStr *data= (PosSpeedStr *) env->GetDirectBufferAddress(buffer);
    //int len = env->GetDirectBufferCapacity(buffer);
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

    const long panesPerWindow = windowSize / windowPaneSize;
    const long panesPerSlide = windowSlide / windowPaneSize;


printf("---- \n");
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

    const int mapSize = 1024; // TODO: the size of the hashtable is hard-coded at the moment!
    hashtable *map = ht_create(mapSize);
    ht_node *hTable = map->table;

    long tempPane;
    long tempCompletePane = data[bufferStartPointer].timestamp / windowPaneSize;
    long tempOpenPane = data[bufferStartPointer].timestamp / windowPaneSize;
    startPositions[0] = bufferStartPointer;
    int currentSlide = 1;
    int currentWindow = 0;
    int currPos = bufferStartPointer;

    if (data[bufferEndPointer-1].timestamp < data[bufferStartPointer].timestamp) {
        printf("The input is messed up...");
        exit(-1);
    }

    //TODO: deal with equality by +1 ???
    int resultCounter = 0;
    long activePane;
    // the beginning of the stream. Check if we have at least one complete window so far!
    if (streamStartPointer == 0) {
        tempPane = data[bufferStartPointer].timestamp / windowPaneSize;
        // compute the first window and check if it is complete!
        while (currPos < bufferEndPointer) {
            ht_insert_and_increment(hTable, data[currPos].vehicle, 1, mapSize);
            activePane = data[currPos].timestamp/windowPaneSize;
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

    } else if ((data[bufferEndPointer-1].timestamp / windowPaneSize) < panesPerWindow) { //we still have a pending window until the first full window is closed.
            while (currPos < bufferEndPointer) {
                ht_insert_and_increment(hTable, data[currPos].vehicle, 1, mapSize);
                currPos++;
            }
    } else { // this is not the first batch, so we get the previous panes for the closing and opening windows
        tempPane = data[bufferStartPointer-1].timestamp / windowPaneSize;
        bool hasComplete =  ((data[bufferEndPointer].timestamp - data[bufferStartPointer].timestamp) / windowPaneSize) >= panesPerWindow;
        // compute the closing windows util we reach the first complete window. After this point we start to remove slides!
        while (currPos < bufferEndPointer) {
            ht_insert_and_increment(hTable, data[currPos].vehicle, 1, mapSize);

            activePane = data[currPos].timestamp/windowPaneSize;
            if (activePane - tempOpenPane >= panesPerSlide) { // new slide and possible opening windows
                tempOpenPane = activePane;
                // count here and not with the closing windows the starting points of slides!!
                startPositions[currentSlide] = currPos;
                currentSlide++;
                if (!hasComplete) { // we don't have complete windows, so everything is opening
                    // write result to the opening windows
                    std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                    openingWindowsPointer += mapSize;
                    openingWindowsPointers[openingWindows] = openingWindowsPointer - 1;
                    openingWindows++;
                }
            }
            if (activePane - tempCompletePane >= panesPerWindow) { // complete window
                startPositions[currentSlide] = currPos;
                currentSlide++;
                endPositions[currentWindow] = currPos;
                currentWindow++;
                currPos++;
                completeWindows++;
                break;
            }
            // Does the second check stand???
            if (activePane - tempPane >= panesPerSlide && activePane - tempPane < panesPerWindow) { // closing window
                tempPane = activePane;

                // write result to the closing windows
                std::memcpy(closingWindowsResults + closingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                closingWindowsPointer += mapSize;
                closingWindowsPointers[closingWindows] = closingWindowsPointer - 1;
                closingWindows++;
            }
            currPos++;
        }
    }

    auto resultTimestamp = data[currPos-1].timestamp;
    if (completeWindows == 0 && streamStartPointer == 0) { // We only have one opening window, so we write it and return...
        // write results
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        openingWindowsPointer += mapSize;
        openingWindowsPointers[openingWindows] = openingWindowsPointer - 1;
        openingWindows++;
    } else if (completeWindows == 0 && closingWindows == 0) { // We have one pending window and maybe some opening results which are written
        // write results
        std::memcpy(pendingWindowsResults + pendingWindowsPointer, hTable, mapSize * sizeof(ht_node));
        pendingWindowsPointer += mapSize;
        pendingWindowsPointers[pendingWindows] = pendingWindowsPointer - 1;
        pendingWindows++;
        //} else if (completeWindows == 0 && closingWindows > 0) { // We have only opening and closing windows which are already written
    } else if (completeWindows > 0) { // we have at least one complete window...
        // write results and pack them for the first complete window in the batch
        for (int i = 0; i < mapSize; i++) {
            completeWindowsResults[completeWindowsPointer].timestamp = resultTimestamp;
            completeWindowsResults[completeWindowsPointer].vehicle = hTable[i].key;
            completeWindowsResults[completeWindowsPointer].count = hTable[i].counter;
            completeWindowsPointer += hTable[i].status; // pack items!!!
        }
        // write in the correct slot, as the value has already been incremented!
        completeWindowsPointers[completeWindows - 1] = completeWindowsPointer - 1;
        //completeWindows++;
        resultCounter++;

        // compute the rest windows
        int tempStartPosition;
        int tempEndPosition;
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
                ht_insert_and_increment(hTable, data[currPos].vehicle, 1, mapSize);

                activePane = data[currPos].timestamp/windowPaneSize;
                // complete windows
                if (activePane - tempPane >= panesPerSlide) {
                    tempPane = data[currPos].timestamp/windowPaneSize;
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                    endPositions[currentWindow] = currPos;
                    currentWindow++;
                    // write and pack the complete window result
                    resultTimestamp = data[currPos].timestamp;
                    for (int i = 0; i < mapSize; i++) {
                        completeWindowsResults[completeWindowsPointer].timestamp = resultTimestamp;
                        completeWindowsResults[completeWindowsPointer].vehicle = hTable[i].key;
                        completeWindowsResults[completeWindowsPointer].count = hTable[i].counter;
                        completeWindowsPointer += hTable[i].status; // pack items!!!
                    }
                    completeWindowsPointers[completeWindows] = completeWindowsPointer - 1;
                    completeWindows++;
                    // increment before exiting for a complete window
                    currPos++;
                    break;
                }
                currPos++;
                if (currPos >= bufferEndPointer) { // we have reached the first open window after all the complete ones
                    // write the first open window
                    std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, mapSize * sizeof(ht_node));
                    openingWindowsPointer += mapSize;
                    openingWindowsPointers[openingWindows] = openingWindowsPointer - 1;
                    openingWindows++;
                    break;
                }
            }
            resultCounter++;
        }

        // compute the rest opening windows
        currentWindow++; // TODO: why to I have to increment ???
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
            openingWindowsPointers[openingWindows] = openingWindowsPointer - 1;
            openingWindows++;

            resultCounter++;
            currentWindow++;
        }
    }

    // free resources!!!
    free(startPositions);
    free(endPositions);
    ht_free(map);

    // return the variables required for consistent logic with the Java part
    arrayHelper[0] = openingWindowsPointer;
    arrayHelper[1] = closingWindowsPointer;
    arrayHelper[2] = pendingWindowsPointer;
    arrayHelper[3] = completeWindowsPointer;
    arrayHelper[4] = openingWindows;
    arrayHelper[5] = closingWindows;
    arrayHelper[6] = pendingWindows;
    arrayHelper[7] = completeWindows;


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

    return 0;
}
