#include "uk_ac_imperial_lsds_saber_devices_TheCPU.h" 
#include <jni.h>

#include <float.h>
#include <limits.h>

typedef struct {
	long timestamp;
	int _1;
	float _2;
	int _3;
	int _4;
	int _5;
	int _6;
} input_tuple_t __attribute__((aligned(1)));


typedef struct {
	long timestamp;
	int _1;
	float _2;
} output_tuple_t __attribute__((aligned(1)));


#define RANGE_BASED

#define WINDOW_SIZE      300L
#define WINDOW_SLIDE     10L
#define PANES_PER_WINDOW 30L
#define PANES_PER_SLIDE  1L
#define PANE_SIZE        10L
#define BUFFER_SIZE      1073741824


#include <cstdlib> 
#include <cstring> 
#define MAP_SIZE         1024
#define KEY_SIZE         4
#define VALUE_SIZE       4
#define BUCKET_SIZE       1024
enum AggregationType {MIN, MAX};


template <class Value, AggregationType type>
class TwoStacks {
public:
    Value currValue;
private:
    int size;
    Value *queue;
    int queueFront;
    int queueRear;
    int backStackSize;
    int backStackPointer;
    Value backStackValue;
    int frontStackSize;
    int frontStackPointer;
    Value *frontStackValues;
public:
    TwoStacks () : TwoStacks(BUCKET_SIZE) {}
    TwoStacks (int size) {
        // rounding up to next power of 2
        size = round(size);
        this->size = size;
        this->queue = (Value *)  malloc( sizeof(Value) * size );
        this->queueFront = -1;
        this->queueRear = -1;
        this->backStackSize = 0;
        this->backStackPointer = -1;
        this->backStackValue = (type == MIN) ? (Value) FLT_MAX : (Value) FLT_MIN;
        this->frontStackSize = 0;
        this->frontStackPointer = -1;
        this->frontStackValues = (Value *)  malloc( sizeof(Value) * size );
    }

    void insert (Value inputValue) {
        Value tempValue =(this->backStackSize==0) ? ((type == MIN) ? (Value) FLT_MAX : (Value) FLT_MIN) :
                this->backStackValue;
        if (type == MIN) {
            this->backStackValue = (tempValue < inputValue) ? this->backStackValue : inputValue;
        } else {
            this->backStackValue = (tempValue > inputValue) ? this->backStackValue : inputValue;
        }
        this->backStackSize++;
        this->backStackPointer++;
        if (this->backStackPointer == this->size)
            this->backStackPointer = 0;
        enqueue(inputValue);
    }

    void evict () {
        if (this->frontStackSize == 0) // swap if the front stack is empty and we can't evict
            swap ();
        this->frontStackSize--;
        this->frontStackPointer++;
        if (this->frontStackPointer == this->size)
            this->frontStackPointer = 0;
        dequeue();
    }

    Value query () {
        Value tempValue1, tempValue2;
        switch(type) {
            case MIN:
                tempValue1 = (this->frontStackSize == 0) ? (Value) FLT_MAX : this->frontStackValues[this->frontStackSize-1];
                tempValue2 = (this->backStackSize == 0) ? (Value) FLT_MAX : this->backStackValue;
                this->currValue = (tempValue1 < tempValue2) ? tempValue1 : tempValue2;
                break;
            case MAX:
                tempValue1 = (this->frontStackSize == 0) ? (Value) FLT_MIN : this->frontStackValues[this->frontStackSize-1];
                tempValue2 = (this->backStackSize == 0) ? (Value) FLT_MIN : this->backStackValue;
                this->currValue = (tempValue1 > tempValue2) ? tempValue1 : tempValue2;
        }
        return this->currValue;;
    }

    Value getCurrentValue () {
        return this->currValue;
    }

    /*void printQueue () {
        printf("[ ");
        if (this->queueFront < this->queueRear) {
            for (int i = this->queueFront; i < this->queueRear; i++) {
                printf("%ld, ", this->queue[i]);
            }
        } else {
            for (int i = this->queueFront; i < this->size; i++) {
                printf("%ld, ", this->queue[i]);
            }
            for (int i = 0; i < this->queueRear; i++) {
                printf("%ld, ", this->queue[i]);
            }
        }
        printf("] \n");
    }*/

    void deleteTwoStacks () {
        free(this->queue);
        free(this->frontStackValues);
    }

private:
    inline int round (int value) {
        if (!(value && (!(value&(value-1))))) {
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            value++;
        }
        return value;
    }

    inline void enqueue (Value inputValue) {
        if ((this->queueFront == 0 && this->queueRear == this->size-1)){ //|| (tRear == tFront-1)) {
            printf("Queue is Full \n");
            throw;
        } else {
            if (this->queueFront == -1) { /* Insert First Element */
                this->queueFront = 0;
            }
            this->queueRear++;
            if (this->queueRear == this->size)
                this->queueRear = 0;
            queue[this->queueRear] = inputValue;
        }
    }

    inline void dequeue () {
        if (this->queueFront == -1) {
            printf("Queue is Empty \n");
        }
        if (this->queueFront == this->queueRear) {
            this->queueFront = -1;
            this->queueRear = -1;
        }
        else {
            this->queueFront = (this->queueFront + 1);
            if (this->queueFront == this->size)
                this->queueFront = 0;
        }
    }

    inline void swap () {
        Value tempValue = (type == MIN) ? (Value) FLT_MAX : (Value) FLT_MIN;
        int outputIndex;
        int inputIndex = this->backStackPointer;
        int limit = this->backStackSize;
        Value *arr = this->queue;
        Value *outputStackArr = this->frontStackValues;
        for (outputIndex=0 ; outputIndex<limit; outputIndex++) {
            if (type == MIN) {
                tempValue = (tempValue < arr[inputIndex]) ? tempValue : arr[inputIndex];
            } else {
                tempValue = (tempValue > arr[inputIndex]) ? tempValue : arr[inputIndex];
            }
            outputStackArr[outputIndex] = tempValue;
            inputIndex--;
        }
        this->backStackSize = 0;
        this->backStackPointer = -1;
        this->frontStackSize = limit;
        this->frontStackPointer = this->queueRear - this->frontStackSize + 1;
        //if (this->frontStackPointer < 0)
        //    this->frontStackPointer = (this->queueRear + this->frontStackSize) & (this->size -1);
    }
};



template <class Key, class Value>
struct ht_node {
    char status;
    long timestamp;
    Key key;
    Value value;
    Value sum;
    int counter;
    //char padding[3];
};


template <class Key, class Value, AggregationType type>
class hashtable {
private:
    int size;
    struct ht_node<Key, Value> *table;
    TwoStacks<Value, type> *twoStacksArray;

private:

    inline int round (int value) {
        if (!(value && (!(value&(value-1))))) {
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            value++;
        }
        return value;
    }

    inline int hashFunction (const int x) { return (x & (this->size - 1)); }
    inline int hash (const int *key) {
        return hashFunction(*key);
    }
    inline int hash (const long *key) {
        return hashFunction((int) *key);
    }
    inline int hash (const float *key) {
        return hashFunction((int) *key);
    }
    inline int hash (const void *key) {
        /*unsigned */int hashval = 0;
        int i = KEY_SIZE-1;

        unsigned char * diref = (unsigned char *) key;
        /* Convert key to an integer */
        while (i >= 0) { // little-endian
            hashval = hashval << 8;
            hashval += diref[i];
            i--;
        }
        //printf ("%d \n", hashval);
        return hashFunction(hashval);
    }

    inline bool isEqual (const int *v1, const int *v2) { return *v1 == *v2; }
    inline bool isEqual (const long *v1, const long *v2) { return *v1 == *v2; }
    inline bool isEqual (const float *v1, const float *v2) { return *v1 == *v2; }
    inline bool isEqual (const void *v1, const void *v2) { return memcmp(v1, v2, KEY_SIZE) == 0; }

public:

    hashtable () : hashtable(MAP_SIZE) {}
    hashtable (int size) {
        // rounding up to next power of 2
        size = round(size);
        this->size = size;
        this->table = (ht_node<Key, Value> *)  malloc( sizeof(ht_node<Key, Value>) * size );

        this->twoStacksArray = (TwoStacks<Value, type> *)  malloc( sizeof(TwoStacks<Value, type>) * BUCKET_SIZE * size );
        //printf("%d \n", sizeof(ht_node<Key, Value>));
        for (int i = 0; i < size; i++) {
            memset(&this->table[i], 0, sizeof(ht_node<Key, Value>));
            this->twoStacksArray[i] = TwoStacks<Value, type>();
        }
    }

    hashtable (ht_node<Key, Value> * table) : hashtable(table, MAP_SIZE) {}

    hashtable (ht_node<Key, Value> * table, int size) {
        this->table = table;
        this->size = size;
    }

    int getSize () { return this->size; }

    ht_node<Key, Value> * getTable () { return this->table; }

    void insert (const Key * key, const Value value, const long timestamp) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->twoStacksArray[i].insert(value);
                return;
            }
            if (!tempStatus) { // first insert
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->twoStacksArray[i].insert(value);
                return;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->twoStacksArray[i].insert(value);
                return;
            }
            if (!tempStatus) {
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->twoStacksArray[i].insert(value);
                return;
            }
        }

        printf ("error: the hashtable is full \n");
        exit(1);
    }

    void evict (const Key * key) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->twoStacksArray[i].evict();
                return;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->twoStacksArray[i].evict();
                return;
            }
        }

        printf ("error: entry not found \n");
        exit(1);
    }

    int insert_and_modify (const Key * key, const Value value, const long timestamp) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->table[i].sum += value;
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
            if (!tempStatus) { // first insert
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->table[i].sum = value;
                this->table[i].counter = 1;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->table[i].sum += value;
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
            if (!tempStatus) {
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->table[i].sum = value;
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
        }

        printf ("error: the hashtable is full \n");
        exit(1);
    }

    int evict_and_modify (const Key * key, const Value value, const long timestamp) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->table[i].sum -= value;
                this->table[i].counter--;
                this->twoStacksArray[i].evict();
                return 0;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->table[i].sum -= value;
                this->table[i].counter--;
                this->twoStacksArray[i].evict();
                return 0;
            }
        }

        printf ("error: entry not found \n");
        exit(1);
    }

    int insert_and_increment_counter (const Key * key, const Value value, const long timestamp) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
            if (!tempStatus) { // first insert
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->table[i].counter = 1;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
            if (!tempStatus) {
                this->table[i].status = 1;
                this->table[i].timestamp = timestamp;
                memcpy(&this->table[i].key, key, KEY_SIZE); //strcpy(table[hashIndex].key, key);
                this->table[i].counter++;
                this->twoStacksArray[i].insert(value);
                return 0;
            }
        }

        printf ("error: the hashtable is full \n");
        exit(1);
    }

    int evict_and_decrement_counter (const Key * key) {
        int ind = hash(key), i = ind;
        char tempStatus;
        for (; i < this->size; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) { //update
                this->table[i].counter--;
                this->twoStacksArray[i].evict();
                return 0;
            }
        }
        for (i = 0; i < ind; i++) {
            tempStatus = this->table[i].status;
            if (tempStatus && isEqual(&this->table[i].key, key)) {
                this->table[i].counter--;
                this->twoStacksArray[i].evict();
                return 0;
            }
        }

        printf ("error: entry not found \n");
        exit(1);
    }

    void setValues () {
        int size = this->size;
        ht_node<Key, Value> *table = this->table;
        TwoStacks<Value, type> *twoStacks = this->twoStacksArray;
        for (int i = 0; i < size; i++) {
            if (table[i].status) {
                table[i].value = twoStacks[i].query();
            }
        }
    }

    bool get_value (const Key * key, Value &result) {
        int ind = hash(this->size, key), i = ind;
        for (; i < this->size; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                this->table[i].value = this->twoStacksArray[i].query();
                result = this->table[i].value;
                return true;
            }
        }
        for (i = 0; i < ind; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                this->table[i].value = this->twoStacksArray[i].query();
                result = this->table[i].value;
                return true;
            }
        }
        return false;
    }

    bool get_sum (const Key * key, Value &result) {
        int ind = hash(this->size, key), i = ind;
        for (; i < this->size; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                result = this->table[i].sum;
                return true;
            }
        }
        for (i = 0; i < ind; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                result = this->table[i].sum;
                return true;
            }
        }
        return false;
    }

    bool get_counter (const Key * key, int &result) {
        int ind = hash(this->size, key), i = ind;
        for (; i < this->size; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                result = this->table[i].counter;
                return true;
            }
        }
        for (i = 0; i < ind; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                result = this->table[i].counter;
                return true;
            }
        }
        return false;
    }

    bool get_index (const Key * key, int &index) {
        int ind = hash(this->size, key), i = ind;
        for (; i < this->size; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                index = i;
                return true;
            }
            if (!this->table[i].status) {
                index = i;
                return false;
            }
        }
        for (i = 0; i < ind; i++) {
            if ((this->table[i].status) && isEqual(&this->table[i].key, key)) {
                index = i;
                return true;
            }
            if (!this->table[i].status) {
                index = i;
                return false;
            }
        }
        index = -1;
        return false;
    }

    bool get_index (ht_node<Key,Value> * table, const Key * key, int &index) {
        int ind = hash(key), i = ind;
        for (; i < this->size; i++) {
            if ((table[i].status) && isEqual(&table[i].key, key)) {
                index = i;
                return true;
            }
            if (!table[i].status) {
                index = i;
                return false;
            }
        }
        for (i = 0; i < ind; i++) {
            if ((table[i].status) && isEqual(&table[i].key, key)) {
                index = i;
                return true;
            }
            if (!table[i].status) {
                index = i;
                return false;
            }
        }
        index = -1;
        return false;
    }

    void printHashtable () {
        printf("---HashTalbe----\n");
        for (int i = 0; i < this->size; i++) {
            if (this->table[i].status) {
                printf("%d, %f \n", i, this->table[i].value);
            }
        }
    }

    void deleteHashtable () {
        free(this->table);
        for (int i = 0; i < this->size; i++)
            this->twoStacksArray[i].deleteTwoStacks();
    }

    /*~hashtable () {
        free(this->table);
    }*/
};

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_singleOperatorComputation
  (JNIEnv * env, jobject obj, jobject buffer, jint bufferStartPointer, jint bufferEndPointer,
   jobject openingWindowsBuffer, jobject closingWindowsBuffer, jobject pendingWindowsBuffer, jobject completeWindowsBuffer,
   jobject openingWindowsStartPointers, jobject closingWindowsStartPointers,
   jobject pendingWindowsStartPointers, jobject completeWindowsStartPointers,
   jlong streamStartPointer, jint openingWindowsPointer, jint closingWindowsPointer,
   jint pendingWindowsPointer, jint completeWindowsPointer,
   jobject arrayHelperBuffer) {

    (void) obj;

    // Input Buffer
    input_tuple_t *data= (input_tuple_t *) env->GetDirectBufferAddress(buffer);

    // Output Buffers
    ht_node<int, float> *openingWindowsResults = (ht_node<int, float> *) env->GetDirectBufferAddress(openingWindowsBuffer); // the results here are in the
    ht_node<int, float> *closingWindowsResults = (ht_node<int, float> *) env->GetDirectBufferAddress(closingWindowsBuffer); // form of the hashtable
    ht_node<int, float> *pendingWindowsResults = (ht_node<int, float> *) env->GetDirectBufferAddress(pendingWindowsBuffer);
    output_tuple_t *completeWindowsResults = (output_tuple_t *) env->GetDirectBufferAddress(completeWindowsBuffer); // the results here are packed
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

    /*printf("---- \n");
    printf("bufferStartPointer %d \n", bufferStartPointer);
    printf("bufferEndPointer %d \n", bufferEndPointer);
    printf("streamStartPointer %d \n", streamStartPointer);
    printf("data[bufferStartPointer].timestamp %lu, vehicle %d \n", data[bufferStartPointer].timestamp, data[bufferStartPointer]._1);
    printf("data[bufferEndPointer-1].timestamp %lu, vehicle %d \n", data[bufferEndPointer-1].timestamp, data[bufferEndPointer]._1);
    fflush(stdout);
    printf("windowSize %ld \n", WINDOW_SIZE);
    printf("windowSlide %ld \n", WINDOW_SLIDE);
    printf("openingWindowsPointer %d \n", openingWindowsPointer);
    printf("closingWindowsPointer %d \n", closingWindowsPointer);
    printf("pendingWindowsPointer %d \n", pendingWindowsPointer);
    printf("completeWindowsPointer %d \n", completeWindowsPointer);
    printf("panesPerWindow %ld \n", PANES_PER_WINDOW);
    printf("panesPerSlide %ld \n", PANES_PER_SLIDE);
    printf("windowPaneSize %ld \n", PANE_SIZE);
    printf("---- \n");
    fflush(stdout);*/
    int openingWindows = 0;
    int closingWindows = 0;
    int pendingWindows = 0;
    int completeWindows = 0;

    // Query specific variables
    const int startPositionsSize = (int) ((bufferEndPointer-bufferStartPointer) / WINDOW_SLIDE);
    const int endPositionsSize = startPositionsSize; //- WINDOW_SIZE/1024 + 1;
    int *startPositions = (int *) malloc(startPositionsSize* 2 * sizeof(int));
    int *endPositions = (int *) malloc(endPositionsSize* 2 * sizeof(int));

    for (int i = 0; i < startPositionsSize*2; i++) {
        startPositions[i] = -1;
        endPositions[i] = -1;
    }

    long tempPane;
    long tempCompletePane = (data[bufferStartPointer].timestamp%WINDOW_SLIDE==0) ?
                            (data[bufferStartPointer].timestamp / PANE_SIZE) :
                            (data[bufferStartPointer].timestamp / PANE_SIZE) + 1;
    long tempOpenPane = (data[bufferStartPointer].timestamp%WINDOW_SLIDE==0) ?
                        (data[bufferStartPointer].timestamp / PANE_SIZE) - 1 :
                        (data[bufferStartPointer].timestamp / PANE_SIZE);
    startPositions[0] = bufferStartPointer;
    int currentSlide = 1;
    int currentWindow = 0;
    int currPos = bufferStartPointer;

    if (data[bufferEndPointer-1].timestamp < data[bufferStartPointer].timestamp) {
        printf("The input is messed up...");
        exit(-1);
    }

    long activePane;
    bool hasComplete = ((data[bufferEndPointer - 1].timestamp - data[bufferStartPointer].timestamp) / PANE_SIZE) >=
                       PANES_PER_WINDOW;
    int firstPane = data[bufferStartPointer].timestamp / PANE_SIZE;
    bool startingFromPane = (bufferStartPointer==0) ? true : (data[bufferStartPointer].timestamp % firstPane == 0);

	hashtable<int, float, MIN>map; 
	ht_node<int, float> *hTable = map.getTable(); 
	const int ht_node_size = sizeof(ht_node<int, float>); 

    // the beginning of the stream. Check if we have at least one complete window so far!
    if (streamStartPointer == 0) {
        tempPane = data[bufferStartPointer].timestamp / PANE_SIZE;
        // compute the first window and check if it is complete!
        while (currPos < bufferEndPointer) {
            activePane = data[currPos].timestamp / PANE_SIZE;
            if (activePane - tempPane >= PANES_PER_SLIDE) {
                tempPane = activePane;
                startPositions[currentSlide] = currPos;
                currentSlide++;
            }
            if (activePane - tempCompletePane >= PANES_PER_WINDOW) {
                endPositions[currentWindow] = currPos;
                currentWindow++;
                //currPos++;
                completeWindows++;
                break;
            }
			map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
            currPos++;
        }

    } else if ((data[bufferEndPointer - 1].timestamp / PANE_SIZE) <
               PANES_PER_WINDOW) { //we still have a pending window until the first full window is closed.
        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / PANE_SIZE :
                   data[BUFFER_SIZE / sizeof(input_tuple_t) - 1].timestamp / PANE_SIZE;
        while (currPos < bufferEndPointer) {
			map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
            activePane = data[currPos].timestamp / PANE_SIZE;
            if (activePane - tempPane >= PANES_PER_SLIDE) { // there may be still opening windows
                tempPane = activePane;
                startPositions[currentSlide] = currPos;
                currentSlide++;
            }
            currPos++;
        }
    } else { // this is not the first batch, so we get the previous panes for the closing and opening windows
        tempPane = (bufferStartPointer != 0) ? data[bufferStartPointer - 1].timestamp / PANE_SIZE :
                   data[BUFFER_SIZE / sizeof(input_tuple_t) - 1].timestamp / PANE_SIZE; // TODO: fix this!!
        // compute the closing windows util we reach the first complete window. After this point we start to remove slides!
        // There are two discrete cases depending on the starting timestamp of this batch. In the first we don't count the last closing window, as it is complete.
        //printf("data[bufferStartPointer].timestamp %ld \n", data[bufferStartPointer].timestamp);
        while (currPos < bufferEndPointer) {
            activePane = data[currPos].timestamp / PANE_SIZE;
            if (activePane - tempCompletePane >= PANES_PER_WINDOW) { // complete window
                if (startPositions[currentSlide - 1] !=
                    currPos) { // check if I have already added this position in the previous step!
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                }
                endPositions[currentWindow] = currPos;
                currentWindow++;
                //currPos++;
                completeWindows++;
                break;
            }
            // Does the second check stand???
            if (activePane - tempPane >= PANES_PER_SLIDE &&
                activePane >= PANES_PER_WINDOW) {//activePane - tempPane < PANES_PER_WINDOW) { // closing window
                tempPane = activePane;
                map.setValues();
                // write result to the closing windows
                std::memcpy(closingWindowsResults + closingWindowsPointer, hTable, MAP_SIZE * ht_node_size);
                closingWindowsPointer += MAP_SIZE;
                closingWindows++;
                closingWindowsPointers[closingWindows] = closingWindowsPointer; //- 1;
                //printf("activePane %d \n", activePane);
            }
			map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
            if (activePane - tempOpenPane >= PANES_PER_SLIDE) { // new slide and possible opening windows
                tempOpenPane = activePane;
                // count here and not with the closing windows the starting points of slides!!
                //startPositions[currentSlide] = currPos;
                //currentSlide++;
                if (startPositions[currentSlide - 1] !=
                    currPos) { // check if I have already added this position in the previous step!
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                }
            }
            currPos++;
        }
        // remove the extra values so that we have the first complete window
        if (completeWindows > 0 && !startingFromPane) {
            for (int i = bufferStartPointer; i < startPositions[1]; i++ ) {
				map.evict(&data[i]._1);
            }
        }
    }
    map.setValues();
    int tempStartPosition;
    int tempEndPosition;
    // Check if we have one pending window
    if (streamStartPointer != 0 && !hasComplete) {  // (completeWindows == 0 && closingWindows == 0) {
        // write results
        std::memcpy(pendingWindowsResults + pendingWindowsPointer, hTable, MAP_SIZE * ht_node_size);
        pendingWindowsPointer += MAP_SIZE;
        pendingWindows++;
        pendingWindowsPointers[pendingWindows] = pendingWindowsPointer; // - 1;
    }

    if (completeWindows == 0 && ((streamStartPointer == 0) || (currentSlide > 1 && data[startPositions[0]].timestamp%WINDOW_SLIDE==0))) { // We only have one opening window, so we write it and return...
        // write results
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);
        openingWindowsPointer += MAP_SIZE;
        openingWindows++;
        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;
    } else if (completeWindows > 0) { // we have at least one complete window...

        // write results and pack them for the first complete window in the batch
        for (int i = 0; i < MAP_SIZE; i++) {
			completeWindowsResults[completeWindowsPointer].timestamp = hTable[i].timestamp; 
			completeWindowsResults[completeWindowsPointer]._1 = hTable[i].key; 
			completeWindowsResults[completeWindowsPointer]._2 = hTable[i].value; 
			completeWindowsPointer += hTable[i].status; 

        }
        // write in the correct slot, as the value has already been incremented!
        completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;
		map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
        currPos++;

        // compute the rest windows
        currPos = endPositions[0];
        tempPane = data[currPos].timestamp/PANE_SIZE; //currStartPos = data[currPos].timestamp; //startPositions[currentWindow];
        int removalIndex = (startingFromPane) ? currentWindow : currentWindow + 1; 
        while (currPos < bufferEndPointer) {
            // remove previous slide
            tempStartPosition = startPositions[removalIndex - 1];
            tempEndPosition = startPositions[removalIndex];
            for (int i = tempStartPosition; i < tempEndPosition; i++) {
				map.evict(&data[i]._1);
            }
            // add elements from the next slide
            currPos = endPositions[currentWindow - 1] + 1; // take the next position, as we have already computed this value
            while (true) {

                activePane = data[currPos].timestamp/PANE_SIZE;
                // complete windows
                if (activePane - tempPane >= PANES_PER_SLIDE ) { //&& (data[bufferEndPointer-1].timestamp/PANE_SIZE) - activePane>= PANES_PER_WINDOW-1
                    tempPane = data[currPos].timestamp/PANE_SIZE;
                    startPositions[currentSlide] = currPos;
                    currentSlide++;
                    endPositions[currentWindow] = currPos;
                    currentWindow++;
                    // write and pack the complete window result
                    map.setValues();
                    for (int i = 0; i < MAP_SIZE; i++) {
						completeWindowsResults[completeWindowsPointer].timestamp = hTable[i].timestamp; 
						completeWindowsResults[completeWindowsPointer]._1 = hTable[i].key; 
						completeWindowsResults[completeWindowsPointer]._2 = hTable[i].value; 
						completeWindowsPointer += hTable[i].status; 

                    }
                    completeWindows++;
                    completeWindowsPointers[completeWindows] = completeWindowsPointer; // - 1;
                    // increment before exiting for a complete window
					map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
                    currPos++;
                    break;
                }
				map.insert(&data[currPos]._1, data[currPos]._2, data[currPos].timestamp);
                currPos++;
                if (currPos >= bufferEndPointer) { // we have reached the first open window after all the complete ones
                    // write the first open window if we have already computed the result, otherwise remove the respective tuples
                    if ((data[bufferEndPointer-1].timestamp/PANE_SIZE) -
                                (data[tempEndPosition].timestamp/PANE_SIZE) < PANES_PER_WINDOW) {
                        map.setValues();
                        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);
                        openingWindowsPointer += MAP_SIZE;
                        openingWindows++;
                        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;
                    }
                    break;
                }
            }
            removalIndex++;
        }
    }

    // compute the rest opening windows
    //currentWindow += (openingWindows==1); // if opening windows are already one, we have to compute one less
    if (completeWindows > 0) {
        currentWindow = (startingFromPane) ? currentWindow : currentWindow + 1;
    }
    while (currentWindow < currentSlide - 1) {
        // remove previous slide
        tempStartPosition = startPositions[currentWindow];
        tempEndPosition = startPositions[currentWindow + 1];
        currentWindow++;
        if (tempStartPosition == tempEndPosition) continue; 
        for (int i = tempStartPosition; i < tempEndPosition; i++) {
			map.evict(&data[i]._1);
        }
        // write result to the opening windows
        map.setValues();
        std::memcpy(openingWindowsResults + openingWindowsPointer, hTable, MAP_SIZE * ht_node_size);
        openingWindowsPointer += MAP_SIZE;
        openingWindows++;
        openingWindowsPointers[openingWindows] = openingWindowsPointer; // - 1;

    }


/*printf("bufferStartPointer %d \n", bufferStartPointer);
printf("bufferEndPointer %d \n", bufferEndPointer);
printf("streamStartPointer %d \n", streamStartPointer);
printf("data[bufferStartPointer].timestamp %lu, vehicle %d \n", data[bufferStartPointer].timestamp, data[bufferStartPointer]._1);
printf("data[bufferEndPointer-1].timestamp %lu, vehicle %d \n", data[bufferEndPointer-1].timestamp, data[bufferEndPointer]._1);
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

printf("bufferStartPointer %d \n", bufferStartPointer);
printf("bufferEndPointer %d \n", bufferEndPointer);
printf("streamStartPointer %d \n", streamStartPointer);
printf("first timestamp %lu \n", data[bufferStartPointer].timestamp);
printf("second timestamp %lu \n", data[bufferEndPointer-1].timestamp);
printf("streamStartPointer %d \n", streamStartPointer);
printf("openingWindows %d \n", openingWindows);
if (openingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < openingWindows; i++) {
        int base = i * MAP_SIZE;
        for (int j = 0; j < MAP_SIZE ; j++) {
            printf(" %d, %ld, %d, %d \n", openingWindowsResults[base + j].status, openingWindowsResults[base + j].timestamp,
            openingWindowsResults[base + j].key, openingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}

printf("closingWindows %d \n", closingWindows);
if (closingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < closingWindows; i++) {
        int base = i * MAP_SIZE;
        for (int j = 0; j < MAP_SIZE ; j++) {
            printf(" %d, %ld, %d, %d \n", closingWindowsResults[base + j].status, closingWindowsResults[base + j].timestamp,
            closingWindowsResults[base + j].key, closingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}

printf("pendingWindows %d \n", pendingWindows);
if (pendingWindows > 0) {
    printf("occupancy, timestamp, key, value \n");
    for (int i = 0;  i < pendingWindows; i++) {
        int base = i * MAP_SIZE;
        for (int j = 0; j < MAP_SIZE; j++) {
            printf(" %d, %ld, %d, %d \n", pendingWindowsResults[base + j].status, pendingWindowsResults[base + j].timestamp,
            pendingWindowsResults[base + j].key, pendingWindowsResults[base + j].counter);
        }
        printf("------ \n");
    }
}

printf("completeWindows %d \n", completeWindows);
fflush(stdout);

if (completeWindows > 0) {
    printf("timestamp, key, value \n");
    for (int i = 0;  i < completeWindows; i++) {
        int base = i * MAP_SIZE;
        for (int j = 0; j < MAP_SIZE; j++) {
            printf("%ld, %d, %f \n", data[bufferStartPointer].timestamp,
            completeWindowsResults[base + j]._1, completeWindowsResults[base + j]._2);
        }
        printf("------ \n");
    }
}

printf("----xxx---- \n");
fflush(stdout);*/    // free resources!!!
    free(startPositions);
    free(endPositions);
    map.deleteHashtable();

    // return the variables required for consistent logic with the Java part
    arrayHelper[0] = openingWindowsPointer * ht_node_size;
    arrayHelper[1] = closingWindowsPointer * ht_node_size;
    arrayHelper[2] = pendingWindowsPointer * ht_node_size;
    arrayHelper[3] = completeWindowsPointer * sizeof(output_tuple_t);
    arrayHelper[4] = openingWindows;
    arrayHelper[5] = closingWindows;
    arrayHelper[6] = pendingWindows;
    arrayHelper[7] = completeWindows;

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
    ht_node<int, float> *buffer1= (ht_node<int, float> *) env->GetDirectBufferAddress(buff1);
    ht_node<int, float> *buffer2= (ht_node<int, float> *) env->GetDirectBufferAddress(buff2);
    hashtable<int, float, MIN> map1 (buffer1, MAP_SIZE);
    hashtable<int, float, MIN> map2 (buffer2, MAP_SIZE);
    //int len = env->GetDirectBufferCapacity(buffer);
    //const int inputSize = len/32; // 32 is the size of the tuple here

    // temp variables for the merging
    int posInB2;
    bool isFound;
    int resultIndex = (pack) ? resultBufferPosition/sizeof(output_tuple_t) : resultBufferPosition; //sizeof(ht_node);
    int posInRes = 0;
    int *pendingValidPos; // keep the valid items from the pending window, as they will be reused by other opening windows!
    int pendingIndex = 0;

    // Output Buffers
    ht_node<int, float> *openingWindowsResults;
    output_tuple_t *completeWindowsResults; // the results here are packed

    if (!pack) {
        openingWindowsResults = (ht_node<int, float> *) env->GetDirectBufferAddress(openingWindowsBuffer);
        pendingValidPos = (int *) malloc(MAP_SIZE * sizeof(int));
    } else {
        completeWindowsResults = (output_tuple_t *) env->GetDirectBufferAddress(completeWindowsBuffer);
    }

    /* Iterate over tuples in first table. Search for key in the hash table.
     * If found, merge the two entries. */
    for (int idx = start1; idx < end1; idx++) {

        if (buffer1[idx].status != 1) /* Skip empty slot */
            continue;

        // search in the correct hashtable by moving the respective pointer
        isFound = map2.get_index(&buffer2[start2], &buffer1[idx].key, posInB2); //ht_get_index(&buffer2[start2], buffer1[idx].key, MAP_SIZE, posInB2);
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
				completeWindowsResults[resultIndex]._1 = buffer1[idx].key; 
				/* Put value(s) */
				completeWindowsResults[resultIndex]._2 = buffer1[idx].value; 
				resultIndex ++; 

                // Do I need padding here ???

            } else {
                // we operating already on the hashtable,
                // as b1 and openingWindowsResults are the same, so we don't need to copy anything!

                /* Create a new hash table entry */
                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, MAP_SIZE, posInRes);
                if (posInRes < 0 || isFound) {
                    printf ("error in C: failed to insert new key in intermediate hash table \n");
                    exit(1);
                }*/
            }
        } else { // merge values based on the number of aggregated values and their types!
            // TODO: now is working only for count!
            if (pack) {
                /* Copy tuple based on output schema */

				/* Put timestamp */
				completeWindowsResults[resultIndex].timestamp = buffer1[idx].timestamp; 
				/* Put key */
				completeWindowsResults[resultIndex]._1 = buffer1[idx].key; 
				/* Put value(s) */
				completeWindowsResults[resultIndex]._2 = (buffer1[idx].value < buffer2[posInB2].value) ? buffer1[idx].value : buffer2[posInB2].value; 
				resultIndex ++; 

                // Do I need padding here ???

            } else {
                /* Create a new hash table entry */
                /*isFound = ht_get_index(&openingWindowsResults[resultIndex], buffer1[idx].key, MAP_SIZE, posInRes);

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
				openingWindowsResults[idx].value = (openingWindowsResults[idx].value < buffer2[posInB2].value) ? openingWindowsResults[idx].value : buffer2[posInB2].value;
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
			completeWindowsResults[resultIndex].timestamp = buffer2[idx].timestamp; 
			/* Put key */
			completeWindowsResults[resultIndex]._1 = buffer2[idx].key; 
			/* Put value(s) */
			completeWindowsResults[resultIndex]._2 = buffer2[idx].value; 
			resultIndex ++; 

            // Do I need padding here ???

        } else {
            /* Create a new hash table entry */
            isFound = map2.get_index(&openingWindowsResults[resultIndex], &buffer2[idx].key, posInRes); //ht_get_index(&openingWindowsResults[resultIndex], buffer2[idx].key, MAP_SIZE, posInRes);

            if (posInRes < 0 || isFound) {

                printf ("error in C: failed to insert new key in intermediate hash table \n");
                exit(1);
            }

			/* Mark occupancy */ 
			openingWindowsResults[posInRes + resultIndex].status = 1; 
			/* Put timestamp */ 
			openingWindowsResults[posInRes + resultIndex].timestamp = buffer2[idx].timestamp; 
			/* Put key and TODO: value(s) */ 
			openingWindowsResults[posInRes + resultIndex].key = buffer2[idx].key; 
			openingWindowsResults[posInRes + resultIndex].value = buffer2[idx].value; 

        }
    }


    if (!pack) {

        resultIndex += MAP_SIZE;
        // Remark occupancy in second buffer if it is a pending window
        for (int i = 0; i < pendingIndex; i++) {
            buffer2[pendingValidPos[i]].status = 1;
        }
        free(pendingValidPos);
    }

    // return the variables required for consistent logic with the Java part
    return (pack) ? resultIndex*sizeof(output_tuple_t) : (resultIndex+MAP_SIZE)*sizeof(ht_node<int, float>) ;
}

JNIEXPORT jint JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_getIntermediateTupleSize
  (JNIEnv *env, jobject obj) {
    (void) obj;
    (void) env;
    return sizeof(ht_node<int, float>);
}

JNIEXPORT jlong JNICALL Java_uk_ac_imperial_lsds_saber_devices_TheCPU_changeTimestamps
  (JNIEnv *env, jobject obj, jobject buffer, jint startPos, jint endPos, jint dataLength, jlong timestamp) {

    (void) obj;

    // Input Buffer
    input_tuple_t *inputBuffer= (input_tuple_t *) env->GetDirectBufferAddress(buffer);

    int start = startPos/sizeof(input_tuple_t);
    int end = endPos/sizeof(input_tuple_t);
    int changeOffset = dataLength/sizeof(input_tuple_t);

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
