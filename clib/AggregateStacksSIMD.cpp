#include <climits>
#include "Utils.h"
#include "AggregateStacksSIMD.h"
#include <stdexcept>
#include <map>
#include <functional>

#include "immintrin.h"
#include "emmintrin.h"


AggregateStacksSIMD* newAggregateStacksSIMD(int windowSize, int windowSlide, AggregationType type,  bool isSIMD, bool timeBased, TimeGranularity granularity) {
    //struct AggregateStacks *aggr = nullptr;
    //aggr = (struct AggregateStacks*)malloc(sizeof(struct AggregateStacks));
    auto aggr = new AggregateStacksSIMD();

    aggr->twoStacks = newTwoStackCircular(1*windowSize, type);

    aggr->windowSize = windowSize;
    aggr->windowSlide = windowSlide;
    aggr->previousSize = windowSize;
    aggr->previousSlide = windowSlide;
    aggr->windowPane = windowSize / windowSlide; // assume that the slide is a multiple of the size
    aggr->currentWindowPane = 0;
    aggr->type = type;
    aggr->isSIMD =  isSIMD;
    aggr->timeBased = timeBased;
    aggr->granularity = granularity;
    aggr->countBasedCounter = 0;
    switch(type) {
        case MIN:
            aggr->runningValue = INT_MAX;
            break;
        case MAX:
            aggr->runningValue = INT_MIN;
            break;
        case CNT:
            aggr->runningValue = 0;
            break;
        case SUM:
            aggr->runningValue = 0;
            break;
        case AVG:
            aggr->runningValue = 0;
            break;
        default:
            printf("Not supported yet. \n ");
            throw;
    }
    return aggr;
}

template<AggregationType type>
int processStacksSIMD (AggregateStacksSIMD *aggr , int input [], int length, int results []) {
    int i = 0;
    int diff = 0;
    int endPointer = 0;

    int tempWindowSize = aggr->windowSize;
    int tempWindowSlide = aggr->windowSlide;
    int tempCounter = aggr->countBasedCounter;
    int tempWindowPane = aggr->windowPane;
    int tempCurWindowPane = aggr->currentWindowPane;
    bool isSIMD = aggr->isSIMD;
    int *inputStack = aggr->twoStacks->queue->arr;
    TwoStackCircular *twoStacks = aggr->twoStacks;

    int resultsPointer = 0;

    bool isFirst = true;

    //printf("Start the computation...\n");
    while (true) {
        while (tempCounter < tempWindowSize && i < length)  {

            diff = (tempWindowSlide+i) < length ? tempWindowSlide : (length-i);
            endPointer = i + diff;
            insert<type>(twoStacks, input, i, endPointer, tempWindowSlide, isSIMD);
            i+=diff;
            tempCounter+=tempWindowSlide;

            if (isFirst) {
                if (tempCounter >= tempWindowSize)
                    break;
                int res = emitEarlyStream<type>(twoStacks, isSIMD, tempWindowSlide, tempWindowSize);
                results[resultsPointer++] = res;
                //printf("First writes... : %d\n", i);
            }
        }
        isFirst = false;
        if (tempCounter >= tempWindowSize) {

            int res = emitStream<type>(twoStacks, isSIMD, tempWindowSlide, tempWindowSize);
            results[resultsPointer++] = res;
            //printf("Writing later... : %d\n", i);
            evict(twoStacks, tempWindowSlide);
            tempCounter-=tempWindowSlide;
        } else {
            int res = emitStream<type>(twoStacks, isSIMD, tempWindowSlide, tempWindowSize);
            results[resultsPointer++] = res;
            printf("Last write ... : %d\n", i);
        }
        if (i == length)
            break;
    }


    aggr->twoStacks->queue->arr = inputStack;
    aggr->twoStacks = twoStacks;
    aggr->windowSize = tempWindowSize;
    aggr->windowSlide = tempWindowSlide;
    aggr->countBasedCounter = tempCounter;
    aggr->currentWindowPane = tempCurWindowPane;
    aggr->windowPane = tempWindowPane;


    return (resultsPointer-1);
}

int processStacksSIMD(AggregateStacksSIMD *aggr , int input [], int length, int results []){
    return std::map<AggregationType,std::function<int(AggregateStacksSIMD * , int [], int, int[])>>{
            {AVG,processStacksSIMD<AggregationType::AVG>},
            {MIN,processStacksSIMD<AggregationType::MIN>},
            {MAX,processStacksSIMD<AggregationType::MAX>},
            {CNT,processStacksSIMD<AggregationType::CNT>},
            {SUM,processStacksSIMD< AggregationType::SUM>},
    }.at(aggr->type)(aggr, input, length, results);
}


template<AggregationType type>
void insert (TwoStackCircular *twoStacks, int input [], int start, int end, int windowSlide, bool isSIMD) {

    int inputStackSize = twoStacks->inputStackSize;
    int inputStackValue = twoStacks->inputStackValue;
    int *queueArr = twoStacks->queue->arr;
    CircularQueue *queue = twoStacks->queue;
    int size = twoStacks->size;

    if (windowSlide < 16 || !isSIMD) { // skip vectorization for 8 integers
        int tempValue; //getStackValue(aggr->twoStacks); //(aggr->queue->inputStack->topPointer==-1) ? INT_MAX :
        switch(type) {
            case MIN:
                tempValue = (inputStackSize == 0) ? INT_MAX : inputStackValue;//s[twoStacks->inputStackSize-1];
                break;
            case MAX:
                tempValue = (inputStackSize == 0) ? INT_MIN : inputStackValue;//s[twoStacks->inputStackSize-1];
                break;
            case CNT:
                tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
                break;
            case SUM:
                tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
                break;
            case AVG:
                tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
        }

        //aggr->queue->inputStack->arr[aggr->queue->inputStack->topPointer];
        int tempRunningValue;
        int i;
        for (i = start; i< end; i++) {

            //temp = top(aggr->queue->inputStack);
            tempRunningValue; //= computeRunningValue (tempValue, input[i], aggr->type);//(tempValue < input[i]) ? tempValue : input[i] ;

            switch(type) {
                case MIN:
                    tempRunningValue = (tempValue < input[i]) ? tempValue : input[i];
                    break;
                case MAX:
                    tempRunningValue = (tempValue > input[i]) ? tempValue : input[i];
                    break;
                case CNT:
                    tempRunningValue = tempValue+1;
                    break;
                case SUM:
                    tempRunningValue = tempValue+input[i];
                    break;
                case AVG:
                    tempRunningValue = tempValue+input[i];
            }
            //enqueue(twoStacks, input[i], tempRunningValue);


            inputStackValue = tempRunningValue;
            //inputStackSize++;
            enQueue(queue, input[i], queueArr); //unsafeEnque
            size++;

            //push(aggr->queue->inputStackAggr, tempRunningValue);
            tempValue = tempRunningValue;
            //aggr->countBasedCounter++;
        }
        twoStacks->inputStackPointer=twoStacks->queue->rear;
        twoStacks->inputStackSize+=(end-start);
        twoStacks->inputStackValue=inputStackValue;
        twoStacks->queue->arr=queueArr;
        twoStacks->size=size;

    }
    else {
        switch (type) {
            case MIN:
            {
                int roundedStart = (start%8==0) ? start : start + (8 - start%8);
                int roundedEnd = (end%8==0) ? end : end + (-end%8);
                int diff = roundedEnd - roundedStart;
                int n = (diff / 8);

                // compute the first elements
                if (start != roundedStart) {
                    int tempRoundedStart = roundedStart;
                    if (n > 2)
                        tempRoundedStart--;
                    insertSingleValue(twoStacks, input, start, tempRoundedStart, windowSlide, isSIMD, type);
                }
                // compute the elements that can be vectorized
                if (n > 0) {
                    int tempMin = (inputStackSize == 0) ? INT_MAX :
                                  inputStackValue;//s[twoStacks->inputStackSize-1];

                    int startingPos = roundedStart / 8;
                    __m256i *f4 = (__m256i *) input;
                    __m256i minVal1 = _mm256_set1_epi32(INT_MAX);
                    for (int i = startingPos; i < startingPos + n; i++) {
                        minVal1 = _mm256_min_epi32(minVal1, f4[i]);
                    }
                    const U256i ins = {minVal1};
                    for (int i = 0; i < 8; i++) {
                        tempMin = (ins.a[i] < tempMin) ? ins.a[i] : tempMin;
                    }

                    /*for (int i = aggr->twoStacks->inputStackSize-1; i<aggr->twoStacks->inputStackSize-1+diff; i++)
                        aggr->twoStacks->inputStackValues[i] = tempMin;
                    aggr->twoStacks->inputStackSize+=diff;*/

                    // insert elements to the circular queue
                    //enqueueUnsafe (aggr->twoStacks, input, roundedStart, roundedEnd, tempMin);

                    int diff = roundedEnd-roundedStart;

                    twoStacks->inputStackValue = tempMin;//s[twoStacks->inputStackSize-1] = value;

                    queueArr = twoStacks->queue->arr;
                    unsafeEnQueue(twoStacks->queue, input, start, end, queueArr); //unsafeEnque
                    twoStacks->size+=diff;
                    twoStacks->inputStackPointer=twoStacks->queue->rear;
                    twoStacks->inputStackSize += diff;
                    //twoStacks->queue->arr=queueArr;

                }
                // compute the remaining elements
                if (end!=roundedEnd)
                    insertSingleValue(twoStacks, input, roundedEnd, end, windowSlide, isSIMD, type);
            }
                break;
            case SUM:
            case AVG:
            {
                int roundedStart = (start%8==0) ? start : start + (8 - start%8);
                int roundedEnd = (end%8==0) ? end : end + (-end%8);
                int diff = roundedEnd - roundedStart;
                int n = (diff / 8);

                // compute the first elements
                if (start != roundedStart) {
                    int tempRoundedStart = roundedStart;
                    if (n>2)
                        tempRoundedStart--;
                    insertSingleValue(twoStacks, input, start, tempRoundedStart, windowSlide, isSIMD, type);
                }

                // compute the elements that can be vectorized
                if (n > 0) {
                    int startingPos = roundedStart / 8;
                    __m256i *f4 = (__m256i *) input;
                    __m256i tempVal = _mm256_set1_epi32(0);
                    for (int i = startingPos; i < startingPos + n; i++) {
                        tempVal = _mm256_add_epi32(tempVal, f4[i]);
                    }
                    tempVal = _mm256_hadd_epi32(tempVal, tempVal);
                    tempVal = _mm256_add_epi32(tempVal, _mm256_permute2f128_si256(tempVal, tempVal, 0x1));
                    __m128i tempSum = _mm_hadd_epi32( _mm256_castsi256_si128(tempVal), _mm256_castsi256_si128(tempVal) );

                    const U128i ins = {tempSum};
                    /*for (int i = aggr->twoStacks->inputStackSize-1; i<aggr->twoStacks->inputStackSize-1+diff; i++)
                        aggr->twoStacks->inputStackValues[i] = ins.a[0];
                    aggr->twoStacks->inputStackSize+=diff;*/

                    // insert elements to the circular queue
                    /*enqueueUnsafe (aggr->twoStacks, input, roundedStart, roundedEnd,
                                   ((inputStackSize == 0) ? 0 : inputStackValue) + ins.a[0]);*/

                    int diff = roundedEnd-roundedStart;

                    twoStacks->inputStackValue = ((inputStackSize == 0) ? 0 : inputStackValue) + ins.a[0];

                    queueArr = twoStacks->queue->arr;
                    unsafeEnQueue(twoStacks->queue, input, start, end, queueArr); //unsafeEnque
                    twoStacks->size+=diff;
                    twoStacks->inputStackPointer=twoStacks->queue->rear;
                    twoStacks->inputStackSize += diff;
                    //twoStacks->queue->arr=queueArr;

                }
                // compute the remaining elements
                if (end!=roundedEnd)
                    insertSingleValue(twoStacks, input, roundedEnd, end, windowSlide, isSIMD, type);
            }
                break;
            case MAX:
            case CNT:
            default:
                printf("Not supported yet \n");
                throw;
        }
    }
}

inline void insertSingleValue (TwoStackCircular *twoStacks, int input [], int start, int end, int windowSlide, bool isSIMD, AggregationType type) {
    int inputStackSize = twoStacks->inputStackSize;
    int inputStackValue = twoStacks->inputStackValue;
    int *queueArr = twoStacks->queue->arr;
    int size = twoStacks->size;
    CircularQueue *queue = twoStacks->queue;

    int tempValue; //getStackValue(aggr->twoStacks); //(aggr->queue->inputStack->topPointer==-1) ? INT_MAX :
    switch(type) {
        case MIN:
            tempValue = (inputStackSize == 0) ? INT_MAX : inputStackValue;//s[twoStacks->inputStackSize-1];
            break;
        case MAX:
            tempValue = (inputStackSize == 0) ? INT_MIN : inputStackValue;//s[twoStacks->inputStackSize-1];
            break;
        case CNT:
            tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
            break;
        case SUM:
            tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
            break;
        case AVG:
            tempValue = (inputStackSize == 0) ? 0 : inputStackValue;//s[twoStacks->inputStackSize-1];
    }

    //aggr->queue->inputStack->arr[aggr->queue->inputStack->topPointer];
    int tempRunningValue;
    int i;
    for (i = start; i< end; i++) {

        //temp = top(aggr->queue->inputStack);
        tempRunningValue; //= computeRunningValue (tempValue, input[i], aggr->type);//(tempValue < input[i]) ? tempValue : input[i] ;

        switch(type) {
            case MIN:
                tempRunningValue = (tempValue < input[i]) ? tempValue : input[i];
                break;
            case MAX:
                tempRunningValue = (tempValue > input[i]) ? tempValue : input[i];
                break;
            case CNT:
                tempRunningValue = tempValue+1;
                break;
            case SUM:
                tempRunningValue = tempValue+input[i];
                break;
            case AVG:
                tempRunningValue = tempValue+input[i];
        }
        //enqueue(twoStacks, input[i], tempRunningValue);


        inputStackValue = tempRunningValue;
        //inputStackSize++;
        enQueue(queue, input[i], queueArr); //unsafeEnque
        size++;

        //push(aggr->queue->inputStackAggr, tempRunningValue);
        tempValue = tempRunningValue;
        //aggr->countBasedCounter++;
    }
    twoStacks->inputStackPointer=twoStacks->queue->rear;
    twoStacks->inputStackSize+=(end-start);
    twoStacks->inputStackValue=inputStackValue;
    twoStacks->queue->arr=queueArr;
    twoStacks->size=size;
}

inline void evict (TwoStackCircular *twoStacks, int numberOfItems){
    twoStacks->outputStackPointer += numberOfItems;
    twoStacks->outputStackSize -= numberOfItems;
    twoStacks->size-= numberOfItems;
    CircularQueue *queue = twoStacks->queue;
    unsafeDeQueue(queue, numberOfItems);
};

template<AggregationType type>
inline int emitStream (TwoStackCircular *twoStacks, bool isSIMD, int windowSlide, int windowSize){
    int outputStackSize = twoStacks->outputStackSize;
    int inputStackSize = twoStacks->inputStackSize;
    int inputStackValue = twoStacks->inputStackValue;
    int *outputStackValues = twoStacks->outputStackValues;


    if (outputStackSize == 0)
        swap<type>(twoStacks, isSIMD, windowSlide);

    outputStackSize = twoStacks->outputStackSize;
    inputStackSize = twoStacks->inputStackSize;
    int tempValue1 = outputStackValues[outputStackSize-1];

    int tempValue2;
    switch(type) {
        case MIN:
            tempValue2 = (inputStackSize == 0) ? INT_MAX : inputStackValue;
            break;
        case MAX:
            tempValue2 = (inputStackSize == 0) ? INT_MIN : inputStackValue;
            break;
        case CNT:
            tempValue2 = (inputStackSize == 0) ? 0 : inputStackValue;
            break;
        case SUM:
            tempValue2 = (inputStackSize == 0) ? 0 : inputStackValue;
            break;
        case AVG:
            tempValue2 = (inputStackSize == 0) ? 0 : inputStackValue;
    }

    int res;
    switch(type) {
        case MIN:
            res = (tempValue1 < tempValue2) ? tempValue1 : tempValue2;
            break;
        case MAX:
            res = (tempValue1 > tempValue2) ? tempValue1 : tempValue2;
            break;
        case CNT:
            res = (tempValue1 + tempValue2);
            break;
        case SUM:
            res = (tempValue1 + tempValue2);
            break;
        case AVG:
            res =  (tempValue1 + tempValue2);
    }

#ifdef DEBUG
    printf("Window with %d size and %d slide \n", windowSize, windowSlide);
    /*if (aggr->type==AVG)
        printf("%.4f \n", getAverage(aggr));
    else */
        printf("%d \n", res);
    printf("\n");
#endif
    return res;
};

template<AggregationType type>
inline int emitEarlyStream (TwoStackCircular *twoStacks, bool isSIMD, int windowSlide, int windowSize){
    int inputStackSize = twoStacks->inputStackSize;
    int inputStackValue = twoStacks->inputStackValue;

    int res;
    switch(type) {
        case MIN:
            res = (inputStackSize == 0) ? INT_MAX : inputStackValue;
            break;
        case MAX:
            res = (inputStackSize == 0) ? INT_MIN : inputStackValue;
            break;
        case CNT:
            res = (inputStackSize == 0) ? 0 : inputStackValue;
            break;
        case SUM:
            res = (inputStackSize == 0) ? 0 : inputStackValue;
            break;
        case AVG:
            res = (inputStackSize == 0) ? 0 : inputStackValue;
    }

#ifdef DEBUG
    printf("Window with %d size and %d slide \n", windowSize, windowSlide);
    /*if (aggr->type==AVG)
        printf("%.4f \n", getAverage(aggr));
    else */
        printf("%d \n", res);
    printf("\n");
#endif
    return res;
};


template<AggregationType type>
void swap(TwoStackCircular *twoStacks, bool isSIMD, int windowSlide) {
    int tempValue;

    switch(type) {
        case MIN:
            tempValue = INT_MAX;
            break;
        case MAX:
            tempValue = INT_MIN;
            break;
        case CNT:
            tempValue = 0;
            break;
        case SUM:
            tempValue = 0;
            break;
        case AVG:
            tempValue = 0;
    }
    int tempTuple;

    int outputIndex = 0;//twoStacks->inputStackSize-1;
    int inputIndex = twoStacks->inputStackPointer;

    int limit = twoStacks->inputStackSize;
    int tempQueueSize = twoStacks->queue->size-1;
    int *arr = twoStacks->queue->arr;
    int *outputStackArr = twoStacks->outputStackValues;
    int tempRear = twoStacks->queue->rear;
    int queueSize = twoStacks->queue->size;
    int inputStackSize = twoStacks->inputStackSize;

    if (windowSlide < 16 || !isSIMD /*|| aggr->timeBased*/) {
        for (outputIndex=0 ; outputIndex<limit; outputIndex++) {
            tempTuple = arr[inputIndex];//queue->inputStack->arr[queue->inputStack->topPointer];
            switch (type) {
                case MIN:
                    tempValue = (tempTuple < tempValue) ? tempTuple : tempValue;
                    break;
                case MAX:
                    tempValue = (tempTuple > tempValue) ? tempTuple : tempValue;
                    break;
                case CNT:
                    tempValue++;
                    break;
                case SUM:
                case AVG:
                    tempValue = tempTuple + tempValue;
            }

            //push(queue->outputStack, tempTuple);
            outputStackArr[outputIndex] = tempValue;
            //outputIndex++;                           //push(queue->outputStackAggr, tempValue);
            inputIndex--;
            /*if (inputIndex < 0)
                inputIndex = tempQueueSize;*/
            //twoStacks->inputStackPointer--;
        }
    } else { //SIMD
        switch (type) {
            case MIN:
            {
                int windowSize = inputStackSize;
                int tempSize =  0;
                while (tempSize < windowSize) {
                    int tempMin = INT_MAX;

                    int tempQueueFront = (tempRear - inputStackSize + 1 + tempSize)%queueSize;
                    int tempQueueRear = (tempQueueFront + windowSlide-1)%queueSize;

                    int roundedFront = (tempQueueFront%8 == 0) ? tempQueueFront : (tempQueueFront) + (8 - (tempQueueFront)%8);
                    int roundedRear = ((tempQueueRear+1)%8 == 0) ? tempQueueRear : (tempQueueRear) + (-(tempQueueRear)%8);

                    int diff1 = roundedRear - roundedFront;
                    int diff2 = (queueSize - 1) - roundedFront;
                    int diff3 = roundedRear;

                    int n1 = 0;
                    int n2 = 0;
                    int endFront = 0; //between initial poisition and rounded front
                    int endRear = 0;  //between the last rounded position and rear
                    int startRear = 0;
                    if (diff1 >= 0) {
                        n1 = (diff1 >= 8) ? ((diff1+1) / 8) : 0; //skip the case when n = 1
                        endFront = (n1<2)? roundedFront+diff1 : roundedFront;
                        if (endFront!=tempQueueFront){
                            if (n1 >= 2) endFront--;
                            for (int i = tempQueueFront; i<=endFront; i++)
                                tempMin = (tempMin > arr[i]) ? arr[i] : tempMin;
                        }
                        if (tempQueueRear!=roundedRear) {
                            startRear = roundedRear;
                            if (roundedRear==endFront || n1>=2) startRear++;
                            for (int i = startRear; i <= tempQueueRear; i++)
                                tempMin = (tempMin > arr[i]) ? arr[i] : tempMin;
                        }
                    }
                    else {
                        n1 = (diff2 >= 8) ? (diff2 / 8) : 0;  //skip the case when n = 1
                        endFront = (n1<2)? roundedFront+diff2-1 : roundedFront;
                        n2 = (diff3 >= 8) ? ((diff3+1) / 8) : 0;  //skip the case when n = 1
                        endRear = (n2<2)? tempQueueRear : roundedRear;
                        if (endFront!=tempQueueFront) {
                            if (n1 >= 2) endFront--;
                            for (int i = tempQueueFront; i <= endFront; i++)
                                tempMin = (tempMin > arr[i]) ? arr[i] : tempMin;
                        }
                        if (endRear!=roundedRear)
                            for (int i = roundedRear; i<=endRear; i++)
                                tempMin = (tempMin > arr[i]) ? arr[i] : tempMin;
                    }

                    // between front and rear or size
                    if (n1 > 1) {
                        int start = roundedFront/8;
                        __m256i *f4 = (__m256i*) arr;
                        __m256i minVal1 = _mm256_set1_epi32(INT_MAX);
                        for (int i = start; i < start+n1; i++) {
                            minVal1 = _mm256_min_epi32(minVal1, f4[i]);
                        }
                        const U256i r1 = {minVal1};
                        for (int i = 0; i<8;i++) {
                            tempMin = (r1.a[i] < tempMin) ? r1.a[i] : tempMin;
                        }
                    }

                    // between start and rear
                    if (n2 > 1) {
                        int start = roundedRear/8;
                        __m256i *f4 = (__m256i*) arr;
                        __m256i minVal2 = _mm256_set1_epi32(INT_MAX);
                        for (int i = 0; i < n2; i++) {
                            minVal2 = _mm256_min_epi32(minVal2, f4[i]);
                        }
                        const U256i r2 = {minVal2};
                        for (int i = 0; i<8;i++) {
                            tempMin = (r2.a[i] < tempMin) ? r2.a[i] : tempMin;
                        }
                    }

                    outputStackArr[inputStackSize - tempSize - 1] = tempMin;
                    tempSize+=windowSlide;
                }
            }
                break;
            case MAX:
            {}
                break;
            case CNT:
                tempValue++;
                break;
            case SUM:
            case AVG:
            {
                int windowSize = inputStackSize;
                int tempSize =  windowSize;

                int writePosition = 0;
                int tempSum = 0;
                while (tempSize > 0) {

                    int tempQueueFront = (tempRear - inputStackSize + 1 + tempSize)%queueSize;
                    int tempQueueRear = (tempQueueFront + windowSlide-1)%queueSize;

                    int roundedFront = (tempQueueFront%8 == 0) ? tempQueueFront : (tempQueueFront) + (8 - (tempQueueFront)%8);
                    int roundedRear = ((tempQueueRear+1)%8 == 0) ? tempQueueRear : (tempQueueRear) + (-(tempQueueRear)%8);

                    int diff1 = roundedRear - roundedFront;
                    int diff2 = (queueSize - 1) - roundedFront;
                    int diff3 = roundedRear;

                    int n1 = 0;
                    int n2 = 0;
                    int endFront = 0; //between initial poisition and rounded front
                    int endRear = 0;  //between the last rounded position and rear
                    int startRear = 0;
                    if (diff1 >= 0) {
                        n1 = (diff1 >= 8) ? ((diff1+1) / 8) : 0; //skip the case when n = 1
                        endFront = (n1<2)? roundedFront+diff1 : roundedFront;
                        if (endFront!=tempQueueFront){
                            if (n1 >= 2) endFront--;
                            for (int i = tempQueueFront; i<=endFront; i++)
                                tempSum += arr[i];
                        }
                        if (tempQueueRear!=roundedRear) {
                            startRear = roundedRear;
                            if (roundedRear==endFront || n1>=2) startRear++;
                            for (int i = startRear; i <= tempQueueRear; i++)
                                tempSum += arr[i];
                        }
                    }
                    else {
                        n1 = (diff2 >= 8) ? (diff2 / 8) : 0;  //skip the case when n = 1
                        endFront = (n1<2)? roundedFront+diff2-1 : roundedFront;
                        n2 = (diff3 >= 8) ? ((diff3+1) / 8) : 0;  //skip the case when n = 1
                        endRear = (n2<2)? tempQueueRear : roundedRear;
                        if (endFront!=tempQueueFront) {
                            if (n1 >= 2) endFront--;
                            for (int i = tempQueueFront; i <= endFront; i++)
                                tempSum += arr[i];
                        }
                        if (endRear!=roundedRear)
                            for (int i = 0; i<=endRear; i++)
                                tempSum += arr[i];
                    }

                    // between front and rear or size
                    if (n1 > 1) {
                        int start = roundedFront/8;
                        __m256i *f4 = (__m256i*) arr;
                        __m256i tempVal1 = _mm256_set1_epi32(0);
                        for (int i = start; i < start+n1; i++) {
                            tempVal1 = _mm256_add_epi32(tempVal1, f4[i]);
                        }
                        tempVal1 = _mm256_hadd_epi32(tempVal1, tempVal1);
                        tempVal1 = _mm256_add_epi32(tempVal1, _mm256_permute2f128_si256(tempVal1, tempVal1, 0x1));
                        __m128i tempSum1 = _mm_hadd_epi32( _mm256_castsi256_si128(tempVal1), _mm256_castsi256_si128(tempVal1) );

                        const U128i r1 = {tempSum1};
                        tempSum += r1.a[0];
                    }

                    // between start and rear
                    if (n2 > 1) {
                        roundedRear++;
                        int start = roundedRear/8;
                        __m256i *f4 = (__m256i*) arr;
                        __m256i tempVal2 = _mm256_set1_epi32(0);
                        for (int i = 0; i < n2; i++) {
                            tempVal2 = _mm256_add_epi32(tempVal2, f4[i]);
                        }
                        tempVal2 = _mm256_hadd_epi32(tempVal2, tempVal2);
                        tempVal2 = _mm256_add_epi32(tempVal2, _mm256_permute2f128_si256(tempVal2, tempVal2, 0x1));
                        __m128i tempSum2 = _mm_hadd_epi32( _mm256_castsi256_si128(tempVal2), _mm256_castsi256_si128(tempVal2) );

                        const U128i r2 = {tempSum2};
                        tempSum += r2.a[0];
                    }

                    writePosition+=windowSlide;
                    outputStackArr[writePosition - 1] = tempSum;
                    tempSize-=windowSlide;
                }
            }
        }

        /*while (outputIndex < limit) {
            outputIndex++;
            inputIndex--;
            if (inputIndex < 0)
                inputIndex = tempQueueSize;
        }*/
        /*outputIndex+= inputIndex;
        inputIndex = 0;*/
    }

    twoStacks->outputStackSize=limit;
    twoStacks->inputStackSize=0;

    twoStacks->queue->arr = arr;
    twoStacks->outputStackValues = outputStackArr;

    twoStacks->outputStackPointer = twoStacks->queue->rear - twoStacks->outputStackSize + 1;
    if (twoStacks->outputStackPointer < 0)
        twoStacks->outputStackPointer = (twoStacks->queue->rear + twoStacks->outputStackSize)% (twoStacks->queue->size -1);

    twoStacks->inputStackPointer = -1;
}

float getAverage(AggregateStacksSIMD *aggr) {
    return (float)aggr->runningValue/(float)aggr->countBasedCounter;
}

void resetStacksSIMD (AggregateStacksSIMD *aggr){
    aggr->twoStacks->queue->rear=-1;
    aggr->twoStacks->queue->front=-1;
    switch(aggr->type) {
        case MIN:
            aggr->runningValue = INT_MAX;
            break;
        case MAX:
            aggr->runningValue = INT_MIN;
            break;
        case CNT:
            aggr->runningValue = 0;
            break;
        case SUM:
            aggr->runningValue = 0;
            break;
        case AVG:
            aggr->runningValue = 0;
    }
    aggr->twoStacks->queue->itemsCounter=0;
    aggr->countBasedCounter=0;

    aggr->twoStacks->size=0;
    aggr->twoStacks->inputStackSize = 0;
    aggr->twoStacks->outputStackSize = 0;
    aggr->twoStacks->inputStackPointer = -1;
    aggr->twoStacks->outputStackPointer = -1;

    aggr->currentWindowPane = 0;
    aggr->windowSize = aggr->previousSize;
    aggr->windowSlide = aggr->previousSlide;
};

void deleteStacks(AggregateStacksSIMD *aggr){

    deleteTwoArrayStackQueue(aggr->twoStacks);
    delete(aggr);
};