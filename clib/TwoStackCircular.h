//
// Created by grt17 on 23/3/2018.
//

#ifndef INTRINSICSTEST_TWOSTACKCIRCULAR_H
#define INTRINSICSTEST_TWOSTACKCIRCULAR_H

#include "CircularQueue.h"
#include "Utils.h"

#include <climits>

#include "immintrin.h"
#include "emmintrin.h"

struct TwoStackCircular {
    CircularQueue *queue;
    int size;
    int capacity;
    int inputStackSize;
    int outputStackSize;
    int inputStackPointer;
    int outputStackPointer;
    //int *inputStackValues;
    int inputStackValue;
    int *outputStackValues;
    AggregationType type;
};

static struct TwoStackCircular* newTwoStackCircular(int capacity, AggregationType type);

static void dequeue (TwoStackCircular *queue, int numberOfItems, bool isSIMD);

static void enqueue (TwoStackCircular *queue, int input, int value);

static void enqueueUnsafe (TwoStackCircular *queue, int input [], int start, int end, int value);

static bool isQueueEmpty(TwoStackCircular *queue);

static void printQueue (TwoStackCircular *queue);

static void deleteTwoArrayStackQueue(TwoStackCircular *queue);

typedef union{
    __m256i v;
    int a[8];
}U256i;

typedef union{
    __m128i v;
    int a[4];
}U128i;


static struct TwoStackCircular* newTwoStackCircular (int capacity, AggregationType type) {
    struct TwoStackCircular *twoStacks;
    twoStacks = (struct TwoStackCircular*)malloc(sizeof(struct TwoStackCircular));
    twoStacks->queue = newQueue(capacity);
    twoStacks->size = 0;
    twoStacks->capacity = capacity;
    twoStacks->type = type;
    twoStacks->inputStackSize = 0;
    twoStacks->outputStackSize = 0;
    twoStacks->inputStackPointer = -1;
    twoStacks->outputStackPointer = -1;
    //twoStacks->inputStackValues = (int*) _mm_malloc(capacity * sizeof(int), 64);
    twoStacks->outputStackValues = (int*) _mm_malloc(capacity * sizeof(int), 64);
    switch(type) {
        case MIN:
            twoStacks->inputStackValue= INT_MAX;
            break;
        case MAX:
            twoStacks->inputStackValue = INT_MIN;
            break;
        case CNT:
            twoStacks->inputStackValue= 0;
            break;
        case SUM:
            twoStacks->inputStackValue= 0;
            break;
        case AVG:
            twoStacks->inputStackValue= 0;
            break;
        default:
            printf("Not supported yet. \n ");
            throw;
    }

    return twoStacks;
};

static void dequeue(TwoStackCircular *twoStacks, int numberOfItems, bool isSIMD){
    int diff = (numberOfItems <= (twoStacks->outputStackSize -1)) ? numberOfItems : twoStacks->outputStackSize;
    twoStacks->outputStackPointer += diff*1; //queue->outputStack->topPointer-=diff*1; //pop(queue->outputStack);
    twoStacks->outputStackSize -= diff*1;    //queue->outputStackAggr->topPointer-=diff*1;
    twoStacks->size-= diff;

    //if (numberOfItems!=1)
        unsafeDeQueue(twoStacks->queue, diff);
/*    else
        unsafeDeQueue(twoStacks->queue);*/

    /*if (twoStacks->outputStackSize==0 && diff!=numberOfItems) {
        swap(twoStacks, isSIMD, numberOfItems);
        twoStacks->outputStackPointer+= (numberOfItems-diff)*1;
        twoStacks->outputStackSize -= (numberOfItems-diff)*1;
        twoStacks->size-= (numberOfItems-diff);
        unsafeDeQueue(twoStacks->queue, (numberOfItems-diff));
    }*/

    if ((twoStacks->outputStackSize)<0) {
        twoStacks->outputStackPointer = -1;
        twoStacks->outputStackSize = 0;
    }
}

static void enqueue(TwoStackCircular *twoStacks, int input, int value) {
    // only works for min now!
    //int tempValue = (twoStacks->inputStackSize==0) ? INT_MAX : twoStacks->inputStackValues[twoStacks->inputStackSize-1];

    //twoStacks->inputStackValues[twoStacks->inputStackSize++] = value;//(value < tempValue) ? value : tempValue;
    twoStacks->inputStackValue = value;
    twoStacks->inputStackSize++;
    enQueue(twoStacks->queue, input, twoStacks->queue->arr); //unsafeEnque
    twoStacks->size++;
    twoStacks->inputStackPointer=twoStacks->queue->rear;
}

static void enqueueUnsafe(TwoStackCircular *twoStacks, int input [], int start, int end, int value){
    // only works for min now!
    int diff = end-start;
    int tempValue = (twoStacks->inputStackSize==0) ? INT_MAX : twoStacks->inputStackValue;//s[twoStacks->inputStackSize-1];

    int startPointer = twoStacks->inputStackSize;
    int endPointer = twoStacks->inputStackSize+diff;
    /*for (int i = startPointer; i<endPointer; i++)
        twoStacks->inputStackValues[i] = value;*/

    twoStacks->inputStackSize+=diff;

    twoStacks->inputStackValue = value;//s[twoStacks->inputStackSize-1] = value;

    unsafeEnQueue(twoStacks->queue, input, start, end, twoStacks->queue->arr); //unsafeEnque
    twoStacks->size+=diff;
    twoStacks->inputStackPointer=twoStacks->queue->rear;
}

static bool isQueueEmpty(TwoStackCircular *twoStacks) {
    return (twoStacks->size == 0);
}

static void printQueue (TwoStackCircular *twoStacks) {
    printf ("Input ");
    if (twoStacks->inputStackSize > 0)
        for (int i = (twoStacks->queue->rear); i > twoStacks->queue->rear-twoStacks->inputStackSize; --i) {
            printf("%d ", twoStacks->queue->arr[i]);
        }
    printf("\n");

    printf ("Values ");
    if (twoStacks->inputStackSize > 0)
        for (int i = 0; i < twoStacks->inputStackSize; ++i) {
            printf("%d ", twoStacks->inputStackValue);//s[i]);
        }    printf("\n");

    printf ("Output ");
    if (twoStacks->outputStackSize > 0)
        for (int i = twoStacks->queue->front; i < twoStacks->queue->front+twoStacks->outputStackSize; ++i) {
            printf("%d ", twoStacks->queue->arr[i]);
        }
    printf("\n");

    printf ("Values ");
    //for (int i = (twoStacks->outputStackSize-1); i >= 0; --i) {
    if (twoStacks->outputStackSize > 0)
        for (int i = 0; i < twoStacks->outputStackSize; ++i) {
            printf("%d ", twoStacks->outputStackValues[i]);
        }
    printf("\n");
}

static void deleteTwoArrayStackQueue(TwoStackCircular *twoStacks) {
    free((void*) twoStacks->queue->arr);
    free((void*) twoStacks->outputStackValues);
    delete twoStacks;
}


#endif //INTRINSICSTEST_TWOSTACKCIRCULAR_H
