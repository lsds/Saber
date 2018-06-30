//
// Created by george on 2/3/2018.
//

#ifndef INTRINSICSTEST_CIRCULARQUEUE_H
#define INTRINSICSTEST_CIRCULARQUEUE_H

#include <stdio.h>
#include <mm_malloc.h>
#include <cstring>
#include <climits>


struct CircularQueue
{
    int rear;
    int front;
    int size;
    int *arr;
    int itemsCounter;
};

static struct CircularQueue* newQueue(int size);
static void enQueue(CircularQueue *queue, int value, int queueArr[]);
static void unsafeEnQueue (CircularQueue *queue, int input [], int start, int end, int queueArr[]);
static int deQueue(CircularQueue *queue);
static void unsafeDeQueue(CircularQueue *queue, int numOfItems);
static void displayQueue(CircularQueue *queue);

static struct CircularQueue* newQueue(int size) {
    struct CircularQueue *queue;
    queue = new CircularQueue();//(struct CircularQueue*)malloc(sizeof(struct CircularQueue));
    queue->arr = (int*) _mm_malloc(size * sizeof(int), 64);
    queue->front=-1;
    queue->rear=-1;
    queue->size = size;
    queue->itemsCounter = 0;
    return queue;
}

static void enQueue(CircularQueue * queue, int value, int queueArr[]) {
    int tFront = queue->front;
    int tRear =queue->rear;
    int tSize = queue->size;

    if ((tFront == 0 && tRear == tSize-1)){ //|| (tRear == tFront-1)) {
        printf("Queue is Full \n");
        throw;
    } else {
        if (tFront == -1) { /* Insert First Element */
            tFront = 0;
            queue->front = tFront;
        }
        tRear++;
        if (tRear == tSize)
            tRear = 0;
        queueArr[tRear] = value;

        queue->rear = tRear;
        //queue->itemsCounter ++;
    }
}

static void unsafeEnQueue (CircularQueue *queue, int input [], int start, int end, int queueArr[]) {
    int tFront = queue->front;
    int tRear =queue->rear;
    int tSize = queue->size;

    if (tFront == -1) { /* Insert First Element */
        tFront = 0;
        queue->front = tFront;
    }

    tRear++;
    int diff = (((end-start) + tRear) < tSize) ? (end-start) : tSize - tRear;
    memcpy(&queueArr[tRear], &input[start], sizeof(int) * diff);

    if (diff != (end-start)) {
        int diff1 = (end-start) - diff;
        memcpy(&queueArr[0], &input[start + diff], sizeof(int) * diff1);
    }

    tRear = (tRear+(end - start)-1);
    if (tRear >= tSize)
        tRear -= tSize;


    queue->rear = tRear;
    //queue->size = tSize;
    //queue->itemsCounter += (end - start);
}

static int deQueue(CircularQueue *queue) {
    if (queue->front == -1) {
        printf("Queue is Empty \n");
        return INT_MIN;
    }
    int data = queue->arr[queue->front];
    queue->arr[queue->front] = -1;
    if (queue->front == queue->rear) {
        queue->front = -1;
        queue->rear = -1;
    }
    else
        queue->front = (queue->front + 1) % queue->size;

    //queue->itemsCounter --;
    return data;
}

static void unsafeDeQueue(CircularQueue *queue, int numOfItems) {
    int tFront = queue->front;
    int tSize = queue->size;
    //queue->front = (queue->front+numOfItems) % queue->size;
    tFront+= numOfItems;
    if (tFront > tSize)
        tFront -= tSize;
    queue->front = tFront;
    //queue->itemsCounter -= numOfItems;
}

static void displayQueue(CircularQueue *queue) {
    if (queue->front == -1) {
        printf("Queue is Empty\n");
        throw;
    }
    printf("Elements in Circular Queue are: \n");
    if (queue->rear >= queue->front) {
        for (int i = queue->front; i <= queue->rear; i++)
            printf("%d ",queue->arr[i]);
    } else {
        for (int i = queue->front; i < queue->size; i++)
            printf("%d ", queue->arr[i]);

        for (int i = 0; i <= queue->rear; i++)
            printf("%d ", queue->arr[i]);
    }
    printf("\n");
}


#endif //INTRINSICSTEST_CIRCULARQUEUE_H
