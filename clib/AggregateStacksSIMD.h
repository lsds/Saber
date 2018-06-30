//
// Created by grt17 on 23/3/2018.
//

#ifndef INTRINSICSTEST_AGGREGATESTACKSSIMD_H
#define INTRINSICSTEST_AGGREGATESTACKSSIMD_H


#include "TwoStackCircular.h"

struct AggregateStacksSIMD {
    int windowSize;
    int windowSlide;
    int previousSize; // for the time-based approach
    int previousSlide;
    int windowPane;
    int currentWindowPane;
    int runningValue;
    int countBasedCounter;
    bool timeBased;
    bool isSIMD;
    AggregationType type;
    TimeGranularity granularity;
    TwoStackCircular *twoStacks; //deallocate queue!!
};

struct AggregateStacksSIMD* newAggregateStacksSIMD(int windowSize, int windowSlide, AggregationType type,  bool isSIMD, bool timeBased, TimeGranularity granularity);

int processStacksSIMD (AggregateStacksSIMD *aggr , int input [], int length, int *results);

inline void computeLimits (AggregateStacksSIMD *aggr , int length, int input []);

template<AggregationType type>
inline void insert (TwoStackCircular *twoStacks, int input [], int start, int end, int windowSlide, bool isSIMD);

inline void insertSingleValue(TwoStackCircular *twoStacks, int input [], int start, int end, int windowSlide, bool isSIMD, AggregationType type);

inline void insertSIMD (AggregateStacksSIMD *aggr, int input [], int start, int end);

inline void insertSIMD_Min (AggregateStacksSIMD *aggr, int input [], int start, int end);

inline void insertSIMD_Sum (AggregateStacksSIMD *aggr, int input [], int start, int end);

inline int computeRunningValue (int tempValue, int inputValue, AggregationType type);

inline void evict (TwoStackCircular *twoStacks, int windowSlide);

template<AggregationType type>
inline int emitStream (TwoStackCircular *twoStacks, bool isSIMD, int windowSlide, int windowSize);

template<AggregationType type>
inline int emitEarlyStream (TwoStackCircular *twoStacks, bool isSIMD, int windowSlide, int windowSize);

inline float getAverage (AggregateStacksSIMD *aggr);

template<AggregationType type>
inline void swap (TwoStackCircular *queue, bool isSIMD, int counter);

void resetStacksSIMD (AggregateStacksSIMD *aggr);

void deleteStacks(AggregateStacksSIMD *aggr);

#endif //INTRINSICSTEST_AGGREGATESTACKSSIMD_H
