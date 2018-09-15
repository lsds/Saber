//
// Created by george on 15/7/2018.
//

#ifndef EFFICIENTOPERATORS_LRB_UTILS_H
#define EFFICIENTOPERATORS_LRB_UTILS_H

// row representation
// size = 32 bytes
struct PosSpeedStr {
    long timestamp;
    int vehicle;
    float speed;
    int highway;
    int lane;
    int direction;
    int position;
};

// size = 16 bytes
struct DistinctRes {
    long timestamp;
    int vehicle;
    int count;
};

// size = 4 bytes
struct CurActiveCars {
    int vehicle;
};

// size = 32 bytes
struct SegSpeedStr {
    long timestamp;
    int vehicle;
    float speed;
    int highway;
    int lane;
    int direction;
    int segment;
};

// size = 32 bytes
struct VehicleSegEntryStr {
    long timestamp;
    int vehicle;
    float speed;
    int highway;
    int lane;
    int direction;
    int segment;
};

// columnar representation
struct PosSpeedStrCol {
    long *timestamp;
    int *vehicle;
    float *speed;
    int *highway;
    int *lane;
    int *direction;
    int *position;
};

struct CurActiveCarsCol {
    int *vehicle;
};

struct SegSpeedStrCol {
    long *timestamp;
    int *vehicle;
    float *speed;
    int *highway;
    int *lane;
    int *direction;
    int *segment;
};

struct VehicleSegEntryStrCol {
    long *timestamp;
    int *vehicle;
    float *speed;
    int *highway;
    int *lane;
    int *direction;
    int *segment;
};

#endif //EFFICIENTOPERATORS_LRB_UTILS_H