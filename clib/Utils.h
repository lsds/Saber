//
// Created by george on 2/3/2018.
//

#ifndef INTRINSICSTEST_UTILS_H
#define INTRINSICSTEST_UTILS_H

//#define DEBUG
#undef DEBUG

#undef dbg
#ifdef DEBUG
#	define dbg(fmt, args...) do { fprintf(stdout, "DEBUG %35s (l. %4d) > " fmt, __FILE__, __LINE__, ## args); fflush(stdout); } while (0)
#else
#	define dbg(fmt, args...)
#endif

#define info(fmt, args...) do { fprintf(stdout, "INFO  %35s (l. %4d) > " fmt, __FILE__, __LINE__, ## args); fflush(stdout); } while (0)

enum AggregationType {MIN, MAX, CNT, SUM, AVG};
enum TimeGranularity {sec, msec, nsec};

#endif //INTRINSICSTEST_UTILS_H
