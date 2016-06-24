#ifndef TIMER_H_
#define TIMER_H_

#include <sys/time.h>

typedef unsigned long long tstamp_t;

typedef struct timer *timerP;
typedef struct timer {
	struct timeval start;
	struct timeval end;
	int isRunning;
} _timer_t;

timerP timer_new ();

void timer_start (timerP);

void timer_stop (timerP);

void timer_clear (timerP);

tstamp_t timer_getElapsedTime (timerP);

void timer_free (timerP);

#endif /* TIMER_H_ */
