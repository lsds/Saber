#include "timer.h"

#include <string.h>
#include <stdlib.h>

#include <stdio.h>

timerP timer_new () {
	timerP t = (timerP) malloc(sizeof(_timer_t));
	if (! t) {
		fprintf(stderr, "error: out of memory");
		return NULL;
	}
	timer_clear(t);
	return t;
}

void timer_start (timerP t) {
	gettimeofday(&(t->start), NULL);
	t->isRunning = 1;
	return ;
}

void timer_stop (timerP t) {
	gettimeofday(&(t->end), NULL);
	t->isRunning = 0;
	return ;
}

void timer_clear (timerP t) {
	memset(t, 0, sizeof(_timer_t));
	return ;
}

tstamp_t timer_getElapsedTime (timerP t) {
	tstamp_t t_, _t;
	if (t->isRunning)
		timer_stop(t);
	t_ = (tstamp_t) (t->start.tv_sec * 1000000L + t->start.tv_usec);
	_t = (tstamp_t) (t->end.tv_sec * 1000000L + t->end.tv_usec);
	return _t - t_;
}

void timer_free (timerP t) {
	if (t)
		free(t);
	return ;
}
