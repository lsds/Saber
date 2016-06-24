#ifndef __GPU_DEBUG_H_
#define __GPU_DEBUG_H_

#undef GPU_VERBOSE
// #define GPU_VERBOSE

#undef GPU_PROFILE
// #define GPU_PROFILE

#undef dbg
#ifdef GPU_VERBOSE
#	define dbg(fmt, args...) fprintf(stdout, fmt, ## args)
#else
#	define dbg(fmt, args...)
#endif

#endif /* __GPU_DEBUG_H_ */
