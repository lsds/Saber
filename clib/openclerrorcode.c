#include "openclerrorcode.h"

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

const char *getErrorMessage (int error) {

	switch (error) {
	case CL_DEVICE_NOT_FOUND:                          return "CL_DEVICE_NOT_FOUND";
	case CL_DEVICE_NOT_AVAILABLE:                      return "CL_DEVICE_NOT_AVAILABLE";
	case CL_COMPILER_NOT_AVAILABLE:                    return "CL_COMPILER_NOT_AVAILABLE";
	case CL_MEM_OBJECT_ALLOCATION_FAILURE:             return "CL_MEM_OBJECT_ALLOCATION_FAILUR";
	case CL_OUT_OF_RESOURCES:                          return "CL_OUT_OF_RESOURCES";
	case CL_OUT_OF_HOST_MEMORY:                        return "CL_OUT_OF_HOST_MEMORY";
	case CL_PROFILING_INFO_NOT_AVAILABLE:              return "CL_PROFILING_INFO_NOT_AVAILABLE";
	case CL_MEM_COPY_OVERLAP:                          return "CL_MEM_COPY_OVERLAP";
	case CL_IMAGE_FORMAT_MISMATCH:                     return "CL_IMAGE_FORMAT_MISMATCH";
	case CL_IMAGE_FORMAT_NOT_SUPPORTED:                return "CL_IMAGE_FORMAT_NOT_SUPPORTED";
	case CL_BUILD_PROGRAM_FAILURE:                     return "CL_BUILD_PROGRAM_FAILURE";
	case CL_MAP_FAILURE:                               return "CL_MAP_FAILURE";
	case CL_MISALIGNED_SUB_BUFFER_OFFSET:              return "CL_MISALIGNED_SUB_BUFFER_OFFSET";
	case CL_EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST: return "CL_EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST";
	/*
	 * case CL_COMPILE_PROGRAM_FAILURE:                return "CL_COMPILE_PROGRAM_FAILURE";
	 * case CL_LINKER_NOT_AVAILABLE:                   return "CL_LINKER_NOT_AVAILABLE";
	 * case CL_LINK_PROGRAM_FAILURE:                   return "CL_LINK_PROGRAM_FAILURE";
	 * case CL_DEVICE_PARTITION_FAILED:                return "CL_DEVICE_PARTITION_FAILED";
	 * case CL_KERNEL_ARG_INFO_NOT_AVAILABLE:          return "CL_KERNEL_ARG_INFO_NOT_AVAILABLE";
	 */
	case CL_INVALID_VALUE:                             return "CL_INVALID_VALUE";
	case CL_INVALID_DEVICE_TYPE:                       return "CL_INVALID_DEVICE_TYPE";
	case CL_INVALID_PLATFORM:                          return "CL_INVALID_PLATFORM";
	case CL_INVALID_DEVICE:                            return "CL_INVALID_DEVICE";
	case CL_INVALID_CONTEXT:                           return "CL_INVALID_CONTEXT";
	case CL_INVALID_QUEUE_PROPERTIES:                  return "CL_INVALID_QUEUE_PROPERTIES";
	case CL_INVALID_COMMAND_QUEUE:                     return "CL_INVALID_COMMAND_QUEUE";
	case CL_INVALID_HOST_PTR:                          return "CL_INVALID_HOST_PTR";
	case CL_INVALID_MEM_OBJECT:                        return "CL_INVALID_MEM_OBJECT";
	case CL_INVALID_IMAGE_FORMAT_DESCRIPTOR:           return "CL_INVALID_IMAGE_FORMAT_DESCRIPTOR";
	case CL_INVALID_IMAGE_SIZE:                        return "CL_INVALID_IMAGE_SIZE";
	case CL_INVALID_SAMPLER:                           return "CL_INVALID_SAMPLER";
	case CL_INVALID_BINARY:                            return "CL_INVALID_BINARY";
	case CL_INVALID_BUILD_OPTIONS:                     return "CL_INVALID_BUILD_OPTIONS";
	case CL_INVALID_PROGRAM:                           return "CL_INVALID_PROGRAM";
	case CL_INVALID_PROGRAM_EXECUTABLE:                return "CL_INVALID_PROGRAM_EXECUTABLE";
	case CL_INVALID_KERNEL_NAME:                       return "CL_INVALID_KERNEL_NAME";
	case CL_INVALID_KERNEL_DEFINITION:                 return "CL_INVALID_KERNEL_DEFINITION";
	case CL_INVALID_KERNEL:                            return "CL_INVALID_KERNEL";
	case CL_INVALID_ARG_INDEX:                         return "CL_INVALID_ARG_INDEX";
	case CL_INVALID_ARG_VALUE:                         return "CL_INVALID_ARG_VALUE";
	case CL_INVALID_ARG_SIZE:                          return "CL_INVALID_ARG_SIZE";
	case CL_INVALID_KERNEL_ARGS:                       return "CL_INVALID_KERNEL_ARGS";
	case CL_INVALID_WORK_DIMENSION:                    return "CL_INVALID_WORK_DIMENSION";
	case CL_INVALID_WORK_GROUP_SIZE:                   return "CL_INVALID_WORK_GROUP_SIZE";
	case CL_INVALID_WORK_ITEM_SIZE:                    return "CL_INVALID_WORK_ITEM_SIZE";
	case CL_INVALID_GLOBAL_OFFSET:                     return "CL_INVALID_GLOBAL_OFFSET";
	case CL_INVALID_EVENT_WAIT_LIST:                   return "CL_INVALID_EVENT_WAIT_LIST";
	case CL_INVALID_EVENT:                             return "CL_INVALID_EVENT";
	case CL_INVALID_OPERATION:                         return "CL_INVALID_OPERATION";
	case CL_INVALID_GL_OBJECT:                         return "CL_INVALID_GL_OBJECT";
	case CL_INVALID_BUFFER_SIZE:                       return "CL_INVALID_BUFFER_SIZE";
	case CL_INVALID_MIP_LEVEL:                         return "CL_INVALID_MIP_LEVEL";
	case CL_INVALID_GLOBAL_WORK_SIZE:                  return "CL_INVALID_GLOBAL_WORK_SIZE";
	case CL_INVALID_PROPERTY:                          return "CL_INVALID_PROPERTY";
	/*
	 * case CL_INVALID_IMAGE_DESCRIPTOR:               return "CL_INVALID_IMAGE_DESCRIPTOR";
	 * case CL_INVALID_COMPILER_OPTIONS:               return "CL_INVALID_COMPILER_OPTIONS";
	 * case CL_INVALID_LINKER_OPTIONS:                 return "CL_INVALID_LINKER_OPTIONS";
	 * case CL_INVALID_DEVICE_PARTITION_COUNT:         return "CL_INVALID_DEVICE_PARTITION_COUNT";
	 */
	default:
		return "unknown error";
	}
}

const char *getCommandType (cl_command_type type) {

	switch (type) {
	case CL_COMMAND_NDRANGE_KERNEL:       return "CL_COMMAND_NDRANGE_KERNEL";
	case CL_COMMAND_TASK:                 return "CL_COMMAND_TASK";
	case CL_COMMAND_NATIVE_KERNEL:        return "CL_COMMAND_NATIVE_KERNEL";
	case CL_COMMAND_READ_BUFFER:          return "CL_COMMAND_READ_BUFFER";
	case CL_COMMAND_WRITE_BUFFER:         return "CL_COMMAND_WRITE_BUFFER";
	case CL_COMMAND_COPY_BUFFER:          return "CL_COMMAND_COPY_BUFFER";
	case CL_COMMAND_READ_IMAGE:           return "CL_COMMAND_READ_IMAGE";
	case CL_COMMAND_WRITE_IMAGE:          return "CL_COMMAND_WRITE_IMAGE";
	case CL_COMMAND_COPY_IMAGE:           return "CL_COMMAND_COPY_IMAGE";
	case CL_COMMAND_COPY_BUFFER_TO_IMAGE: return "CL_COMMAND_COPY_BUFFER_TO_IMAGE";
	case CL_COMMAND_COPY_IMAGE_TO_BUFFER: return "CL_COMMAND_COPY_IMAGE_TO_BUFFER";
	case CL_COMMAND_MAP_BUFFER:           return "CL_COMMAND_MAP_BUFFER";
	case CL_COMMAND_MAP_IMAGE:            return "CL_COMMAND_MAP_IMAGE";
	case CL_COMMAND_UNMAP_MEM_OBJECT:     return "CL_COMMAND_UNMAP_MEM_OBJECT";
	case CL_COMMAND_MARKER:               return "CL_COMMAND_MARKER";
	case CL_COMMAND_ACQUIRE_GL_OBJECTS:   return "CL_COMMAND_ACQUIRE_GL_OBJECTS";
	case CL_COMMAND_RELEASE_GL_OBJECTS:   return "CL_COMMAND_RELEASE_GL_OBJECTS";
	default:
		return "unknown command type";
	}
}

const char *getCommandExecutionStatus (int status) {

	switch (status) {
	case CL_QUEUED:    return "CL_QUEUED";
	case CL_SUBMITTED: return "CL_SUBMITTED";
	case CL_RUNNING:   return "CL_RUNNING";
	case CL_COMPLETE:  return "CL_COMPLETE";
	default:
		return "unknown command status";
	}
}


