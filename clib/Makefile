CLASS_PATH = ../target/classes
vpath %.class $(CLASS_PATH)

CC = gcc

CFL_BASE = -W -Wall -DWARNING -fpic

CFL_COMMON = $(CFL_BASE) -g

CFLAGS = $(CFL_COMMON)

OS := $(shell uname -s)
ifeq (,$(filter $(OS),Linux Darwin))
	$(error error: unsupported OS $(OS))
endif

ifeq ($(JAVA_HOME),)
	ifeq ($(OS), Darwin)
		JAVA_HOME = /Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home
	else
		JAVA_HOME = /usr/lib/jvm/java-1.7.0-openjdk-amd64
	endif
endif

ifeq ($(OS), Darwin)
	CFLAGS += -I$(JAVA_HOME)/include
	CFLAGS += -I$(JAVA_HOME)/include/darwin
else
	CFLAGS += -I$(JAVA_HOME)/include
	CFLAGS += -I$(JAVA_HOME)/linux
endif

CFLAGS += -I/usr/include -D_GNU_SOURCE

LIBS = 
# OpenCL
ifeq ($(OS), Darwin)
	LIBS += -framework opencl # pthread comes for free
else
	CFLAGS += -I/usr/local/cuda-6.5/include
	LIBS += -lOpenCL -pthread
endif

CLIB = -L$(SABER_HOME)/clib -lGPU

OBJS = GPU.o timer.o openclerrorcode.o resulthandler.o inputbuffer.o outputbuffer.o gpucontext.o gpuquery.o

all: libGPU.so libCPU.so
cpu: libCPU.so 

libGPU.so: $(OBJS)
	$(CC) -shared -o libGPU.so $(OBJS) $(LIBS)

libCPU.so: CPU.o
	$(CC) -shared -o libCPU.so CPU.o

GPU.o: GPU.c GPU.h uk_ac_imperial_lsds_saber_devices_TheGPU.h timer.h openclerrorcode.h 
	$(CC) $(CFLAGS) -c $< -o $@

CPU.o: CPU.c uk_ac_imperial_lsds_saber_devices_TheCPU.h
	$(CC) $(CFLAGS) -c $< -o $@

uk_ac_imperial_lsds_saber_devices_TheGPU.h:
	javah -classpath $(CLASS_PATH) uk.ac.imperial.lsds.saber.devices.TheGPU

uk_ac_imperial_lsds_saber_devices_TheCPU.h:
	javah -classpath $(CLASS_PATH) uk.ac.imperial.lsds.saber.devices.TheCPU

timer.o: timer.c timer.h
openclerrorcode.o: openclerrorcode.c openclerrorcode.h
resulthandler.o: resulthandler.c resulthandler.h
inputbuffer.o: inputbuffer.c inputbuffer.h
outputbuffer.o: outputbuffer.c outputbuffer.h
gpucontext.o: gpucontext.c gpucontext.h
gpuquery.o: gpuquery.c gpuquery.h

clean:
	rm -f *.o *.so
