#!/bin/sh
#
# usage: ./genmakefile.sh
#
# Shell script that generates the Saber C library Makefile
# for the present system
#
# If a Makefile exists, it is renamed to Makefile.save

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
	echo "error: \$SABER_HOME is not set"
	exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

MAKEFILE="$SABER_HOME"/clib/Makefile

# Find OS
OS=`uname -s`
if [ \( "$OS" != "Darwin" \) -a \( "$OS" != "Linux" \) ]; then
	echo "error: unsupported operating system: $OS"
	exit 1
fi

JH="$JAVA_HOME"
[ -z "$JH" ] && { # If JAVA_HOME is not set, try to find it
[ "$OS" = "Darwin" ] && {
	
	saberProgramExistsOrExit "/usr/libexec/java_home"
	JH=`/usr/libexec/java_home`
	
} || { # OS is Linux
	
	saberProgramExistsOrExit "readlink"
	JH=`readlink -f $(which java) | awk '{ split($0, t, "/jre/"); print t[1] }'`
}
}

# Check Java home
saberDirectoryExistsOrExit "$JH"
# Check if jni.h exists
# saberFileExistsOrExit "$JH/include/jni.h"

CH="$CUDA_HOME"
# If CUDA_HOME is not set, try to find it
[ -z "$CH" ] && [ -d "/usr/local/cuda" ] && CH="/usr/local/cuda"

#
# With all the necessary variables in place,
# create Makefile
#
[ -e "$MAKEFILE" ] && mv "$MAKEFILE" "$MAKEFILE".save

touch "$MAKEFILE"

echo "# Makefile for Saber C library" >>"$MAKEFILE"
echo "# Customised for `uname -n` running `uname -s` on `date`" >>"$MAKEFILE"
echo "#" >>"$MAKEFILE"

# Set operating system
echo "OS = $OS" >>"$MAKEFILE"

# Set Java home
echo "JAVA_HOME = $JH" >>"$MAKEFILE"
echo "CUDA_HOME = $CH" >>"$MAKEFILE"

cat <<!endoftemplate! >>"$MAKEFILE"

CLASS_PATH = ../target/classes
vpath %.class \$(CLASS_PATH)

CC = gcc

CFL_BASE = -W -Wall -DWARNING -fpic

CFL_COMMON = \$(CFL_BASE) -g

CFLAGS = \$(CFL_COMMON)

ifeq (,\$(filter \$(OS),Linux Darwin))
   \$(error error: unsupported operating system: \$(OS))
endif

ifndef SABER_HOME
   \$(error error: variable SABER_HOME not set)
endif

ifeq (\$(OS), Darwin)
	CFLAGS += -I\$(JAVA_HOME)/include
	CFLAGS += -I\$(JAVA_HOME)/include/darwin
else
	CFLAGS += -I\$(JAVA_HOME)/include
	CFLAGS += -I\$(JAVA_HOME)/linux
endif

CFLAGS += -I/usr/include -D_GNU_SOURCE

# Set path to CUDA_HOME/include on Linux only
GFLAGS =
ifneq (\$(OS), Darwin)
ifneq (\$(CUDA_HOME),)
	GFLAGS += -I\$(CUDA_HOME)/include
endif
endif

CLIBS =
ifeq (\$(OS), Linux) # On Darwin, pthread comes for free
	CLIBS += -pthread
endif

# Set OpenCL library
GLIBS =
ifeq (\$(OS), Darwin)
	GLIBS += -framework opencl 
else
	GLIBS += -lOpenCL
endif

SABERLIBS = -L\$(SABER_HOME)/clib 
SABERLIBS += -lCPU
SABERLIBS += -lGPU

OBJS = GPU.o timer.o openclerrorcode.o resulthandler.o inputbuffer.o outputbuffer.o gpucontext.o gpuquery.o

all: libCPU.so libGPU.so
gpu: libGPU.so
cpu: libCPU.so 

libGPU.so: \$(OBJS)
	\$(CC) -shared -o libGPU.so \$(OBJS) \$(GLIBS) \$(CLIBS)

libCPU.so: CPU.o
	\$(CC) -shared -o libCPU.so CPU.o

GPU.o: GPU.c GPU.h uk_ac_imperial_lsds_saber_devices_TheGPU.h timer.h openclerrorcode.h 
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

CPU.o: CPU.c uk_ac_imperial_lsds_saber_devices_TheCPU.h
	\$(CC) \$(CFLAGS) -c $< -o \$@

#
# JNI class headers
#
uk_ac_imperial_lsds_saber_devices_TheGPU.h:
	javah -classpath \$(CLASS_PATH) uk.ac.imperial.lsds.saber.devices.TheGPU

uk_ac_imperial_lsds_saber_devices_TheCPU.h:
	javah -classpath \$(CLASS_PATH) uk.ac.imperial.lsds.saber.devices.TheCPU

#
# Objects
#
timer.o: timer.c timer.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

openclerrorcode.o: openclerrorcode.c openclerrorcode.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

resulthandler.o: resulthandler.c resulthandler.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

inputbuffer.o: inputbuffer.c inputbuffer.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

outputbuffer.o: outputbuffer.c outputbuffer.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

gpucontext.o: gpucontext.c gpucontext.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

gpuquery.o: gpuquery.c gpuquery.h
	\$(CC) \$(CFLAGS) \$(GFLAGS) -c $< -o \$@

clean:
	rm -f *.o *.so

!endoftemplate!

exit 0
