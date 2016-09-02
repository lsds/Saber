#!/bin/sh

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
    echo "error: \$SABER_HOME is not set"
    exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

[ ! -d "$SABER_LIBDIR" ] && mkdir "$SABER_LIBDIR"

# Goto home directory, where pom.xml exists
cd $SABER_HOME

TRANSCRIPT="build.log"

[ -f $TRANSCRIPT ] && rm -f $TRANSCRIPT

saberProgramExistsOrExit "mvn"

mvn package -q -e -X >$TRANSCRIPT 2>&1
[ $? -ne 0 ] && {
	echo "error: Saber Java library compilation failed (transcript written on $SABER_HOME/$TRANSCRIPT)"
	exit 1
} 

# Saber was successfully build
echo "Saber Java library build successful (transcript written on $SABER_HOME/$TRANSCRIPT)"

JAR="$SABER_HOME/target/saber-0.0.1-SNAPSHOT.jar"

# Unlikely
saberFileExistsOrExit "$JAR"

[ -f "$JAR" ] && cp "$JAR" "$SABER_LIBDIR"

LIBFILE="$SABER_LIBDIR/saber-0.0.1-SNAPSHOT.jar"

# Unlikely
saberFileExistsOrExit "$LIBFILE"

jarsize=`wc -c < "$LIBFILE" | sed -e 's/^[ \t]*//'`
echo "Output written on $LIBFILE ($jarsize bytes)"

#
# Building Saber C libraries
#
"$SABER_HOME"/clib/genmakefile.sh
# Unlikely
[ $? -ne 0 ] && {
echo "error: failed to generate C library Makefile"
exit 1
}

saberProgramExistsOrExit "make"
saberProgramExistsOrExit "gcc"

# Goto clib/ directory
cd "$SABER_HOME"/clib/

# Log output to clib/build.log
[ -f $TRANSCRIPT ] && rm -f $TRANSCRIPT

# Clean-up
make clean >>$TRANSCRIPT 2>&1

# Make libCPU.so
make cpu >>$TRANSCRIPT 2>&1
[ $? -ne 0 ] && {
echo "error: Saber CPU library compilation failed (transcript written on $SABER_HOME/clib/$TRANSCRIPT)"
exit 1
}

CPULIB="$SABER_HOME/clib/libCPU.so"

# Unlikely
saberFileExistsOrExit "$CPULIB"

cpulibsize=`wc -c < "$CPULIB" | sed -e 's/^[ \t]*//'`

echo "Saber CPU library build successful (transcript written on $SABER_HOME/clib/$TRANSCRIPT)"
echo "Output written on $CPULIB ($cpulibsize bytes)"

# Make libGPU.so
make gpu >>$TRANSCRIPT 2>&1
[ $? -ne 0 ] && {
echo "error: Saber GPU library compilation failed (transcript written on $SABER_HOME/clib/$TRANSCRIPT)"
exit 1
}

GPULIB="$SABER_HOME/clib/libGPU.so"

# Unlikely
saberFileExistsOrExit "$GPULIB"

gpulibsize=`wc -c < "$GPULIB" | sed -e 's/^[ \t]*//'`

echo "Saber GPU library build successful (transcript written on $SABER_HOME/clib/$TRANSCRIPT)"
echo "Output written on $GPULIB ($gpulibsize bytes)"

exit 0
