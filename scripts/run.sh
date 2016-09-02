#!/bin/bash

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
	echo "error: \$SABER_HOME is not set"
	exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

USAGE="usage: run.sh [ --alias ] [ --duration ] [ --mode {foreground, background} ] [ --class ] [ -- <class arguments> ]"

# Java classpath
JCP=

saberSetClasspath () {
	
	JCP="."
	
	# Check Maven repository
	
	saberDirectoryExistsOrExit "${MVN_REPOSITORY}"
	
	# Check Maven packages
	
	JETTY="${MVN_REPOSITORY}/org/eclipse/jetty"
	#
	saberDirectoryExistsOrExit "${JETTY}"
	
	for pkg in jetty-server jetty-util jetty-http jetty-io; do
		
		JAR=`saberFindJar "${JETTY}/${pkg}"`
		if [ -z "$JAR" ]; then
			echo "error: no jar found in ${JETTY}/${pkg}"
			exit 1
		fi
		JCP="$JCP:$JAR"
	done
	
	JACKSON="${MVN_REPOSITORY}/com/fasterxml/jackson/core"
	#
	saberDirectoryExistsOrExit "${JACKSON}"
	
	for pkg in jackson-core jackson-databind jackson-annotations; do
		
		JAR=`saberFindJar "${JACKSON}/${pkg}"`
		if [ -z "$JAR" ]; then
			echo "error: no jar found in ${JACKSON}/${pkg}"
			exit 1
		fi
		JCP="$JCP:$JAR"
	done
	
	LOG4J="${MVN_REPOSITORY}/org/apache/logging/log4j"
	#
	saberDirectoryExistsOrExit "${LOG4J}"
	
	for pkg in log4j-core log4j-api; do
		
		JAR=`saberFindJar "${LOG4J}/${pkg}"`
		if [ -z "$JAR" ]; then
			echo "error: no jar found in ${LOG4J}/${pkg}"
			exit 1
		fi
		JCP="$JCP:$JAR"
	done
	
	JAVAX="${MVN_REPOSITORY}/javax/servlet/javax.servlet-api"
	#
	saberDirectoryExistsOrExit "${JAVAX}"
	JAR=`saberFindJar "${JAVAX}"`
	if [ -z "$JAR" ]; then
		echo "error: no jar found in ${JAVAX}"
		exit 1
	fi
	JCP="$JCP:$JAR"
	
	# Saber test classes
	
	TST="$SABER_HOME/target/test-classes"
	#
	saberDirectoryExistsOrExit "$TST"
	JCP="$JCP:$TST"
	
	# Saber system
	
	SYS="$SABER_HOME/lib/saber-0.0.1-SNAPSHOT.jar"
	#
	saberFileExistsOrExit "$SYS"
	JCP="$JCP:$SYS"
}

#
# Main
#
# Command-line arguments

MODE="foreground"
# When mode is `background`, alias and duration must be set
ALIAS=
DURATION=
# The Saber application
CLS=
# The Saber application arguments
ARGS=

# Parse command-line arguments

while :
do
	case "$1" in
		-m | --mode)
		saberOptionInSet "$1" "$2" "foreground" "background" || exit 1
		MODE="$2"
		# if [ \( "$MODE" != "foreground" \) -a \( "$MODE" != "background" \) ]; then
		#	echo "error: invalid mode: $MODE"
		#	exit 1
		# fi 
		shift 2
		;;
		-a | --alias)
		saberOptionIsAlpha "$1" "$2" || exit 1
		ALIAS="$2"
		shift 2
		;;
		-d | --duration)
		saberOptionIsPositiveInteger "$1" "$2" || exit 1
		DURATION="$2"
		shift 2
		;;
		-c | --class)
		CLS="$2"
		# Check class exists
		saberFileExistsOrExit "$SABER_HOME/target/test-classes/`echo ${CLS} | tr '.' '/'`.class"
		shift 2
		;;
		-h | --help)
		echo $USAGE
		exit 0
		;;
		--) # End of all options
		shift
		break
		;;
		-*)
		echo "error: invalid option: $1" >&2
		exit 1
		;;
		*) # done, if string is empty
		if [ -n "$1" ]; then
			echo "error: invalid argument: $1"
			exit 1
		fi
		break
		;;
	esac
done

# The remaining arguments are class arguments
ARGS="$@"

# Check that CLS is set (if set, it is correct)
[ -z "$CLS" ] && {
	echo "error: no class specified"
	exit 1
}

# Set Java classpath variable, $JCP
saberSetClasspath

#
# JVM options
#
OPTS="-server -XX:+UseConcMarkSweepGC"

OPTS="$OPTS -XX:NewRatio=${SABER_JVM_NEWRATIO}"
OPTS="$OPTS -XX:SurvivorRatio=${SABER_JVM_SURVIVORRATIO}"

OPTS="$OPTS -Xms${SABER_JVM_MS}g"
OPTS="$OPTS -Xmx${SABER_JVM_MX}g"

if [ "$SABER_JVM_LOGGC" = "true" ]; then
	# Log garbage collection events
	OPTS="$OPTS -Xloggc:gc.out"
fi

$SABER_RUN_LOG && saberLogRunCommand "java $OPTS -cp $JCP $CLS $ARGS"

errorcode=0

if [ "$MODE" = "foreground" ]; then
	#
	java $OPTS -cp $JCP $CLS $ARGS
else
	# Running application in the background
	#
	# Check that duration is set
	[ -z "$DURATION" ] && {
		echo "error: experiment duration is not set"
		exit 1
	}
	# Check that class alias is set
	[ -z "$ALIAS" ] && {
		echo "error: class alias is not set"
		exit 1
	}
	#
	CMD="java $OPTS -cp $JCP $CLS $ARGS"
	# Try trap signals
	saberSignalTrapped || saberProcessTrap
	saberProcessStart $ALIAS $CMD
	#
	$SABER_VERBOSE && echo "Running application \"$ALIAS\" for $DURATION seconds..."
	countdown=$DURATION
	length=${#DURATION} # Length to control printf length
	interrupted=0
	
	while [ $countdown -ne 0 ]; do
		printf "\rExperiment will stop in %${length}ds (press any key to exit) " $countdown
		read -n 1 -s -t 1
		key=$?
		if [ $key -eq 0 ]; then
			interrupted=1
			# Set error code
			errorcode=1
			break
		fi
		saberProcessIsRunning $ALIAS $CMD || {
			echo "" # Line break
			echo "error: application \"$ALIAS\" has failed (check "$SABER_LOGDIR"/$ALIAS.err for errors)"
			# Set error code
			errorcode=1
			break
		}
		let countdown--
	done
	if [ $interrupted -ne 0 ]; then
		echo "Interrupted"
	fi
	# 
	# Check until Saber measurements are flushed to output file
	# only if we have exited gracefully
	if [ $errorcode -eq 0 ]; then
		echo ""
		saberProcessDone $ALIAS
		if [ $? -ne 0 ]; then
			echo "warning: failed to detect whether measurements have been flushed"
			errorcode=1
		fi
	fi
	# Stop the process (and clean-up, even if there has been an error)
	saberProcessStop $ALIAS $CMD
	# echo "Done"
fi

exit $errorcode

