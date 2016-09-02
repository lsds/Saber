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

USAGE="usage: w1.sh"

CLS="uk.ac.imperial.lsds.saber.experiments.microbenchmarks.scheduling.W1"

#
# Application-specific arguments
#

# Parse application-specific arguments

while :
do
	case "$1" in
		-h | --help)
		echo $USAGE
		exit 0
		;;
		--*) 
		# Check if $1 is a system configuration argument	
		saberParseSysConfArg "$1" "$2"
		errorcode=$?
		if [ $errorcode -eq 0 ]; then
			shift 2
		elif [ $errorcode -eq 1 ]; then
			# $1 was a valid system configuration argument
			# but with a wrong value
			exit 1
		else
			echo "error: invalid option: $1" >&2
			exit 1
		fi
		;;
		*) # done, if string is empty
		if [ -n "$1" ]; then
			echo "error: invalid option: $1"
			exit 1
		fi
		break
		;;
	esac
done

#
# Configure app args
#
APPARGS=""

#
# Configure system args
#
SYSARGS=""

saberSetSysArgs

#
# Execute application
errorcode=0
#
# if --experiment-duration is set, then we should run the experiment
# in the background.
#
if [ -z "$SABER_CONF_EXPERIMENTDURATION" ]; then
	
	"$SABER_HOME"/scripts/run.sh \
	--mode foreground \
	--class $CLS \
	-- $SYSARGS $APPARGS
else
	#
	# Find the experiments duration is seconds
	#
	interval="$SABER_CONF_PERFORMANCEMONITORINTERVAL"
	[ -z "$interval" ] && interval="1000" # in msec; the default interval
	
	# Duration is `interval` units
	units="$SABER_CONF_EXPERIMENTDURATION"
	
	# Duration in msecs
	msecs=`echo "$units * $interval" | bc`
	
	# Duration is seconds
	duration=`echo "scale=0; ${msecs} / 1000" | bc`
	
	# Round up
	remainder=`echo "${msecs} % 1000" | bc`
	if [ $remainder -gt 0 ]; then
		let duration++
	fi
	
	"$SABER_HOME"/scripts/run.sh --mode background --alias "one" --class $CLS --duration $duration -- $SYSARGS $APPARGS
	errorcode=$?
fi

exit $errorcode

