#!/bin/bash

# Figure 15: Performance impact of HLS scheduling

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
	echo "error: \$SABER_HOME is not set"
	exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

EXECPATH="$( dirname "${BASH_SOURCE[0]}" )"

#
# Create result directory
#
F="$SABER_HOME/scripts/experiments/figure-15"
major=
minor=
if [ ! -z $SABER_MAJOR_VERSION ]; then # Major version is set
	major=$SABER_MAJOR_VERSION
	minor=0
else
	major=`saberCurrentFigureVersion $F 0`
	minor=`saberCurrentFigureVersion $F 1`
	let minor++
fi
RESULTDIR="$F/results/$major.$minor"
mkdir -p "$RESULTDIR"

echo "### DB Reproducibility Results Ver. $major.$minor  "    >>"$RESULTDIR"/README.md
echo "Customized for `uname -n` running `uname -s` on `date`" >>"$RESULTDIR"/README.md
#

FIGID="15"

DURATION=

saberExperimentSetup () {
	# Set options
	#
	# Window settings
	
	# System settings
	OPTS="$OPTS --execution-mode hybrid --number-of-worker-threads 15"
	OPTS="$OPTS --circular-buffer-size 268435456" # --intermediate-buffer-size 32768" # --intermediate-buffer-size 1048576"
	OPTS="$OPTS --intermediate-buffer-size 1048576" # --intermediate-buffer-size 1048576"
	
	# Set the duration of each experiment in the figure
	[ -z "$DURATION" ] && DURATION=$SABER_DEFAULT_DURATION
	
	OPTS="$OPTS --experiment-duration $DURATION"
	
	# Workload-specific settings
	
	W2OPTS="$W2OPTS --number-of-partial-windows 128"
	W2OPTS="$W2OPTS --switch-threshold 10 --throughput-monitor-interval 100"
	
	return 0
}

saberExperimentRun () {
	# 
	# Run W(i) using FCFS   (i-1)
	# Run W(i) using Static (i-2)
	# Run W(i) using HLS    (i-3)
	#
	errors=0
	points=0
	
	for N in 2; do
		
		NAME="W${N}"
		[ $N -eq 1 ] && ALIAS="one" || ALIAS="two"
		
		for mode in "fcfs" "static" "hls"; do
		# for mode in "hls"; do
		
		line=
		key=
			
		case "$mode" in
		"fcfs")
		line="'FCFS throughput'"
		key=1
		;;
		"static")
		line="'Static throughput'"
		key=2
		;;
		*)
		line="'HLS throughput'"
		key=3
		;;
		esac
		
		let points++
		
		printf "Figure %3s: line %-30s: x %2s\n" "$FIGID" "$line" "$NAME"
		
		errorcode=0
		
		case "$N" in
		1)
		"$SABER_HOME"/scripts/experiments/microbenchmarks/scheduling/w1.sh $OPTS \
		--scheduling-policy $mode $W1OPTS
		
		errorcode=$?
		;;
		2)
		"$SABER_HOME"/scripts/experiments/microbenchmarks/scheduling/w2.sh $OPTS \
		--scheduling-policy $mode $W2OPTS
		
		errorcode=$?
		;;
		esac
		
		saberExperimentStoreResults $ALIAS "$RESULTDIR" "$N-$key"
		
		# Increment errors based on exit code
		[ $errorcode -ne 0 ] && let errors++ && continue
		
		done
	done
	
	d=$((points - errors))
	printf "Figure %3s: %2d/%2d experiments run successfully " "$FIGID" "$d" "$points"
	if [ $errors -gt 0 ]; then
		echo "(transcript written on $RESULTDIR/error.log)"
	else
		echo ""
	fi
	
	return $errors
}

saberExperimentClear () {
	return 0
}

saberExperimentParseResults () {
	# 
	# Generate new .dat file(s) from reproduced results
	#
	errors=0
	points=0
	
	for mode in "fcfs" "static" "hls"; do
		
		DAT="$RESULTDIR/${mode}.dat"
		
		key=
		case "$mode" in
		"fcfs")
		key=1;;
		"static")
		key=2;;
		*)
		key=3;;
		esac
		
		for N in 1 2; do
			
			
			[ $N -eq 1 ] && echo "$N 0 0" >>"$DAT" && continue
			
			LOG="$RESULTDIR/${N}-${key}.out"
			
			! saberExperimentLogIsValid "$LOG" && let errors++ && continue
			
			errorcode=0
			
			# Get average throughput
			V1=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 2 2>&1)
			[ $? -ne 0 ] && let errorcode++
			
			# Get stdev
			V2=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 9 2>&1)
			[ $? -ne 0 ] && let errorcode++
			
			[ $errorcode -eq 0 ] && echo "$N" "$V1" "$V2" >>"$DAT" || let errors++
			
		done 
	done
	
	d=$((points - errors))
	printf "Figure %3s: %2d/%2d plot points generated successfully\n" "$FIGID" "$d" "$points"
	
	return 0
}

saberExperimentCheckResults () {
	#
	# Results are stored in:
	#
	ORIGIN="$SABER_HOME/doc/paper/16-sigmod/plots/data/original"
	
	DATDIR="$ORIGIN/figure-15"
	#
	# Data format:
	#
	# x    AVG           STD
	# ________________________________
	# 1     993.68625    89.9731693815
	# 2    2912.11147     6.6246148390
	#
	# In each file, x represents the workload and columns are the 
	# average throughput (given a policy) and the std. dev
	#
	printf "Figure %3s: Reproducibility Report:\n" "$FIGID"
	
	for N in 1 2; do
		
		NAME="W${N}"
		
		for mode in "fcfs" "static" "hls"; do
		
			DAT="$DATDIR/${mode}.dat"
			
			line=
			key=
			
			case "$mode" in
			"fcfs")
			line="'FCFS throughput'"
			key=1
			;;
			"static")
			line="'Static throughput'"
			key=2
			;;
			*)
			line="'HLS throughput'"
			key=3
			;;
			esac
			
			printf "Figure %3s: line %-30s: x %2s: " "$FIGID" "$line" "$NAME"
			
			[ $N -eq 1 ] && echo "not supported yet" && continue
			
			LOG="$RESULTDIR/${N}-${key}.out"
			
			# Is it a valid log file?
			! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
			# Extract average throughput from log file
		
			x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 2 2>&1)
			[ $? -ne 0 ] && echo "error 2" && continue
		
			# Compare with existing value in dat file
			d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$N" -o 1 2>&1)
			[ $? -ne 0 ] && echo "error 3" && continue
		
			printf "y-value (%8.2f) deviates from original value by %+8.2f%%\n" "$x" "$d"
			
		done
	done
	
	return 0
}

#
# Main
#
OPTS=
#
# Workload-specific options
W1_OPTS=
W2_OPTS=

printf "Figure %3s: reproducibility version %d.%d\n" "$FIGID" $major $minor

# Set OPTS, common across all experiments for this figure,
# as well as workload-specific ones
saberExperimentSetup

saberExperimentRun
errors=$?

# Noop
saberExperimentClear

saberExperimentCheckResults

# If no errors occurred, generate .dat file(s)
[ $errors -eq 0 ] && {
	
	printf "Figure %3s: reproducibility version %d.%d successful\n" "$FIGID" $major $minor 
	
	saberExperimentParseResults
	
	# Check for any errors
	[ $? -eq 0 ] && {
		
		if [ ! -z $SABER_MAJOR_VERSION ]; then # Was the major version set?
			
			# The new result directory
			DATDIR="$SABER_HOME/doc/paper/16-sigmod/plots/data/reproducibility-results/${major}.0"
			
			[ ! -d "$DATDIR" ] && mkdir -p "$DATDIR" >/dev/null
			# Unlikely
			saberDirectoryExistsOrExit "$DATDIR"
			
			# The relative path of the reproduced results for this figure
			P="$DATDIR/figure-15"
			
			mkdir -p "$P"
			# Unlikely
			saberDirectoryExistsOrExit "$P"
			
			# Copy the results
			cp "$RESULTDIR"/*.dat "$P"
			
			printf "Figure %3s: new plot files available at %s\n" "$FIGID" "$P"
			
		else
			# .dat files will remain in the result directory
			printf "Figure %3s: new plot files available at %s\n" "$FIGID" "$RESULTDIR"
		fi
		
	} || {
		# Failed to generate .dat files
		printf "Figure %3s: error: failed to generate new plot files\n" "$FIGID"
	}
	
} || {
	# error: failed to reproduce all results
	printf "Figure %3s: error: failed to reproduce all results\n" "$FIGID"
}

exit 0
