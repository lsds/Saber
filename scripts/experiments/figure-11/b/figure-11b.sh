#!/bin/bash

# Figure 11b: Performance impact of window slide

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
F="$SABER_HOME/scripts/experiments/figure-11/b"
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

FIGID="11b"

DURATION=

saberExperimentSetup () {
	# Set options
	#
	# Window settings
	OPTS="$OPTS --window-type row --window-size 1024"
	
	# Query settings
	OPTS="$OPTS --aggregate-expression avg --groups-per-window 0"
	
	# System settings
	OPTS="$OPTS --enable-latency-measurements true --number-of-partial-windows 65536 --intermediate-buffer-size 16777216"
	
	# Set the duration of each experiment in the figure
	[ -z "$DURATION" ] && DURATION=$SABER_DEFAULT_DURATION
	
	OPTS="$OPTS --experiment-duration $DURATION"
	
	CPU_OPTS="$CPU_OPTS --number-of-worker-threads 15"
	GPU_OPTS="$GPU_OPTS --number-of-worker-threads  1 --pipeline-depth 4"
	HBR_OPTS="$CPU_OPTS --number-of-worker-threads 15 --pipeline-depth 4"
	
	return 0
}

saberExperimentRun () {
	#
	# for N in 2 4 8 16 32 64 128 256 512 1024 tuples/slide (i.e. from 64B to 32KB)
	# do 
	# 	Run Aggregation CPU-only with $N tuples/slide (N-1)
	# 	Run Aggregation GPU-only with $N tuples/slide (N-2)
	# 	Run Aggregation Hybrid   with $N tuples/slide (N-3) 
	# done
	#
	errors=0
	points=0
	
	for mode in "cpu" "gpu" "hybrid"; do
		
		line=
		key=
		
		modeoptions=
		
		case "$mode" in
		"cpu")
		line="'Saber (CPU-only) throughput'"
		key=1
		modeoptions="$CPU_OPTS"
		;;
		"gpu")
		line="'Saber (GPU-only) throughput'"
		modeoptions="$GPU_OPTS"
		key=2
		;;
		*)
		line="'Saber throughput'"
		key=3
		modeoptions="$HBR_OPTS"
		;;
		esac
		
		for N in 2 4 8 16 32 64 128 256 512 1024; do
		
		let points++
		
		# Convert to bytes
		SLIDESIZE=`echo "$N * 32" | bc`
		
		printf "Figure %3s: line %-30s: x %5d\n" "$FIGID" "$line" "$SLIDESIZE"
		
		"$SABER_HOME"/scripts/experiments/microbenchmarks/aggregation.sh $OPTS $modeoptions \
		--window-slide $N --execution-mode $mode
		
		errorcode=$?
		
		saberExperimentStoreResults "aggregation" "$RESULTDIR" "$N-$key"
		
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
	points=0
	errors=0
	
	for mode in "cpu" "gpu" "hybrid"; do
		
		DAT1="$RESULTDIR/${mode}-throughput-avg.dat"
		DAT2="$RESULTDIR/${mode}-latency-avg.dat"
		
		key=
		
		case "$mode" in
		"cpu")
		key=1;;
		"gpu")
		key=2;;
		*)
		key=3;;
		esac
		
		for N in 2 4 8 16 32 64 128 256 512 1024; do
		
		let points++
		
		# Convert to bytes
		SLIDESIZE=`echo "$N * 32" | bc`
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && let errors++ && continue
		
		L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t 2>&1)
		
		# Append line if command was successful
		[ $? -eq 0 ] && echo $SLIDESIZE $L >>"$DAT1" || let errors++
		
		# Also process latency measurements
		if [ $mode = "hybrid" ]; then
		
			# Icrement points to account for latency line
			let points++
		
			L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -l 2>&1)
		
			# Append line if command was successful
			[ $? -eq 0 ] && echo $SLIDESIZE $L >>"$DAT2" || let errors++
		
		fi
		
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
	
	DATDIR="$ORIGIN/figure-11/aggregation"
	#
	# Data format:
	#
	#     x    MIN         AVG              ...
	# _________________________________________
	#    64    4397.206    4578.12468333    ...
	#   128    5915.170    6013.03573333    ...
	#   256    6015.984    6101.14541667    ...
	#   ...
	# 32768    5688.312    6082.13458333    ...
	#
	# where x is the window slide (in bytes) and columns are:
	# 
	# [1]  min
	# [2]  avg
	# [3]  max
	# [4]  5th
	# [5] 25th
	# [6] 50th
	# [7] 75th
	# [8] 99th
	# [9]  std
	#
	printf "Figure %3s: Reproducibility Report:\n" "$FIGID"
	
	for mode in "cpu" "gpu" "hybrid"; do
		
		DAT="$DATDIR/${mode}-throughput-avg.dat"
		
		line=
		key=
		
		case "$mode" in
		"cpu")
		line="'Saber (CPU-only) throughput'"
		key=1
		;;
		"gpu")
		line="'Saber (GPU-only) throughput'"
		key=2
		;;
		*)
		line="'Saber throughput'"
		key=3
		;;
		esac
		
		for N in 2 4 8 16 32 64 128 256 512 1024; do
		
		# Convert to bytes
		SLIDESIZE=`echo "$N * 32" | bc`
		
		printf "Figure %3s: line %-30s: x %5d: " "$FIGID" "$line" "$SLIDESIZE"
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
		# Extract average throughput from log file
		
		x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 2 2>&1)
		[ $? -ne 0 ] && echo "error 2" && continue
		
		# Compare with existing value in dat file
		d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$SLIDESIZE" -o 2 2>&1)
		[ $? -ne 0 ] && echo "error 3" && continue
		
		printf "y-value (%8.2f) deviates from original value by %+8.2f%%\n" "$x" "$d"
		
		done
	done
	
	# Repeat inner loop for hybrid latency measurements
	for mode in "hybrid"; do
		
		DAT="$DATDIR/${mode}-latency-avg.dat"
		
		line="'Saber latency'"
		
		for N in 2 4 8 16 32 64 128 256 512 1024; do
		
		# Convert to bytes
		SLIDESIZE=`echo "$N * 32" | bc`
		
		printf "Figure %3s: line %-30s: x %5d: " "$FIGID" "$line" "$SLIDESIZE"
		
		LOG="$RESULTDIR/${N}-3.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
		# Extract average latency from log file
		
		x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -l -x 2 2>&1)
		[ $? -ne 0 ] && echo "error 2" && continue
		
		# Compare with existing value in dat file
		d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$SLIDESIZE" -o 2 2>&1)
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

CPU_OPTS=
GPU_OPTS=
HBR_OPTS=

printf "Figure %3s: reproducibility version %d.%d\n" "$FIGID" $major $minor

# Set OPTS, common across all experiments for this figure
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
			P="$DATDIR/figure-11/aggregation"
			
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

