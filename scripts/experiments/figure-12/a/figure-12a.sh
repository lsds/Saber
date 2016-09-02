#!/bin/bash

# Figure 12a: Performance impact of query task size for different query types

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
F="$SABER_HOME/scripts/experiments/figure-12/a"
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

FIGID="12a"

DURATION=

saberExperimentSetup () {
	# Set options
	#
	# Window settings
	OPTS="$OPTS --window-type row --window-size 1024 --window-slide 1024"
	
	# Query settings
	OPTS="$OPTS --selectivity 0 --comparisons 10"
	
	# System settings
	OPTS="$OPTS --enable-latency-measurements true"
	
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
	# for N in 64 128 256 512 1024 2048 4096 KB/batch
	# do 
	# 	Run Selection CPU-only with task size $N (N-1)
	# 	Run Selection GPU-only with task size $N (N-2)
	# 	Run Selection Hybrid   with task size $N (N-3)
	# done
	#
	errors=0
	points=0
	
	for mode in "hybrid"; do # "cpu" "gpu" "hybrid"; do
		
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
		key=2
		modeoptions="$GPU_OPTS"
		;;
		*)
		line="'Saber throughput'"
		key=3
		modeoptions="$HBR_OPTS"
		;;
		esac
		
		CBS=4  # The initial value is  4 MB
		CBL=16 # The lower bound   is 16 MB
		
		for N in 64; do # 128 256 512 1024 2048 4096; do
		
		let points++
		
		let CBS*=2 # Multiply by two
		
		BUFFERSIZE=
		if [ $CBS -lt $CBL ]; then
		BUFFERSIZE=`echo "$CBL * 1048576" | bc`
		else
		BUFFERSIZE=`echo "$CBS * 1048576" | bc`
		fi
		if [ "$mode" = "hybrid" ]; then
		BUFFERSIZE=`echo "$BUFFERSIZE * 4" | bc`
		fi
		
		# Convert to bytes
		TASKSIZE=`echo "$N * 1024" | bc`
		
		TPI=`echo "$TASKSIZE / 32" | bc`
		if [ $TPI -gt 32768 ]; then
		TPI=32768
		fi
		
		# Compute circular buffer size
		
		
		printf "Figure %3s: line %-30s: x %4d\n" "$FIGID" "$line" "$N"
		
		"$SABER_HOME"/scripts/experiments/microbenchmarks/selection.sh $OPTS $modeoptions \
		--batch-size $TASKSIZE --intermediate-buffer-size $TASKSIZE --circular-buffer-size $BUFFERSIZE --tuples-per-insert $TPI --execution-mode $mode
		
		errorcode=$?
		
		saberExperimentStoreResults "selection" "$RESULTDIR" "$N-$key"
		
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
		
		DAT1="$RESULTDIR/${mode}-throughput.dat"
		DAT2="$RESULTDIR/${mode}-latency.dat"
		
		key=
		
		case "$mode" in
		"cpu")
		key=1;;
		"gpu")
		key=2;;
		*)
		key=3;;
		esac
		
		for N in 64 128 256 512 1024 2048 4096; do
		
		let points++
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && let errors++ && continue
		
		L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t 2>&1)
		
		# Append line if command was successful
		[ $? -eq 0 ] && echo $N $L >>"$DAT1" || let errors++
		
		# Also process latency measurements
		if [ $mode = "hybrid" ]; then
		
			# Icrement points to account for latency line
			let points++
		
			L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -l 2>&1)
		
			# Append line if command was successful
			[ $? -eq 0 ] && echo $N $L >>"$DAT2" || let errors++
		
		fi
		
		done
	done
	
	d=$((points - errors))
	printf "Figure %3s: %2d/%2d plot points generated successfully\n" "$FIGID" "$d" "$points"
	
	return 0
}

saberExperimentCheckResults () {
	#
	# Results stored in:
	#
	ORIGIN="$SABER_HOME/doc/paper/16-sigmod/plots/data/original"
	
	DATDIR="$ORIGIN/figure-12/selection"
	#
	# Data format:
	#
	#    x    MIN         AVG              ...
	# ________________________________________
	#   64    2896.416    2973.14481667    ...
	#  128    2939.122    2966.26853333    ...
	#  256    2942.116    2975.00441667    ...
	#  ...
	# 4096    2989.011    3033.3538        ...
	#
	# where x is the task size (in bytes) and columns are:
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
		
		DAT="$DATDIR/${mode}-throughput.dat"
		
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
		
		for N in 64 128 256 512 1024 2048 4096; do
		
		printf "Figure %3s: line %-30s: x %4d: " "$FIGID" "$line" "$N"
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
		# Extract average throughput from log file
		
		x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 2 2>&1)
		[ $? -ne 0 ] && echo "error 2" && continue
		
		# Compare with existing value in dat file
		d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$N" -o 2 2>&1)
		[ $? -ne 0 ] && echo "error 3" && continue
		
		printf "y-value (%8.2f) deviates from original value by %+8.2f%%\n" "$x" "$d"
		
		done
	done
	
	# Repeat inner loop for hybrid latency measurements
	for mode in "hybrid"; do
		
		DAT="$DATDIR/${mode}-latency.dat"
		
		line="'Saber latency'"
		
		for N in 64 128 256 512 1024 2048 4096; do
		
		printf "Figure %3s: line %-30s: x %4d: " "$FIGID" "$line" "$N"
		
		LOG="$RESULTDIR/${N}-3.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
		# Extract average latency from log file
		
		x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -l -x 2 2>&1)
		[ $? -ne 0 ] && echo "error 2" && continue
		
		# Compare with existing value in dat file
		d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$N" -o 2 2>&1)
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
			P="$DATDIR/figure-12/selection"
			
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

