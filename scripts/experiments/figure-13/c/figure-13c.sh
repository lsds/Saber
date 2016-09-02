#!/bin/bash

# Figure 13c: Performance impact of query task size for different window sizes and slides

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
F="$SABER_HOME/scripts/experiments/figure-13/c"
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

FIGID="13c"

DURATION=

saberExperimentSetup () {
	# Set options
	#
	# Window settings
	OPTS="$OPTS --window-type row --window-size 1024 --window-slide 1024"
	
	# Query settings
	OPTS="$OPTS --selectivity 0 --comparisons 1 --tuples-per-insert 1024"
	
	# System settings
	OPTS="$OPTS --circular-buffer-size 268435456"
	
	# Set the duration of each experiment in the figure
	[ -z "$DURATION" ] && DURATION=$SABER_DEFAULT_DURATION
	
	OPTS="$OPTS --experiment-duration $DURATION"
	
	CPU_OPTS="$CPU_OPTS --number-of-worker-threads 15"
	GPU_OPTS="$GPU_OPTS --number-of-worker-threads 1 --pipeline-depth 4"
	
	return 0
}

saberExperimentRun () {
	#
	# for N in 32 to 4096 KB/batch
	# do 
	# 	Run Selection CPU-only with task size $N (window 1024 rows, slide 1024 rows) (N-1)
	# 	Run Selection GPU-only with task size $N (window 1024 rows, slide 1024 rows) (N-2)
	# done
	#
	errors=0
	points=0
	
	for mode in "cpu" "gpu"; do
		
		line=
		key=
		
		modeoptions=
		
		if [ "$mode" = "cpu" ]; then
		line="'Saber (CPU-only) throughput'"
		key=1
		modeoptions="$CPU_OPTS"
		else
		line="'Saber (GPU-only) throughput'"
		key=2
		modeoptions="$GPU_OPTS"
		fi
		
		for N in 32 64 128 256 512 1024 2048 4096; do
		
		let points++
		
		# Convert to bytes
		TASKSIZE=`echo "$N * 1024" | bc`
		
		printf "Figure %3s: line %-30s: x %7d\n" "$FIGID" "$line" "$TASKSIZE"
		
		"$SABER_HOME"/scripts/experiments/microbenchmarks/selection.sh $OPTS $modeoptions \
		--batch-size $TASKSIZE --intermediate-buffer-size $TASKSIZE --execution-mode $mode
		
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
	
	for mode in "cpu" "gpu"; do
		
		DAT="$RESULTDIR/${mode}-throughput.dat"
		
		key=
		[ $mode = "cpu" ] && key=1 || key=2
		
		for N in 32 64 128 256 512 1024 2048 4096; do
		
		let points++
		
		# Convert to bytes
		TASKSIZE=`echo "$N * 1024" | bc`
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && let errors++ && continue
		
		L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t 2>&1)
		
		# Append line if command was successful
		[ $? -eq 0 ] && echo $TASKSIZE $L >>"$DAT" || let errors++
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
	
	DATDIR="$ORIGIN/figure-13/selection/tumbling-window/1024-1024"
	#
	# Data format:
	#
	#       x         MIN         AVG    ...
	# ______________________________________
	#   32768    6092.564    6141.328    ...
	#   65536    6469.623    6584.219    ...
	#     ...
	# 4194304    6343.313    6692.860    ...
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
	
	for mode in "cpu" "gpu"; do
		
		DAT="$DATDIR/${mode}-throughput.dat"
		
		line=
		key=
		
		if [ "$mode" = "cpu" ]; then
		line="'Saber (CPU-only) throughput'"
		key=1
		else
		line="'Saber (GPU-only) throughput'"
		key=2
		fi
		
		for N in 32 64 128 256 512 1024 2048 4096; do
		
		# Convert to bytes
		TASKSIZE=`echo "$N * 1024" | bc`
		
		printf "Figure %3s: line %-30s: x %7d: " "$FIGID" "$line" "$TASKSIZE"
		
		LOG="$RESULTDIR/${N}-${key}.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && echo "error 1" && continue
		
		# Extract average throughput from log file
		
		x=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t -x 2 2>&1)
		[ $? -ne 0 ] && echo "error 2" && continue
		
		# Compare with existing value in dat file
		d=$(python "$SABER_HOME"/scripts/comparator.py -x "$x" -f "$DAT" -k "$TASKSIZE" -o 2 2>&1)
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
			P="$DATDIR/figure-13/selection/tumbling-window/1024-1024"
			
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

