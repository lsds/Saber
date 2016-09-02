#!/bin/bash

# Figure 14: Scalability of CPU operator implementation

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
F="$SABER_HOME/scripts/experiments/figure-14"
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

FIGID="14"

DURATION=

saberExperimentSetup () {
	# Set options
	# 
	# Window settings
	OPTS="$OPTS --window-type row --window-size 1024 --window-slide 1024"
	
	# Query settings
	OPTS="$OPTS --projected-attributes 6"
	
	# System settings
	OPTS="$OPTS --execution-mode cpu"
	
	# Set the duration of each experiment in the figure
	[ -z "$DURATION" ] && DURATION=$SABER_DEFAULT_DURATION
	
	OPTS="$OPTS --experiment-duration $DURATION"
	
	return 0
}

saberExperimentRun () {
	#
	# for N in 1 2 4 8 16 32 worker threads
	# do 
	# 	Run Projection CPU-only with $N worker threads (N-1)
	# done
	#
	errors=0
	points=0
	
	line="'Saber (CPU-only) throughput'"
	
	for N in 1 2 4 8 16 32; do
		
		let points++
		
		printf "Figure %3s: line %-30s: x %2d\n" "$FIGID" "$line" "$N"
		
		"$SABER_HOME"/scripts/experiments/microbenchmarks/projection.sh $OPTS \
		--number-of-worker-threads $N
		
		errorcode=$?
		
		saberExperimentStoreResults "projection" "$RESULTDIR" "$N-1"
		
		# Increment errors based on exit code
		[ $errorcode -ne 0 ] && let errors++ && continue
		
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
	DAT="$RESULTDIR/cpu-scalability.dat"
	
	points=0
	errors=0
	
	for N in 1 2 4 8 16 32; do
		
		let points++
		
		LOG="$RESULTDIR/$N-1.out"
		
		# Is it a valid log file?
		! saberExperimentLogIsValid "$LOG" && let errors++ && continue
		
		L=$(python "$SABER_HOME"/scripts/parser.py -f "$LOG" -t 2>&1)
		
		# Append line if command was successful
		[ $? -eq 0 ] && echo $N $L >>"$DAT" || let errors++
	done
	
	d=$((points - errors))
	printf "Figure %3s: %2d/%2d plot points generated successfully\n" "$FIGID" "$d" "$points"
	
	# b=`wc -c < "$DATFILE" | sed -e 's/^[ \t]*//'`
	# echo "Output written on $DAT ($b bytes)"
	
	return $errors
}

saberExperimentCheckResults () {
	#
	# Results are stored in:
	#
	ORIGIN="$SABER_HOME/doc/paper/16-sigmod/plots/data/original"
	
	DAT="$ORIGIN/figure-14/cpu-scalability.dat"
	#
	# Data format:
	#
	#  x    MIN         AVG              ...
	# ______________________________________
	#  1     233.766     248.29272131    ... 
	#  2     442.557     493.75234426    ... 
	#  4     925.075    1028.25632787    ... 
	#  8    1929.071    2028.51506557    ... 
	# 16    3399.600    3840.81101639    ... 
	# 32    3226.773    3807.46011475    ... 
	#
	# where x is the number of worker threads and columns are:
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
	
	for line in "'Saber (CPU-only) throughput'"; do
			
		for N in 1 2 4 8 16 32; do
		
		LOG="$RESULTDIR/$N-1.out"
		
		printf "Figure %3s: line %-30s: x %2d: " "$FIGID" "$line" "$N"
		
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
	
	return 0
}

#
# Main
#
OPTS=

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
			P="$DATDIR/figure-14"
			
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
