#!/bin/bash

# Figure 7: Performance of application benchmark queries

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
F="$SABER_HOME/scripts/experiments/figure-07"
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

FIGID="7"

DURATION=

saberExperimentSetup () {
	return 0
}

saberExperimentRun () {
	#
	# Run  CM1
	# "$SABER_HOME"/scripts/experiments/benchmarks/cluster-monitoring/cluster-monitoring.sh
	# Run  CM2
	# "$SABER_HOME"/scripts/experiments/benchmarks/cluster-monitoring/cluster-monitoring.sh
	# Run  SG1
	# "$SABER_HOME"/scripts/experiments/benchmarks/smartgrid/smartgrid.sh
	# Run  SG2
	# "$SABER_HOME"/scripts/experiments/benchmarks/smartgrid/smartgrid.sh
	# Run  SG3
	# "$SABER_HOME"/scripts/experiments/benchmarks/smartgrid/smartgrid.sh
	# Run LRB1
	# "$SABER_HOME"/scripts/experiments/benchmarks/linear-road/linear-road-benchmark.sh
	# Run LRB2
	# "$SABER_HOME"/scripts/experiments/benchmarks/linear-road/linear-road-benchmark.sh
	# Run LRB3
	# "$SABER_HOME"/scripts/experiments/benchmarks/linear-road/linear-road-benchmark.sh
	# Run LRB4
	# "$SABER_HOME"/scripts/experiments/benchmarks/linear-road/linear-road-benchmark.sh
	#
	# Run  CM1 using Esper
	# Run  CM2 -"-
	# Run  SG1 -"-
	# Run  SG2 -"-
	# Run  SG3 -"-
	# Run LRB1 -"-
	# Run LRB2 -"-
	# Run LRB3 -"-
	# Run LRB4 -"-
	#
	return 0
}

saberExperimentClear () {
	return 0
}

saberExperimentParseResults () {
	return 0
}

saberExperimentCheckResults () {
	return 0
}

#
# Main
#
OPTS=

printf "Figure %3s: reproducibility version %d.%d\n" "$FIGID" $major $minor

printf "Figure %3s: reproducibility of this figure is not supported yet\n" "$FIGID"

exit 0
