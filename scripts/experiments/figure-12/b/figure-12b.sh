#!/bin/bash

# Figure 12b: Performance impact of query task size for different query types

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
F="$SABER_HOME/scripts/experiments/figure-12/b"
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

FIGID="12b"

DURATION=

saberExperimentSetup () {
    return 0
}

saberExperimentRun () {
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

