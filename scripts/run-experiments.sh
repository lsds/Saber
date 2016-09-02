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

USAGE="usage: run-experiments.sh [ -f, --figure ]* [ -r, --recompile-paper ]"

# Command-line arguments
F=
R="true"

# Parse command-line arguments
while :
do
	case "$1" in
		-f | --figure)
		saberOptionIsValidFigureId "$1" "$2" || exit 1
		saberOptionIsDuplicateFigureId "$1" "$2" $F || exit 1
		[ -z "$F" ] && F="$2" || F="$F $2"
		shift 2
		;;
		-r | --recompile-paper)
		saberOptionIsBoolean "$1" "$2" || exit 1
		R="$2"
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


# Find next major version
major=`saberNextMajorVersion`

# Export variable
export SABER_MAJOR_VERSION="$major"

if [ -z "$F" ]; then # Run all available experiments
	
	for N in 1 7 8 9 10 11 12 13 14 15 16; do
		
		# Run experiments to reproduce Figure $N
		idx=`printf "%02d" $N`
		"$SABER_HOME"/scripts/experiments/figure-${idx}/figure-${idx}.sh
	done

else # Reproduce specific figure(s)
	
	for fig in $F; do
		
		# Split string
		f=`echo "$fig" | tr -d [a-z,A-Z]`
		s=`echo "$fig" | tr -d [0-9]`
		
		idx=`printf "%02d" $f`
		
		if [ -n "$s" ]; then
		"$SABER_HOME"/scripts/experiments/figure-${idx}/${s}/figure-${idx}${s}.sh
		else
		"$SABER_HOME"/scripts/experiments/figure-${idx}/figure-${idx}.sh
		fi
	done
fi

if [ "$R" = "true" ]; then # Recompile the paper
	"$SABER_HOME"/scripts/recompile-paper.sh -v $major
fi

echo "Bye."

exit 0
