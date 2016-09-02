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

USAGE="usage: recompile-paper.sh [ -v, --version ]"

replot () {
	major="$1"
	
	# Assumes we are in $DOC
	cd plots/
	
	P="data/reproducibility-results/${major}.0"
	
	args=""
	
	[ -n "$major" ] && [ -d "$P" ] && { 
	#
	# Find out which figures were reproduced
	# based on their relative path P
	#
	args="ver = '${major}.0';"
	
	FIG01="$P/figure-01/"
	FIG07="$P/figure-07/"
	FIG08="$P/figure-08/"
	FIG09="$P/figure-09/"
	FIG10a="$P/figure-10/selection/"
	FIG10b="$P/figure-10/join/"
	FIG11a="$P/figure-11/selection/"
	FIG11b="$P/figure-11/aggregation/"
	FIG12a="$P/figure-12/selection/"
	FIG12b="$P/figure-12/group-by/"
	FIG12c="$P/figure-12/join/"
	FIG13a="$P/figure-13/selection/tumbling-window/1-1/"
	FIG13b="$P/figure-13/selection/sliding-window/1024-1/"
	FIG13c="$P/figure-13/selection/tumbling-window/1024-1024/"
	FIG14="$P/figure-14/"
	FIG15="$P/figure-15/"
	FIG16="$P/figure-16/"
	
	[ -d "$FIG01"  ] && args="$args fig01  = '$FIG01';"
	[ -d "$FIG07"  ] && args="$args fig07  = '$FIG07';"
	[ -d "$FIG08"  ] && args="$args fig08  = '$FIG08';"
	[ -d "$FIG09"  ] && args="$args fig09  = '$FIG09';"
	[ -d "$FIG10a" ] && args="$args fig10a = '$FIG10a';"
	[ -d "$FIG10b" ] && args="$args fig10b = '$FIG10b';"
	[ -d "$FIG11a" ] && args="$args fig11a = '$FIG11a';"
	[ -d "$FIG11b" ] && args="$args fig11b = '$FIG11b';"
	[ -d "$FIG12a" ] && args="$args fig12a = '$FIG12a';"
	[ -d "$FIG12b" ] && args="$args fig12b = '$FIG12b';"
	[ -d "$FIG12c" ] && args="$args fig12c = '$FIG12c';"
	[ -d "$FIG13a" ] && args="$args fig13a = '$FIG13a';"
	[ -d "$FIG13b" ] && args="$args fig13b = '$FIG13b';"
	[ -d "$FIG13c" ] && args="$args fig13c = '$FIG13c';"
	[ -d "$FIG14"  ] && args="$args fig14  = '$FIG14';"
	[ -d "$FIG15"  ] && args="$args fig15  = '$FIG15';"
	[ -d "$FIG16"  ] && args="$args fig16  = '$FIG16';"
	
	}
	
	if [ -n "$args" ]; then
	gnuplot -e "$args" plotall.gnuplot
	else
	gnuplot plotall.gnuplot
	fi
	# Convert eps files to pdf
	./epstopdf.sh
	# Go back to $DOC
	cd ..
	return 0
}

compile () { 
	# Assumes we are in $DOC
	./compile.sh --mode auto --silent --force
	errorcode=$?
	# [ $errorcode -ne 0 ] && echo "error: failed to compile paper"
	return $errorcode
}

#
# Main
#

# Command-line argument
VERSION=""

# Parse command-line arguments
while :
do
	case "$1" in
		-v | --version)
		saberOptionIsPositiveInteger "$1" "$2" || exit 1
		VERSION="$2"
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

# Absolute path to the directory where the paper is stored
DOC="$SABER_HOME/doc/paper/16-sigmod"

saberDirectoryExistsOrExit $DOC

saberProgramExistsOrExit "gnuplot"

cd "$DOC" # Goto paper

replot $VERSION

#
# Re-generating the paper .pdf
# from LaTeX sources disabled.
#
# compile

exit 0

