#!/bin/bash
#
# Delete all files generated automatically
# by various scripts in this distribution.
#
# usage: ./reset.sh
#

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
	echo "error: \$SABER_HOME is not set"
	exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

echo -n "Delete all generated files, including any reproduced results (yes/no)? "
while true; do
	read a
	case "$a" in
	y|yes)
	result=0
	break
	;;
	n|no)
	result=1
	break
	;;
	*)
	echo -n "Invalid option: \"$a\". Choose 'yes' or 'no': "
	;;
	esac
done

[ $result -eq 1 ] && exit 1

# Start

rm -rf "$SABER_LOGDIR"
rm -rf "$SABER_PIDDIR"

rm -f "$SABER_TRAP"

# Delete log files in Saber home directory

rm -f "$SABER_HOME"/apt.log*
rm -f "$SABER_HOME"/build.log
rm -f "$SABER_HOME"/run.log

# Delete Saber code distribution

rm -rf "$SABER_HOME"/target
rm -rf "$SABER_HOME"/lib

# clib 
rm -f "$SABER_HOME"/clib/Makefile*
rm -f "$SABER_HOME"/clib/build.log
rm -f "$SABER_HOME"/clib/*.so
rm -f "$SABER_HOME"/clib/*.o

# Delete cuda-6.5 files
rm -rf "$SABER_HOME"/cuda-6.5/deb
rm -f "$SABER_HOME"/cuda-6.5/*.log

# Delete intermediate files in doc/
# (equivalent to `make clean`)
rm -f "$SABER_HOME"/doc/paper/16-sigmod/saber-sigmod.pdf
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.log 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.dvi 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.aux 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.bbl 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.blg 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.tpm 
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.out
rm -f "$SABER_HOME"/doc/paper/16-sigmod/*.ps

# Delete reproduced results in doc/
rm -rf "$SABER_HOME"/doc/paper/16-sigmod/plots/data/reproducibility-results

# Delete any reproduced results for each figure

rm -rf "$SABER_HOME"/scripts/experiments/figure-01/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-07/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-08/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-09/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-10/a/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-10/b/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-11/a/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-11/b/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-12/a/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-12/b/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-12/c/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-13/a/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-13/b/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-13/c/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-14/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-15/results
rm -rf "$SABER_HOME"/scripts/experiments/figure-16/results

echo "Bye."

exit 0
