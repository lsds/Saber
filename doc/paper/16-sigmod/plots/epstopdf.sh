#!/bin/bash

EPSDIR="bin/eps/"

if [ ! -d "$EPSDIR" ]; then
	echo "error: ${EPSDIR}: directory not found"
	exit 1
fi

# Check if `epstopdf` command exists
which epstopdf >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "error: epstopdf: command not found"
	exit 1
fi

# Check if `gs` exists
GS=false
which gs >/dev/null 2>&1
[ $? -eq 0 ] && GS=true

OPTS=
OPTS=${OPTS}" -q"
OPTS=${OPTS}" -dBATCH"
OPTS=${OPTS}" -dNOPAUSE"
OPTS=${OPTS}" -sDEVICE=pdfwrite"
OPTS=${OPTS}" -dPDFSETTINGS=/prepress"
OPTS=${OPTS}" -dEmbedAllFonts=true"
OPTS=${OPTS}" -dSubsetFonts=true"
OPTS=${OPTS}" -dCompatibilityLevel=1.4"

PLOTS=`pwd`
cd $EPSDIR
for filename in `ls *.eps`; do
	printf "%-45s\t" $filename
	n="${filename%.*}"
	
	t="$n-tmp.pdf" # Temporary file
	y="../$n.pdf"  # Final output
	
	epstopdf $filename --outfile=${t}
	
	if [ "$GS" = true ]; then
		gs ${OPTS} -sOutputFile=${y} ${t}
	else
		# Do not embed fonts; simply copy
		cp ${t} ${y}
	fi
	rm -f ${t}
	echo "OK"
done
# Back to parent directory
cd $PLOTS

exit 0
