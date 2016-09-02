#!/bin/bash
#
# Scripts that tries to install cuda-6.5 on Ubuntu 14.04
# usage: ./try-install-cuda-6.5.sh

# Check if SABER_HOME is set
if [ -z "$SABER_HOME" ]; then
	echo "error: \$SABER_HOME is not set"
	exit 1
fi

# Source common functions
. "$SABER_HOME"/scripts/common.sh

# Source configuration parameters
. "$SABER_HOME"/scripts/saber.conf

# Check that this system is running Linux, 
# and more specifically Ubuntu 14.04

OS=`uname -s`

[ $OS != "Linux" ] && {
echo "error: unsupported operating system: $OS"
exit 1
}

saberProgramExistsOrExit "lsb_release"

D=$(lsb_release -i | awk '{ split($0, t, ":"); print t[2] }') # Distribution
R=$(lsb_release -r | awk '{ split($0, t, ":"); print t[2] }') # Release
# Trim whitespaces
D=`echo $D | sed 's/[ 	]*//'` # <space> or <tab>
R=`echo $R | sed 's/[ 	]*//'`

[ \( "$D" != "Ubuntu" \) -a \( "$R" != "14.04" \) ] && {
echo "error: unsupported Linux distribution: $D $R"
exit 1
}

# Unlikely
saberProgramExistsOrExit "wget"

# Download package to $SABER_HOME/cuda-6.5/deb
#
if [ -d "$SABER_HOME/cuda-6.5/deb" ]; then
	# If the directory exists, delete the deb file
	rm -f "$SABER_HOME"/cuda-6.5/deb/*.deb
else
	mkdir -p "$SABER_HOME/cuda-6.5/deb"
fi

wget --input-file="$SABER_HOME/cuda-6.5/URL" --output-file="$SABER_HOME/cuda-6.5/wget.log" --directory-prefix="$SABER_HOME/cuda-6.5/deb"
[ $? -ne 0 ] && {
echo "error: failed to download cuda-6.5 package (transcript written on $SABER_HOME/cuda-6.5/wget.log)"
exit 1
}

# Unlikely
saberProgramExistsOrExit "dpkg"

# At this point, try install cuda-6.5

PKG=$(ls "$SABER_HOME/cuda-6.5/deb")
LOG="$SABER_HOME/cuda-6.5/install.log"

[ -f "$LOG" ] && rm -f "$LOG"
touch "$LOG"

sudo dpkg -i "$SABER_HOME/cuda-6.5/deb/$PKG" >>"$LOG" 2>&1
[ $? -ne 0 ] && {
echo "error: failed to install cuda-6.5 package (transcript written on $LOG)"
exit 1
}

echo "Updating package list. This may take some time."
sudo apt-get update >>"$SABER_HOME/cuda-6.5/install.log" 2>&1

echo "Installing cuda-6.5..."
saberInstallPackage "cuda-6-5" "$LOG" || exit 1

echo "CUDA 6.5 library install successful (transcript written on $LOG)"
echo ""
echo "Post-installation commands include:"
echo ""
echo "export CUDA_HOME=/usr/local/cuda-6.5"
echo "export PATH=\$CUDA_HOME/bin:\$PATH"
echo "export LD_LIBRARY_PATH=\$CUDA_HOME/lib64:\$LD_LIBRARY_PATH"
echo ""
echo "Bye."

exit 0
