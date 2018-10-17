#!/bin/bash
USAGE="usage: ./build_cpu.sh"

if [ -z "$1" ]
  then
    echo "No argument supplied"
    exit 1
fi

echo "Building CPU lib..."

make -C $1

echo "Done."
echo "Bye."

exit 0

