# Saber
Window-based hybrid stream processing engine

#### Quick Start

$ git clone https://github.com/lsds/saber.git

$ cd saber

$ export SABER_HOME=\`pwd\`

Make sure your JAVA_HOME is set. E.g.:

$ export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64

$ ./build.sh

$ cd clib/

$ make cpu

$ cd ..

$ ./run.sh uk.ac.imperial.lsds.saber.microbenchmarks.TestNoOp --mode cpu

