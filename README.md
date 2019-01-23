# Saber
Window-based hybrid stream processing engine

#### Quick Start

$ git clone -b Performance_Engineering_339 https://github.com/lsds/Saber.git

$ cd Saber

$ echo "export SABER_HOME=$(pwd)" >> /home/$USER/.profile

$ echo "export PATH=\$SABER_HOME/clib:\$PATH >> /home/$USER/.profile

$ source /home/$USER/.profile

Make sure your JAVA_HOME is set. E.g.:

$ export JAVA_HOME=/usr/lib/jvm/java-8-oracle

$ ./build.sh

$ cd clib/

$ make cpu

$ cd ..

$ ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp
