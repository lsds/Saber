# Saber
Window-based hybrid stream processing engine


### Programming Language & Compiler Info

Saber has been implemented in Java and C. The Java code is compiled and packaged using Apache Maven (3.3.1) and the Java SDK (Oracle Java 8). The C code is compiled and packaged using GNU make (3.81) and gcc (4.8.4).

The CPU and GPU query operators have been implemented in Java and OpenCL, respectively. The GPU OpenCL operators are compiled _just-in-time_, when an operator is instantiated by a Saber application.

#### Quick Start

$ git clone -b Performance_Engineering_339 https://github.com/lsds/Saber.git

$ cd Saber

$ echo "export SABER_HOME=$(pwd)" >> $HOME/.profile

$ echo "export PATH=\$SABER_HOME/clib:\$PATH" >> $HOME/.profile

$ source /home/$USER/.profile

Make sure your JAVA_HOME is set. E.g.:

$ echo "export JAVA_HOME=/usr/lib/jvm/oracle-java10-jdk-amd64" >> $HOME/.bashrc

$ echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> $HOME/.bashrc

$ ./build.sh

$ cd clib/

$ make cpu

$ cd ..

$ ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp
