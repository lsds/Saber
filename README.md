# Readme for reproducibility submission of paper ID 195

### Paper title 

SABER: Window-based hybrid stream processing for heterogeneous architectures

### Authors

Alexandros Koliousis, Matthias Weildich, Raul Castro Fernandez, Alexander L. Wolf, Paolo Costa, and Peter Pietzuch 

## A) Source code info

The Saber source code is available to download from:

[https://github.com/lsds/saber/tree/sigmod16-reproducibility](https://github.com/lsds/saber/tree/sigmod16-reproducibility)

### Programming Language & Compiler Info

Saber has been implemented in Java and C. The Java code is compiled and packaged using Apache Maven (3.3.1) and the Java SDK (1.7.0.79). The C code is compiled and packaged using GNU make (3.81) and gcc (4.8.4).

The CPU and GPU query operators have been implemented in Java and OpenCL, respectively. The GPU OpenCL operators are compiled _just-in-time_, when an operator is instantiated by a Saber application.

### Packages & Libraries Needed

The system has been mainly tested on **Ubuntu Linux 14.04**. The `prepare-software.sh` script uses APT to handle the installation of these dependencies.

Essential commands and their corresponding Ubuntu packages are: `javac` and `java` (openjdk-7-jdk), `mvn` (maven), `make` (make), `gcc` (gcc), and `python` (python).

Optional commands and their packages are `epstopdf` (texlive-font-utils), and `gnuplot` (gnuplot). These are used to replot figures from any reproduced results.

#### The libOpenCL.so library

Saber requires libOpenCL.so to be present on the OS library path (`LD_LIBRARY_PATH`). In our installation, it is located under `/usr/local/cuda/lib64/`. The OpenCL headers are located under `/usr/local/cuda/include`.

## B) Hardware Info

We performed our experiments on an HP Z840 Workstation.

### Processors

Our machine has 2 Intel Xeon CPU E5-2640 v3 @ 2.60GHz. There are 8 CPU cores per socket. Hyper-threading is disabled. The L1d (L1i), L2, and L3 cache sizes are 32 KB, 256 KB, and 20 MB, respectively.

### Memory 

Our machine has a total of 64 GB of DDR4 SDRAM, spread equally across 8 CPU DIMM slots. Clock frequency is 2,133 MHz.

### GPU

The GPU used in our experiments is an NVIDIA Quadro K5200 with 2,304 CUDA cores and 8,123 MB of RAM, attached to the host via PCIe 3.0 x16. The GPU has **two copy engines**, allowing for concurrent copy and kernel execution.

```
Device 0: "Quadro K5200"  
  CUDA Driver Version / Runtime Version          8.0 / 7.0  
  CUDA Capability Major/Minor version number:    3.5  
  Total amount of global memory:                 8123 MBytes (8517124096 bytes)  
  (12) Multiprocessors, (192) CUDA Cores/MP:     2304 CUDA Cores  
  GPU Max Clock rate:                            771 MHz (0.77 GHz)  
  Memory Clock rate:                             3004 Mhz  
  Memory Bus Width:                              256-bit  
  L2 Cache Size:                                 1048576 bytes  
  Maximum Texture Dimension Size (x,y,z)         1D=(65536), 2D=(65536, 65536), 3D=(4096, 4096, 4096)  
  Maximum Layered 1D Texture Size, (num) layers  1D=(16384), 2048 layers  
  Maximum Layered 2D Texture Size, (num) layers  2D=(16384, 16384), 2048 layers  
  Total amount of constant memory:               65536 bytes  
  Total amount of shared memory per block:       49152 bytes  
  Total number of registers available per block: 65536  
  Warp size:                                     32  
  Maximum number of threads per multiprocessor:  2048  
  Maximum number of threads per block:           1024  
  Max dimension size of a thread block (x,y,z): (1024, 1024, 64)  
  Max dimension size of a grid size    (x,y,z): (2147483647, 65535, 65535)  
  Maximum memory pitch:                          2147483647 bytes  
  Texture alignment:                             512 bytes  
  Concurrent copy and kernel execution:          Yes with 2 copy engine(s)  
  Run time limit on kernels:                     No  
  Integrated GPU sharing Host Memory:            No  
  Support host page-locked memory mapping:       Yes  
  Alignment requirement for Surfaces:            Yes  
  Device has ECC support:                        Disabled  
  Device supports Unified Addressing (UVA):      Yes  
  Device PCI Domain ID / Bus ID / location ID:   0 / 4 / 0  
```

## C) Experimentation Info

### Installing the system

The `prepare-software.sh` script will guide you through the installation and compilation process of our system.

```
$ git clone http://github.com/lsds/saber.git saber.git
$ cd saber.git
$ git checkout --track origin/sigmod16-reproducibility
$ export SABER_HOME=`pwd`
$ ./scripts/prepare-software.sh
```

### Executing the experiments

To execute all available experiments and recompile the paper, run:

```
$ ./scripts/run-experiments.sh
```

To reproduce specific figures, use the `--figure` (or `-f`) flag. For example, the command:

```
$ ./scripts/run-experiments.sh -f 11b -f 14
```

reproduces Figure 11(b) and Figure 14 from the paper.

Unless the `--recompile-paper` (or `-r`) flag is set to `false`, the script uses results from any reproduced experiments to **replot figures** (say, Figure 11(b) and Figure 14 from the example above). Figures that have not been reproduced are replotted using the original results from the paper.

### The saber.conf file

Edit `scripts/saber.conf` and revisit Saber's default configuration parameters if necessary.

#### Maven repository

`MVN_REPOSITORY` is the local folder where Saber's Java dependencies are downloaded and stored automatically by Maven. The default location on Linux is `$HOME/.m2`. The variable is used to construct the Java class path when running Saber appications from the command line.

#### JVM configuration

`SABER_JVM_MS` and `SABER_JVM_MX` set the minimum and maximum Java heap size (in GB) of a Saber application. In our experiments, both values are set to 48 GB (out of 64 GB available on the machine), resulting in a **fixed heap size**.

`SABER_JVM_NEWRATIO` sets the proportion of the heap dedicated to the young generation, where all new Java objects are allocated. The default value of `2` dedicates a third of the heap to the young generation.

`SABER_JVM_SURVIVORRATIO` sets the ratio between Eden and Survivor space in the young generation. The default value is `16`.

Based on the default configuration of the JVM, the Eden space of a Saber application is approximately 14 GB. In our experiments, the Eden space is never filled and thus no minor (_Stop the world_) garbage collection events occur. Saber reuses all major objects allocated (e.g., intermediate result buffers, tasks, etc.).

#### Saber system configuration

Variables in **saber.conf** prefixed with `SABER_CONF` configure the Saber runtime. Each of them also corresponds to a command-line argument available to all Saber applications:

######--execution-mode `cpu`|`gpu`|`hybrid`

Sets the execution mode to either CPU-only, GPU-only or Hybrid, respectively. In the latter case, both processors execute query tasks opportunistically. The default execution mode is `cpu`.

It sets the `SABER_CONF_EXECUTIONMODE` variable.

######--number-of-worker-threads _N_

Sets the number of CPU worker threads. The default value is `1`. In GPU-only execution mode, the value must be `1`. **CPU worker threads are pinned to physical cores**. The first thread is pinned to core id 1, the second to core id 2, and so on.

It sets the `SABER_CONF_NUMWORKERTHREADS` variable.

######--number-of-result-slots _N_

Sets the number of intermediate query result slots. The default value is `1024`. 

It sets the `SABER_CONF_NUMRESULTSLOTS` variable.

######--scheduling-policy `fcfs`|`static`|`hls`

Sets the scheduling policy to either First-Come-First-Served (FCFS), Static or Heterogeneous Look-ahead Scheduling (HLS), respectively. The default value is `fcfs`.

It sets the `SABER_CONF_SCHEDULINGPOLICY` variable.

######--switch-threshold _N_

Used by the HLS scheduling algorithm (`--scheduling-policy hls`) to allow the non-preferred processor of a query to execute some of that query's tasks. The default value is `10`, allowing the non-preferred processor to execute 1 every 10 tasks executed on the preferred one.

It sets the `SABER_CONF_SWITCHTHRESHOLD` variable.

######--number-of-partial-windows _N_

Sets the maximum number of window fragments in a query task. The default value is `65536`.

It sets the `SABER_CONF_NUMPARTIALWINDOWS` variable.

######--circular-buffer-size _N_

Sets the circular buffer size, in bytes. The default value is `1073741824`, i.e. 1 GB. 
	
It sets the `SABER_CONF_CIRCULARBUFFERSIZE` variable.

######--intermediate-buffer-size _N_

Sets the intermediate result buffer size, in bytes, The default value is `1048576`, i.e. 1 MB.

It sets the `SABER_CONF_INTERMEDIATEBUFFERSIZE` variable.

######--hash-table-size _N_

Hash table size (in bytes): hash tables hold partial window aggregate results (default is 1048576, i.e. 1MB). 

It sets the `SABER_CONF_HASHTABLESIZE` variable.

######--throughput-monitor-interval _N_

Sets the query throughput matrix update interval, in msec. The default value is `1000` i.e. 1 sec.

It sets the `SABER_CONF_THROUGHPUTMONITORINTERVAL` variable. 

######--performance-monitor-interval _N_

Sets the performance monitor interval, in msec. The default value is `1000`, i.e. 1 sec. Controls how often Saber prints on standard output performance statistics such as throughput and latency. 

It sets the `SABER_CONF_PERFORMANCEMONITORINTERVAL` variable.

######--pipeline-depth _N_

Sets the GPU pipeline depth - the number of query tasks scheduled on the GPU before the result of the first one is returned. The default value is `4`. 

It sets the `SABER_CONF_PIPELINEDEPTH` variable.

######--enable-latency-measurements `true`|`false`

Determines whether Saber should measure task latency or not. Default value is `false`. 

It sets the `SABER_CONF_MEASURELATENCY` variable.

######--launch-web-server `true`|`false`

Determines whether Saber should launch the _Saber Workbench_ back-end server (see our DEBS 2016 demo for details). Default value is `false`.

It sets the `SABER_CONF_LAUNCHWEBSERVER` variable. 

######--experiment-duration _N_

Sets the duration after which no more performance statistics will be recorded by the system, in _performance monitor interval_ units. For example, if `--performance-monitor-interval 500` and `--experiment-duration 20` then the experiment duration is 10 sec. The default value is `0`, i.e. no limit specified.

It sets the `SABER_CONF_EXPERIMENTDURATION` variable.

#### The run.sh command

######--class _Canonical class name_

Runs the specified Saber application class (e.g. `uk.ac.imperial.lsds.saber.experiments.microbenchmarks.TestProjection`).

######--mode `foreground`|`background`

Runs the Saber application specified by `--class` either in the foreground or the background. In background mode, standard output and standard error are redirected to log files in `$SABER_LOGDIR` and the application runs for a fixed duration.

######--alias _string_

Sets a Saber application alias to manage its execution in background mode. For example, if alias is `test` then stdout and stderr are redirected to `$SABER_LOGDIR/test.out` and `$SABER_LOGDIR/test.err`, respectively.

######--duration _N_

Runs the Saber application in background mode for _N_ seconds and then exits gracefully.

######-- [class arguments]

Anything command-line argument after `--` is considered a Saber application argument. For example, `-- --execution-mode cpu --number-of-worker-threads 16` will run the application is CPU-only execution mode with 16 worker threads.

### Operator microbenchmarks

Apart from the system configuration command-line arguments, all microbenchmarks also share the following:

######--batch-size _N_

Sets the query task size, in bytes. The default value is `1048576`, i.e. 1 MB.

######--window-type `row`|`range`, --window-size _N_, and --window-slide _M_

Sets the window definition for a particular query operator. Windows can be row-based or time-based, tumbling (_N_ = _M_) or sliding (_N_ > _M_). 

######--input-attributes _N_

Sets the number of attributes in the input stream. The default value is `6` and depending on the microbenchmark running this will result in 32-byte tuples.

######--tuples-per-insert _N_

Sets the number of tuples pushed to the input stream buffer. Default value is `32768`.

#### The projection.sh command

######--projected-attributes _N_

Sets the number of projected attributes. 

######--expression-depth _N_

The first attribute ("_1") is projected as the expression 3 x "_1" / 2. If the expression depth is 2 then the expression is 3 x (3 x _1 / 2) / 2; and so on.
#### The selection.sh command

######--comparisons _N_

Number of comparisons evaluated on the first attribute of a tuple.

######--selectivity _N_

The selectivity of the selection query (between 1 and 100%). 

#### The aggregation.sh command

######--aggregate-expression _string_

An aggregate expression can be one of `cnt`, `sum`, `avg`, `min`, `max` or combinations of those separated by comma (without spaces), e.g. `cnt,avg,min,max`.

#### The theta-join.sh command

The theta-join microbenchmark is similar to selection, only that it operates on two streams. Each stream can be configured using the suffix `-of-first-stream` or `-of-second-stream` in command-line arguments such as window type, size, slide, and number of input attributes. E.g. `--window-type-of-first-stream row`.

