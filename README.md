# Objective
The goal of this exercise is to evaluate your understanding on performance analysis on a complex system for a given application. With the use of your favorite analysis tools, you should identify the bottlenecks of the application and focus on the computation (operator implementation), rather than other costs regarding scheduling, data generation etc. that will show up on your profiling.

For our evaluation, we’ve chosen to use SABER (https://lsds.doc.ic.ac.uk/projects/saber), a streaming engine that utilises a hybrid processing model on heterogeneous processors within a single node. In SABER, data are represented in a row-oriented format with the use of ByteBuffers (http://mindprod.com/jgloss/bytebuffer.html). We provide you with an extended version of the original system, that utilises Direct ByteBuffers, that reside out of the JVM. This version combined with the Java Native Interface (JNI) calls, allows us to utilise native code and boost the performance of our applications. As memory is allocated out of the Java memory heap, we avoid unnecessary copies when we want to access data. However, it is important to always set the endianness of Byte Buffers to LittleEndian, which is not the default.


# Saber
Window-based hybrid stream processing engine

Saber has been implemented in Java and C. The Java code is compiled and packaged using Apache Maven (3.3.1) and the Java SDK (Oracle Java 8). The C code is compiled and packaged using GNU make (3.81) and gcc (4.8.4).

The CPU and GPU query operators have been implemented in Java and OpenCL, respectively. The GPU OpenCL operators are compiled _just-in-time_, when an operator is instantiated by a Saber application.

# Getting Started

$ git clone -b Performance_Engineering_339 https://github.com/lsds/Saber.git

$ cd Saber

$ echo "export SABER_HOME=$(pwd)" >> $HOME/.profile

$ echo "export PATH=\\$SABER_HOME/clib:\\$PATH" >> $HOME/.profile

$ source /home/$USER/.profile

Make sure your JAVA_HOME is set. E.g.:

$ echo "export JAVA_HOME=/usr/lib/jvm/oracle-java10-jdk-amd64" >> $HOME/.bashrc

$ echo "export PATH=\\$JAVA_HOME/bin:\\$PATH" >> $HOME/.bashrc

$ ./build.sh

$ cd clib/

$ make cpu

$ cd ..

# The Benchmark

The input schema of our benchmark data stream is:

inputSchema {
	
	long timestamp;		// 8 bytes
	
	long long user_id; 	// 16 bytes
	
	long long page_id; 	// 16 bytes
	
	long long ad_id;   	// 16 bytes
	
	int ad_type;	   	// 4 bytes
	
	int event_type;    	// 4 bytes
	
	int ip_address;    	// 4 bytes
	
	char padding[60]   	// 60 bytes
	
				// 128 bytes in total
				
}				

The applications we want to optimise are related to Yahoo Streaming Benchmark (https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at). These are the queries we have:

q0: select(inputStream, Predicate(column, constant, comparisonPredicate))--[25% selectivity]-->

project(inputStream, columns)-->

staticHashJoin(inputStream, columnLeftSide, staticRelation, columnRightSide, comparisonPredicate)-->

select(inputStream, Predicate(column, constant, comparisonPredicate))

q1: select(inputStream, Predicate(column, constant, comparisonPredicate))--[50% selectivity]-->

project(inputStream, columns)-->

staticHashJoin(inputStream, columnLeftSide, staticRelation, columnRightSide, comparisonPredicate)-->

select(inputStream, Predicate(column, constant, comparisonPredicate))

q2: select(inputStream, Predicate(column, constant, comparisonPredicate))--[50% selectivity]-->

select(inputStream, Predicate(column, constant, comparisonPredicate))

q3: select(inputStream, Predicate(column, constant, comparisonPredicate))--[25% selectivity]-->

project(inputStream, columns)-->

staticHashJoin(inputStream, columnLeftSide, staticRelation, columnRightSide, comparisonPredicate)-->

windowAggregate(column, aggrFunction, groupColumn, windowSemantics)

You can run each of the queries like this:

`./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.PerformanceEngineeringApp`

In your console’s output you will see messages like:
Throughput Average (GB/s): 2,135.784, Throughput Average(records/s): 69,985,376.294
[MON] S000   2071.928 MB/s output      0.158 MB/s [null] q      7 ([[0][0]=    0 [0][1]=    0 [1][0]=    4 [1][1]=    1]) t      8 w      8 b      2 p      4 policy [[0][0]=    0 [0][1]=    0 [1][0]= 1040 [1][1]=    0]
The highlighted values are the average and current throughput of the application and the metrics that you try to improve.

# Task 1: Exploration to focus optimization effort
Saber is a complex processing system and, therefore, has significant potential for performance improvement in many parts of the code. In the course of this exercise, we target the performance of the computation operators themselves which you can find inside the uk.ac.imperial.lsds.saber.cql.operators package. We suggest that you treat the system as a black box and focus on the operators. Use the tools you have learned about to focus your effort on the right parts of the code.

To constrain the scope, focus your efforts on the first three queries (q0 through q2). While performance of the last query is interesting, it is not part of this coursework (and will, thus, not be assessed).

# Task 2: Optimization through native code
After identifying the critical operators, you shall accelerate their implementation. For that purpose, you shall reimplement them in native code and bind them/call them through the Java Native Interface (JNI). As a ballpark number, you should get a performance improvement exceeding 50% and (in some cases) more than 2x.


A good tutorial to get you started with JNI is: https://www.ibm.com/developerworks/java/tutorials/j-jni/j-jni.html 
This link will help you with the access of direct byte buffers: https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/functions.html#GetDirectBufferAddress 

As a hint for using JNI, take a look at uk.ac.imperial.lsds.saber.devices.TheCPU.java file and the Makefile in the clib/ folder, to understand how we use the bind native method to bind our worker threads to cores, which otherwise would not be possible with plain Java code. The implementation of the bind() method is in the clib/CPU.c file. 


# Submission
The submission is performed through LabTS. 

Your implementation is restricted to modifying the operators and adding new class attributes and methods. You *must not* change any of the query implementations, all of which have names starting with "query" or "process" (there are comments in the code to guide you). If you are unsure if you can change a piece of code, ask.

In addition to your code submit a report describing your the findings you made during the task 1. Describe the performance impact of your reimplementation by comparing the operator's package performance profile before and after.

# Remarks

For building the project manually or from an IDE, add these flags to the VM:
-ea -server -XX:+UseConcMarkSweepGC -XX:NewRatio=2 -XX:SurvivorRatio=16 -Xms2g -Xmx2g

In SABER, all data structures and tuple sizes are a power of two. In addition, as we are referring to streaming data, the time dimension is a first class citizen and is always the first attribute of our input schema.  Thus, if we have an input schema with a timestamp and one integer value, we should always add padding like this:
inputSchema {
	long timestamp;	// 8 bytes
	int attr_1; 		// 4 bytes
	char padding[4]	// 4 bytes
}                   	// 16 bytes in total

We represent unique ids (user_id, page_id, ad_id, campaign_id) with 16 bytes integers. However, long long integers are not supported by Java, thus we have implemented our own class and we hash and compare the lower 64 bits and then the higher 64 ones for the HashJoin operator. Currently, for the hash join we use a hashtable that points to our actual data in a bytebuffer.

Regarding the execution of the benchmark, we decide to pin our workers to threads in order to improve our system's performance:
a thread is used for scheduling the tasks (1st core)
one for actual computation (2nd core)  <-----
one for fast data ingestion (3rd core)
one for continuous data generation (4rd core)
