#!/usr/local/bin/gnuplot

# Redirect print statements to stdout
set print "-"

# Default data directories
fig01_  = "data/original/figure-01/"
fig07_  = "data/original/figure-07/"
fig08_  = "data/original/figure-08/"
fig09_  = "data/original/figure-09/"
fig10a_ = "data/original/figure-10/selection/"
fig10b_ = "data/original/figure-10/join/"
fig11a_ = "data/original/figure-11/selection/"
fig11b_ = "data/original/figure-11/aggregation/"
fig12a_ = "data/original/figure-12/selection/"
fig12b_ = "data/original/figure-12/group-by/"
fig12c_ = "data/original/figure-12/join/"
fig13a_ = "data/original/figure-13/selection/tumbling-window/1-1/"
fig13b_ = "data/original/figure-13/selection/sliding-window/1024-1/"
fig13c_ = "data/original/figure-13/selection/tumbling-window/1024-1024/"
fig14_  = "data/original/figure-14/"
fig15_  = "data/original/figure-15/"
fig16_  = "data/original/figure-16/"

# PCIe throughput line for figure 13a
fig13p_ = "data/original/figure-13/pcie/"

# Set data directories, unless overridden
if (! exists("fig01" )) fig01  = fig01_
if (! exists("fig07" )) fig07  = fig07_
if (! exists("fig08" )) fig08  = fig08_
if (! exists("fig09" )) fig09  = fig09_
if (! exists("fig10a")) fig10a = fig10a_
if (! exists("fig10b")) fig10b = fig10b_
if (! exists("fig11a")) fig11a = fig11a_
if (! exists("fig11b")) fig11b = fig11b_
if (! exists("fig12a")) fig12a = fig12a_
if (! exists("fig12b")) fig12b = fig12b_
if (! exists("fig12c")) fig12c = fig12c_
if (! exists("fig13a")) fig13a = fig13a_
if (! exists("fig13b")) fig13b = fig13b_
if (! exists("fig13c")) fig13c = fig13c_
if (! exists("fig14" )) fig14  = fig14_
if (! exists("fig15" )) fig15  = fig15_
if (! exists("fig16" )) fig16  = fig16_

# Set data for PCIe throughput line
if (! exists("fig13p")) fig13p = fig13p_

# Set reproducibility version
if (! exists("ver")) ver = "0.0 (original)"

print "#"
print "# Gnuplot reproducibility version ", ver
print "#"

# Set output directory
bin="bin/eps/"

set terminal postscript eps color enhanced "Helvetica" 32
set key font ",24"
set size 1.3, 1
set border lw 2

set macros

#####

CLEAR="\
unset boxwidth;   \
unset style fill; \
unset log  x;     \
unset log  y;     \
unset log y2;     \
unset xrange;     \
unset yrange;     \
unset y2range;    \
unset xlabel;     \
unset ylabel;     \
unset y2label;    \
unset xtics;      \
set   xtics;      \
unset ytics;      \
set   ytics;      \
unset y2tics;     \
set terminal postscript eps color enhanced \"Helvetica\" 32"

TwoInOneColumnFont="\
set terminal postscript eps color enhanced \"Helvetica\" 48; \
set key font \",32\""

ThreeInTwoColumnsFont="\
set terminal postscript eps color enhanced \"Helvetica\" 40; \
set key font \",30\""

##### LINE STYLES

set style line  1 lt -1 lw 4 lc rgb "grey"           # Percentiles
set style line  2 lt -1 lw 6 lc rgb "grey"           # Median
set style line  3 lt -1 lw 3 pt 2 ps 3               # Throughput (generic, hybrid)
set style line  4 lt -1 lw 6 pt 3 ps 3 dashtype 2    # Throughput (cpu-only)
set style line  5 lt -1 lw 6 pt 1 ps 3 dashtype 3    # Throughput (gpu-only)
set style line  6 lt -1 lw 3 lc rgb "grey"           # PCIe throughput
set style line  7 lt -1 lw 1 lc rgb "grey40"         # Latency (hybrid)
set style line 10 lt -1 lw 2 pt 2 ps 3               # Generic y-error bars
set style line 11 lt -1 lw 2 pt 0 ps 0 lc rgb "grey" # Throughput y-error bars

percentilesStyle=1
hybridLatencyPattern=1

medianStyle=2

genericThroughputStyle=3

hybridThroughputStyle=genericThroughputStyle
cpuThroughputStyle=4
gpuThroughputStyle=5

pciThroughputStyle=6

hybridLatencyStyle=7

errorbarStyle=10
throughputErrorbarsStyle=11


##### BOX STYLES

esperBox="fs pattern 0 lt -1 lw 2"
sparkBox="fs pattern 0 lt -1 lw 2"

saberBox="fs solid 1 lt -1 lw 2"
saberCpuBox="fs solid 0 lt -1 lw 2"
saberGpuBox="fs solid 0.5 lt -1 lw 2"

fcfsBox="fs pattern 0 lt -1 lw 2"
staticBox="fs solid 0.5 lt -1 lw 2"
hlsBox="fs pattern 3 lt -1 lw 2"

##### TITLES

# (X,Y)-AXIS LABELS

tupleThroughput="Throughput (10^{6} tuples/s)"

sparkSlide="Window slide (10^{6} tuples)"
sparkThroughput=tupleThroughput

gbps="Throughput (GB/s)"

selectVariable="Number of Predicates (n)"
joinVariable="Number of Predicates (r)"

batchSize1="Query Task Size (KB)"
slideSize1="Window Slide Size (KB)"

latencySec="Latency (sec)"

numWorkers="Number of Worker Threads"
selectivity="Selectivity"
timeSec="Time (s)"
workloads="Workloads"

# LINE & BOX TITLES

esper="Esper"
sparkStreaming="Spark Streaming"

saberCpu="Saber (CPU only)"
saberGpu="Saber (GPGPU only)"
saberGpuContrib="Saber (GPGPU contrib.)"
saber="Saber"

saberLatency="Saber latency"

pcie="PCIe Bus"

fcfsScheduler="FCFS"
staticScheduler="Static"
hlsScheduler="HLS"

##### PLOTS

# Figure 1
#
# Performance of a streaming GROUP-BY query with a 5-second window 
# and different window slides under Spark Streaming
#
print "# Plot Figure  1  with data from ", fig01

set output bin."spark-motivation.eps"

set yrange[0:2.1]
set xrange[0:9]

set xlabel sparkSlide
set ylabel sparkThroughput

norm=1000000

plot \
fig01."/normalised-throughput.dat" \
using ($1/norm):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1/norm):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1/norm):($3/norm) with linespoints \
ls genericThroughputStyle \
notitle

@CLEAR

# 
# Figure 7
#
# Performance of application benchmark queries
#
print "# Plot Figure  7  with data from ", fig07

set output bin."comparisons.eps"

B1="CM_{1}"
B2="CM_{2}"
B3="SG_{1}"
B4="SG_{2}"
B5="SG_{3}"
B6="LRB_{1}"
B7="LRB_{2}"
B8="LRB_{3}"
B9="LRB_{4}"

cmTupleSize=64
sgTupleSize=32
lrbTupleSize=32

set yrange[0:60]
set xrange[0:18]

set ylabel tupleThroughput
unset xlabel

set xtics \
(B1 1, B2 3, B3 5, B4 7, B5 9, B6 11, B7 13, B8 15, B9 17) \
font ",26"

set key top left
set key font ",28"

boxOffset=0.25
set boxwidth (2 * boxOffset)

million=1000000

# The y-axis in the data set is in MB/s. 
# So, divide $2 by (1048576 x |tuple|).

plot \
fig07."comparison-boxes-tuples.dat" \
using ($1-boxOffset):($2/million) with boxes \
@esperBox \
title esper, \
\
fig07."comparison-boxes-tuples.dat" \
using ($1+boxOffset):($3/million) with boxes \
@saberBox \
title saber, \
\
fig07."comparison-boxes-tuples.dat" \
using ($1+boxOffset):($4/million) with boxes \
@saberGpuBox \
title saberGpuContrib, \
\
fig07."comparison-boxes-tuples.dat" \
using ($1+boxOffset):($3/million):(sprintf("%.0f\nMB/s", $5)) with labels \
offset 0,1.1 font ",21" \
notitle

@CLEAR

# 
# Figure 8
#
# Performance of synthetic queries. `AGG*` evaluates all aggregate 
# functions and GPOUP-BY evaluates `count` and `sum`.
#
print "# Plot Figure  8  with data from ", fig08

set output bin."hybrid-operators.eps"

set multiplot

Syn1="PROJ_{4}"
Syn2="SELECT_{16}"
Syn3="AGG_{*}"  
Syn4="GROUP-BY_{8}"
Syn5="JOIN_{1}"

set size 1,1
set origin 0,0

set yrange[0:8]
set xrange[0:8]

set ylabel gbps
set xtics (Syn1 1, Syn2 3, Syn3 5, Syn4 7) font ",28" 

set key font ",28"
set key top left

boxOffset=0.3

set boxwidth boxOffset
set style fill solid border -1

plot \
fig08."hybrid-operator-boxes.dat" \
using ($1-boxOffset):($2/1024) with boxes \
@saberCpuBox \
title saberCpu, \
\
fig08."hybrid-operator-boxes.dat" \
using ($1):($3/1024) with boxes \
@saberGpuBox \
title saberGpu, \
\
fig08."hybrid-operator-boxes.dat" \
using ($1+boxOffset):($4/1024) with boxes \
@saberBox \
title saber

# second sub-plot

set size 0.45,1
set origin 0.85,0

unset key
unset ylabel

set yrange[0:0.4]
set ytics 0,0.1,0.4 offset 0.65

set xrange[8:10]
set xtics (Syn5 9) font ",28"

plot \
fig08."hybrid-operator-boxes.dat" \
using ($1-boxOffset):($2/1024) with boxes \
@saberCpuBox \
title saberCpu, \
\
fig08."hybrid-operator-boxes.dat" \
using ($1):($3/1024) with boxes \
@saberGpuBox \
title saberGpu, \
\
fig08."hybrid-operator-boxes.dat" \
using ($1+boxOffset):($4/1024) with boxes \
@saberBox \
title saber

unset size
unset origin

unset multiplot

@CLEAR
#
# Reset size
set size 1.3,1

# 
# Figure 9
# 
# Performance compared to Spark Streaming
#
print "# Plot Figure  9  with data from ", fig09

set output bin."spark-comparisons.eps"

set size 1,0.73

set ylabel sparkThroughput font ",30"
set xtics ("CM_{1}" 1, "CM_{2}" 3, "SG_{1}" 5) font ",32"

set yrange[0:40]
set ytics 0,10,40 font ",32"

set xrange[0:6]

set key top left
set key font ",32"

norm=1000000
boxOffset=0.15

set boxwidth (2 * boxOffset)

plot \
fig09.'spark-tuples.dat' \
using ($1-boxOffset):($3/norm) with boxes \
@sparkBox \
title sparkStreaming, \
'' \
using ($1-boxOffset):($3/norm):($10/norm) with yerrorbars \
ls errorbarStyle \
notitle, \
\
fig09.'saber-tuples.dat' \
using ($1+boxOffset):($3/norm) with boxes \
@saberBox \
title saber, \
'' \
using ($1+boxOffset):($3/norm):($10/norm) with yerrorbars \
ls errorbarStyle \
notitle

@CLEAR
# 
# Reset size
set size 1.3,1

# 
# Figure 10
# 
# Performance impact of query parameters
#
# print "# Plot Figure 10 with data from ", fig10

set size 1.3,1.15
#
# a) select
#
print "# Plot Figure 10a with data from ", fig10a

@TwoInOneColumnFont

set output bin."select-complexity.eps"

set ylabel gbps offset 1.5
set xlabel selectVariable 

set xrange[1:64]
set log x 2
set yrange[0:10]
set ytics 0,2,10

set key top right
set key font ",42"

norm=1024

plot \
fig10a.'cpu.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig10a.'gpu.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu,\
\
fig10a.'hybrid.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber

@CLEAR

# 
# b) join
#
print "# Plot Figure 10b with data from ", fig10b

@TwoInOneColumnFont

set output bin."join-complexity.eps"

set key top right
set key font ",42"

set xrange[1:64]
set log x 2
set yrange[0:0.5]
set ytics 0,0.1,0.5

set ylabel gbps offset 1.5
set xlabel joinVariable

norm=1024

plot \
fig10b.'cpu.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig10b.'gpu.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7):($7):($7):($7) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
fig10b.'hybrid.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber

@CLEAR
# 
# Reset size
set size 1.3,1 

# Figure 11
# 
# Performance impact of window slide
#
# print "# Plot Figure 11 with data from ", fig11

set size 1.4,1.15

#
# a) select
#
print "# Plot Figure 11a with data from ", fig11a

@TwoInOneColumnFont

set output bin."select-sliding-window.eps"

set key top left
set key font ",38"

set ylabel gbps offset 1.5 
set xlabel slideSize1

set xrange[0.015625:64]
set log x 2
set xtics (0.0625,0.5,2,8,32) font ",44"
set yrange[0:12]
set ytics 0,2,10

# Y2-axis
set y2label latencySec offset -2.5
set ytics nomirror
set y2range [0:0.2]
set y2tics 0,0.1,0.2 offset -0.75

set boxwidth 0.2
set style fill pattern

norm=1024
norm1=1000

plot \
fig11a.'hybrid-throughput.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber, \
\
fig11a.'cpu-throughput.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig11a.'gpu-throughput.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
\
fig11a.'hybrid-latency.dat' \
using ($1/1024):($6/norm1):($5/norm1):($9/norm1):($8/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
title saberLatency whiskerbars, \
'' \
using ($1/1024):($7/norm1):($7/norm1):($7/norm1):($7/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
notitle

@CLEAR

# 
# b) aggregation
#
print "# Plot Figure 11b with data from ", fig11b

@TwoInOneColumnFont

set output bin."agg-avg-sliding-window.eps"

set key top left
set key font ",38"

set ylabel gbps offset 1.5
set xlabel slideSize1

set xrange[0.015625:64]
set log x 2
set xtics (0.0625,0.5,2,8,32) font ",42"
set yrange[0:12]
set ytics 0,2,10

# Y2-axis
set y2label latencySec offset -2.2
set ytics nomirror
set y2range [0:0.4]
set y2tics 0,0.1,0.4 offset -0.75

set boxwidth 0.2
set style fill pattern

norm=1024
norm1=1000

plot \
fig11b.'hybrid-throughput-avg.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber, \
\
fig11b.'cpu-throughput-avg.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig11b.'gpu-throughput-avg.dat' \
using ($1/1024):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1/1024):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
\
fig11b.'hybrid-latency-avg.dat' \
using ($1/1024):($6/norm1):($5/norm1):($9/norm1):($8/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
title saberLatency whiskerbars, \
'' \
using ($1/1024):($7/norm1):($7/norm1):($7/norm1):($7/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
notitle

@CLEAR
# 
# Reset size
set size 1.3,1

#
# Figure 12
# 
# Performance impact of query task size for different query types
#
# print "# Plot Figure 12 with data from ", fig12

#
# a) Selection
#
print "# Plot Figure 12a with data from ", fig12a

@ThreeInTwoColumnsFont

set output bin."select-task-size.eps"

set key top left

set ylabel gbps offset 1.5
set xlabel batchSize1

set xrange[32:8192]
set log x 2
set yrange[0:10]

# Y2-axis
set y2label latencySec offset -1.5
set ytics nomirror
set y2range [0:0.4]
set y2tics 0,0.1,0.4

set xtics 64,4,4096

set boxwidth 0.2
set style fill pattern

norm=1024
norm1=1000

plot \
fig12a.'hybrid-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber, \
\
fig12a.'cpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig12a.'gpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
\
fig12a.'hybrid-latency.dat' \
using ($1):($6/norm1):($5/norm1):($9/norm1):($8/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
title saberLatency whiskerbars, \
'' \
using ($1):($7/norm1):($7/norm1):($7/norm1):($7/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
notitle

@CLEAR

# 
# d) Aggregation w/group-by
#
print "# Plot Figure 12b with data from ", fig12b

@ThreeInTwoColumnsFont

set output bin."groupby-task-size.eps"

set key top left

set ylabel gbps offset 1.5
set xlabel batchSize1

set xrange[32:8192]
set log x 2
set yrange[0:10]

# Y2-axis
set y2label latencySec offset -1.5
set ytics nomirror
set y2range [0:0.4]
set y2tics 0,0.1,0.4

set xtics 64,4,4096

set boxwidth 0.2
set style fill pattern

norm=1024
norm1=1000

plot \
fig12b.'hybrid-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber, \
\
fig12b.'cpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig12b.'gpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
\
fig12b.'hybrid-latency.dat' \
using ($1):($6/norm1):($5/norm1):($9/norm1):($8/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
title saberLatency whiskerbars, \
'' \
using ($1):($7/norm1):($7/norm1):($7/norm1):($7/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
notitle

@CLEAR

# 
# e) Join
#
print "# Plot Figure 12c with data from ", fig12c

@ThreeInTwoColumnsFont

set output bin."join-task-size.eps"

set key top left

set ylabel gbps offset 1.5
set xlabel batchSize1

set xrange[32:8192]
set log x 2
set yrange[0:0.4]

set xtics 64,4,4096

set ytics 0,0.1,0.4

# Y2-axis
set y2label latencySec offset -1
set ytics nomirror
set y2range [0:4]
set y2tics 0,1,4

set boxwidth 0.2
set style fill pattern

plot \
fig12c.'hybrid-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls hybridThroughputStyle \
title saber, \
\
fig12c.'cpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig12c.'gpu-throughput.dat' \
using ($1):($3/norm):($10/norm) with yerrorbars \
ls throughputErrorbarsStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
\
fig12c.'hybrid-latency.dat' \
using ($1):($6/norm1):($5/norm1):($9/norm1):($8/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
title saberLatency whiskerbars, \
'' \
using ($1):($7/norm1):($7/norm1):($7/norm1):($7/norm1) with candlesticks axes x1y2 \
fs pattern hybridLatencyPattern ls hybridLatencyStyle \
notitle

@CLEAR

#
# Figure 13
# 
# Performance impact of query task size for different window sizes 
# and slides
#
# print "# Plot Figure 13 with data from ", fig13

#
# a) select, window size 1, slide 1
#
print "# Plot Figure 13a with data from ", fig13a

@ThreeInTwoColumnsFont

set output bin."select-task-size-window-1-1.eps"

set key top left

set ylabel gbps offset 1.5
set xlabel batchSize1

set xrange[0.015625:8192]
set log x 2
set xtics 1,8,4096
set yrange[0:10]
set ytics 0,2,10

# Y2-axis
set y2label "null" offset -0.5 textcolor rgb "white"
set y2range [0:0.1]
set y2tics 0,0.4,0.4 textcolor rgb "white"

norm=1024

plot \
fig13a.'cpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig13a.'gpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu, \
\
fig13p.'pci-input.dat' \
using ($1):($3/norm) with lines \
ls pciThroughputStyle \
title pcie

@CLEAR

# 
# b) select, window size 1024, slide 1
#
print "# Plot Figure 13b with data from ", fig13b

@ThreeInTwoColumnsFont

set output bin."select-task-size-window-1024-1.eps"

set key top left

set ylabel gbps offset 1.5
set xlabel batchSize1

set xrange[16:8192]
set log x 2
set xtics 64,4,4096
set yrange[0:10]
set ytics 0,2,10

# Y2-axis
set y2label "null" offset -0.5 textcolor rgb "white"
set y2range [0:0.1]
set y2tics 0,0.4,0.4 textcolor rgb "white"

norm=1024

plot \
fig13b.'cpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig13b.'gpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu

@CLEAR

# 
# c) select, window slide 1024, slide 1024
#
print "# Plot Figure 13c with data from ", fig13c

@ThreeInTwoColumnsFont

set output bin."select-task-size-window-1024-1024.eps"

set key top left

set ylabel gbps offset 1.0
set xlabel batchSize1

set xrange[16:8192]
set log x 2
set xtics 64,4,4096
set yrange[0:10]
set ytics 0,2,10

# Y2-axis
set y2label "null" offset -1.0 textcolor rgb "white"
set y2range [0:0.1]
set y2tics 0,0.4,0.4 textcolor rgb "white"

norm=1024

plot \
fig13c.'cpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls cpuThroughputStyle \
title saberCpu, \
\
fig13c.'gpu-throughput.dat' \
using ($11/1024):($6/norm):($5/norm):($9/norm):($8/norm) with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($11/1024):($7/norm):($7/norm):($7/norm):($7/norm) with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($11/1024):($3/norm) with linespoints \
ls gpuThroughputStyle \
title saberGpu

@CLEAR

# 
# Figure 14
#
# Scalability of CPU operator implementation (project)
#
print "# Plot Figure 14  with data from ", fig14

@TwoInOneColumnFont

# a) Projection

set output bin."project-scalability.eps"

set ylabel gbps 
set xlabel numWorkers

set xrange[1:32]
set log x 2
set xtics (1,2,4,8,16,32)
set yrange[0:5]
# set log y 2
set ytics 0,1,5

norm=1024

plot \
fig14.'cpu-scalability.dat' \
using ($1):($6/norm):($5/norm):($9/norm):($8/norm) \
with candlesticks \
ls percentilesStyle \
notitle whiskerbars, \
'' \
using ($1):($7/norm):($7/norm):($7/norm):($7/norm) \
with candlesticks \
ls medianStyle \
notitle, \
'' \
using ($1):($3/norm) with linespoints \
ls genericThroughputStyle \
notitle

@CLEAR

# 
# Figure 15
# 
# Performance impact of HLS scheduling
#
print "# Plot Figure 15  with data from ", fig15

@TwoInOneColumnFont

set output bin."scheduling.eps"

set yrange[0:5]
set xrange[0:3]

set ylabel gbps
set xlabel workloads
set xtics ("W_{1}" 1, "W_{2}" 2)

set key top left
set key font ",38"

boxOffset=0.2

set box boxOffset
set style fill solid border -1

plot \
fig15."fcfs.dat" \
using ($1-boxOffset):($2/1024) with boxes \
@fcfsBox \
title fcfsScheduler,\
fig15."fcfs.dat" \
using ($1-boxOffset):($2/1024):($3/1024) with yerrorbars \
ls errorbarStyle \
notitle,\
\
fig15."static.dat" \
using ($1+0.0):($2/1024) with boxes \
@staticBox \
title staticScheduler, \
fig15."static.dat" \
using ($1+0.0):($2/1024):($3/1024) with yerrorbars \
ls errorbarStyle \
notitle, \
\
fig15."hls.dat" \
using ($1+boxOffset):($2/1024) with boxes \
@hlsBox \
title hlsScheduler, \
fig15."hls.dat" \
using ($1+boxOffset):($2/1024):($3/1024) with yerrorbars \
ls errorbarStyle \
notitle

@CLEAR

# 
# Figure 16
# 
# Adaptation of HLS scheduling
#
print "# Plot Figure 16  with data from ", fig16

set output bin."hlsadaptivity-with-google-cluster-data.eps"

set multiplot # layout 2,1

unset origin

set key font ",24"
set border lw 2
set key top left

set size 1.3,0.4
set origin 0,0.6

unset xlabel
set xtics format ""

set xrange [0:30]

set ylabel selectivity offset 1.7 font ",28"

set yrange [0:0.2]
set ytics 0,0.1,0.5 font ",28"

plot fig16."adaptivity.dat" using ($1/2):($4) \
with lines lt 1 lw 2 lc rgb "black" notitle

set size 1.3,0.67
set origin 0,0

set xlabel timeSec font ",28"
set xtics format "%g"
set xtics 0,5,30 font ",28"

set ylabel gbps offset -0.3 font ",28"
set yrange [0:8]
set ytics 0,2,10 font ",28"

plot \
fig16."adaptivity.dat" using ($1/2):($3/1024) \
with boxes @saberGpuBox title saberGpuContrib, \
"" \
using ($1/2):($2/1024) with linespoints \
ls genericThroughputStyle lw 2 ps 1 pt 71 \
title saber

unset multiplot

@CLEAR

# END OF SCRIPT
