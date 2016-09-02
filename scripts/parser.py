#
# Parse measurements
#

import os

import sys
import numpy as np

import argparse

verbose = False

def getInputThroughput (measurements, index):
	mbps = []
	for line in measurements:
		if not line.startswith("[MON]"):
			continue
		s = line.split()
		# Find index of input throughput based
		# on the first occurence of "MB/s"
		for ndx in range(len(s)):
			token = s[ndx]
			if token == "MB/s":
				mbps.append(float(s[ndx - 1]))
				break
	
	if verbose is True:
		print "#", len(mbps), "input throughput measurements"
	
	values = [0. for i in range(9)]
	
	if len(mbps) > 0:
		values[0] = np.min(mbps)
		values[1] = np.mean(mbps)
		values[2] = np.max(mbps)
		values[3] = np.percentile(mbps,  5)
		values[4] = np.percentile(mbps, 25)
		values[5] = np.percentile(mbps, 50)
		values[6] = np.percentile(mbps, 75)
		values[7] = np.percentile(mbps, 95)
		values[8] = np.std(mbps)
	
	if index <= 0:
		# Print all
		print \
		values[0], \
		values[1], \
		values[2], \
		values[3], \
		values[4], \
		values[5], \
		values[6], \
		values[7], \
		values[8]
	else:
		# Print specific value
		print values[index - 1]
		
	return 0

def getLatency (measurements, index):
	
	values = [0. for i in range(9)]
	# Indices
	_min = 0
	_avg = 1
	_max = 2
	_p05 = 3
	_p25 = 4
	_p50 = 5
	_p75 = 6
	_p99 = 7
	_std = 8
	
	count = 0
	
	for line in measurements:
		
		if not line.startswith("[MON]"):
			continue
		
		# Measurement line
		s = line.split()
		
		if s[1] == "[LatencyMonitor]":
			if s[3] == "measurements":
				count = int(s[2])
			else:
				#
				# parse percentiles
				#
				for ndx in range(len(s)):
					token = s[ndx]
					if token == "5th":
						values[_p05] = float(s[ndx + 1])
					elif token == "25th":
						values[_p25] = float(s[ndx + 1])
					elif token == "50th":
						values[_p50] = float(s[ndx + 1])
					elif token == "75th":
						values[_p75] = float(s[ndx + 1])
					elif token == "99th":
						values[_p99] = float(s[ndx + 1])
		else:
			#
			# parse [avg value
			# parse  min value
			# parse  max value]
			#
			for ndx in range(len(s)):
				token = s[ndx]
				if token == "[avg":
					values[_avg] = float(s[ndx + 1])
				elif token == "min":
					values[_min] = float(s[ndx + 1])
				elif token == "max":
					values[_max] = float(s[ndx + 1][:-1]) # Remove trailing ']'
	#
	# Check values
	#
	
	if verbose is True:
		print "#", count, "latency measurements"

	if index <= 0:
		# Print all
		print \
		values[0], \
		values[1], \
		values[2], \
		values[3], \
		values[4], \
		values[5], \
		values[6], \
		values[7], \
		values[8]
	else:
		# Print specific value
		print values[index - 1]
	
	return 0

def process (filename):
	measurements = []
	f = open(filename, "r")
	lines = 0
	for line in f:
		lines += 1
		if not line.startswith("[MON]"):
			continue
		measurements.append(line.strip())
	# End for
	
	if verbose is True:
		print "#", lines, "lines processed,", len(measurements), "measurements"
	
	return measurements

if __name__ == "__main__":
	#
	# Check and parse command-line arguments
	#
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-t', action = "store_true")
	parser.add_argument('-l', action = "store_true")
	parser.add_argument('-v', action = "store_true")
	parser.add_argument('-x', type = int, choices = xrange(0,10), default = -1)
	parser.add_argument('-f', type = str)
	
	args = parser.parse_args()
	
	if not args.x < 0:
		if args.t is True and args.l is True:
			print >>sys.stderr, "error: when -x is set, either -t or -l must be set, not both"
			sys.exit(1)
	
	if args.f is None:
		print >>sys.stderr, "error: no file specified"
		sys.exit(1)
		
	if not os.path.exists(args.f):
		print >>sys.stderr, "error: file %s not found" % (args.f)
		sys.exit(1)
	
	verbose = args.v
	
	measurements = process (args.f)
	
	if args.t is True:
		getInputThroughput (measurements, args.x)
	
	if args.l is True:
		getLatency (measurements, args.x)
	
	sys.exit(0)
	
	# print args.x
	# print args.t
	# print args.l
	# print args.f
	# filename = sys.argv[1]
	# sys.exit(process(filename))
