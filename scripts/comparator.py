#
# Compare measurements
#

import os

import sys
import numpy as np

import argparse

verbose = False

#
# Returns the (n = offset)th value on line whose first
# value is `key`.
#
def extractValue (filename, key, offset):
	f = open(filename, "r")
	lines = 0
	for line in f:
		lines += 1
		s = line.split()
		if key == s[0].strip():
			# Key found
			if verbose is True:
				print "# Key found on l.", lines
			try:
				v = float(s[offset])
				return  v
			except:
				print >>sys.stderr, "error: invalid offset:", offset
				return -1
	# End for
	print >>sys.stderr, "error: key not found:", key
	return -1

def compare (x, y):
	try:
		z = (float(x - y) / float(y)) * 100.
		return  z
	except:
		print >>sys.stderr, "error: failed to compare", x, "with", y
		sys.exit(1)

if __name__ == "__main__":
	#
	# Check and parse command-line arguments
	#
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-x', type = float, required = True)
	parser.add_argument('-v', action = "store_true")
	parser.add_argument('-o', type = int, choices = xrange(1,10), required = True)
	parser.add_argument('-k', type = str, required = True)
	parser.add_argument('-f', type = str, required = True)
	
	args = parser.parse_args()
	
	if not os.path.exists(args.f):
		print >>sys.stderr, "error: file %s not found" % (args.f)
		sys.exit(1)
	
	verbose = args.v
	
	y = extractValue (args.f, args.k, args.o)
	
	if y < 0:
		sys.exit(1)
	
	z = compare (args.x, y)
	
	print "%.2f" % (z)
	sys.exit(0)
	
