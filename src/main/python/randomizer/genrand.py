#!/usr/bin/python

import sys
import random
import getopt

optlist, args = getopt.getopt(sys.argv[1:], "", ["start=", "stop=", "count=", "int"])
optMap = dict(optlist)

numStart = float(optMap["--start"])
#print "Start:", numStart

numStop = float(optMap["--stop"])
#print "Stop:", numStop

count = int(optMap["--count"])
#print "Count:", count

makeInts = False
if ( "--int" in optMap ):
	makeInts = True
#print "Make Ints:", makeInts

vals = []

while ( len(vals) < count ):

	newVal = None

	if ( makeInts ):
		newVal = random.randrange(numStart, numStop)
	else:
		newVal = random.uniform(numStart, numStop)

	if ( newVal not in vals ):
		vals.append(newVal)

print " ".join(map(lambda x: str(x), sorted(vals)))