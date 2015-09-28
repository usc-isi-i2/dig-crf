#!/usr/bin/python -i

import sys, base64, datetime

inPaths = sys.argv[1:]

outPath = "crf_input_%s" % str(datetime.datetime.utcnow()).replace(' ','-').replace(':','-').replace('.','-')

with open(outPath, 'w') as outFile:
    for inPath in inPaths:
        with open(inPath, 'r') as inFile:
            for line in inFile:
                decoded = base64.b64decode(line)
                print >> outFile, decoded

            
