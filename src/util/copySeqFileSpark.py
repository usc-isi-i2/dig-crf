#!/usr/bin/env python

"""This program will use Apache Spark to read a Sequence file and
write it to a new Sequence file.
"""

import argparse
import datetime
import sys
import time
from pyspark import SparkContext

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--coalesce', type=int, default=0, help="Reduce the number of partitions on input.", required=False)
    parser.add_argument('--count', help="Count the records.", required=False, action='store_true')
    parser.add_argument('-i','--input', help="Input file.", required=True)
    parser.add_argument('-o','--output', help="Output file.", required=True)
    parser.add_argument('-p','--partitions', help="Number of partitions.", required=False, type=int, default=1)
    parser.add_argument('-s','--showPartitions', help="Show the number of partitions.", required=False, action='store_true')
    parser.add_argument('-t','--time', help="Report execution time.", required=False, action='store_true')
    parser.add_argument('-v','--verbose', help="Report progress.", required=False, action='store_true')
    args = parser.parse_args()


    minPartitions = args.partitions
    if minPartitions == 0:
        minPartitions = None
    
    if args.verbose:
        print "========================================"
        print "Starting copySeqFileSpark."
        print "input file: " + args.input
        print "========================================"

    # Open a Spark context.
    if args.time:
        # TODO: Use time.monotonic() in python >= 3.3
        startTime = time.time() # Start timing here.
    sc = SparkContext()

    if args.verbose:
        print "========================================"
        print "SparkContext created. Application ID: "
        print sc.applicationId
        print "========================================"

    inputRDD = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text",  "org.apache.hadoop.io.Text",
                               minSplits=minPartitions)

    if args.showPartitions:
        print "========================================"
        print "There are %d partitions" % inputRDD.getNumPartitions()
        print "========================================"

    if args.coalesce > 0:
        numPartitions = inputRDD.getNumPartitions()
        if args.coalesce < numPartitions:
            if args.verbose:
                print "========================================"
                print "Coalescing partitions on input %d ==> %d" % (numPartitions, args.coalesce)
                print "========================================"
            inputRDD = inputRDD.coalesce(args.coalesce)
        else:
            if args.verbose:
                print "========================================"
                print "Not coalescing partitions on input (%d existing <= %d requested)" % (numPartitions, args.coalesce)
                print "========================================"

    if args.count:
        print "========================================"
        print "Counting records..."
        recordCount = inputRDD.count()
        print "Record count: %d" % recordCount
        print "========================================"

    if args.time:
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "========================================"
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.verbose:
        print "========================================"
        print "Saving data as a Hadoop SEQ file."
        print "output file: " + args.output
        print "========================================"
        inputRDD.saveAsNewAPIHadoopFile(args.output,
                                        outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                                        keyClass="org.apache.hadoop.io.Text",
                                        valueClass="org.apache.hadoop.io.Text")
    if args.verbose:
        print "========================================"
        print "Stopping Spark."
        print "========================================"
    sc.stop()

    if args.time:
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "========================================"
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.verbose:
        print "========================================"
        print "Ending copySeqFileSpark."
        print "========================================"

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
