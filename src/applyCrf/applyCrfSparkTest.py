#!/usr/bin/env python

"""This program will use Apache Spark to read a keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), optionally convert it to a pair RDD,
process it with CRF++, and print detected attributes as pair RDD keyed JSON
files, formatted to Karma's liking. The keys in the input file will be passed
through to the output file, but the text and tokens will not.

"""

import argparse
import datetime
import json
import sys
import time
from pyspark import SparkContext
import applyCrfSpark

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache', help="Optionally cache the RDD in memory.", required=False, action='store_true')
    parser.add_argument('--coalesceInput', type=int, default=0, help="Reduce the number of partitions on input.", required=False)
    parser.add_argument('--coalesceOutput', type=int, default=0, help="Reduce the number of partitions on output.", required=False)
    parser.add_argument('--count', help="Count the records before writing output.", required=False, action='store_true')
    parser.add_argument('-d','--debug', help="Give debugging feedback.", required=False, action='store_true')
    parser.add_argument('--download', help="Ask Spark to download the feature list and model files to the clients.", required=False, action='store_true')
    parser.add_argument('-e','--embedKey', help="Embed the key in the output.", required=False)
    parser.add_argument('-f','--featlist', help="Input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-k','--keyed', help="The input lines are keyed.", required=False, action='store_true')
    parser.add_argument('--hybridJaccardConfig', help="Configuration file for hybrid Jaccard processing.", required=False)
    parser.add_argument('-i','--input', help="Input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('--inputPairs', help="Test the paired input data processing path.", required=False, action='store_true')
    parser.add_argument('--inputSeq', help="Read input from a Hadooop SEQ data file.", required=False, action='store_true')
    parser.add_argument('-j','--justTokens', help="The input JSON line data is just tokens.", required=False, action='store_true')
    parser.add_argument('-m','--model', help="Input model file.", required=True)
    parser.add_argument('-o','--output', help="Output file of phrases in keyed JSON Lines format.", required=True)
    parser.add_argument('--outputCompressionClass', help="Compression class for text files.", required=False)
    parser.add_argument('--outputPairs', help="Test the paired output data processing path.", required=False, action='store_true')
    parser.add_argument('--outputSeq', help="Write output to a Hadooop SEQ data file.", required=False, action='store_true')
    parser.add_argument('--pairs', help="Test the paired data processing path.", required=False, action='store_true')
    parser.add_argument('-p', '--partitions', help="Number of partitions.", required=False, type=int, default=1)
    parser.add_argument('-s','--statistics', help="Report use statistics.", required=False, action='store_true')
    parser.add_argument('-t','--tags', help="Restrict the set of tags and optionally rename them: tagName,tagName:newTagName,...", required=False)
    parser.add_argument('-v','--verbose', help="Report progress.", required=False, action='store_true')
    parser.add_argument('-x','--extract', help="Name the field with text or tokens.", required=False)
    args = parser.parse_args()

    if args.verbose:
        print "========================================"
        print "Starting applyCrfSparkTest."
        print "========================================"

    # Open a Spark context:
    if args.verbose:
        print "========================================"
        print "Creating SparkContext."
        print "========================================"
        # TODO: Use time.monotonic() in python >= 3.3
        startTime = time.time() # Start timing here.

    sc = SparkContext()

    if args.verbose:
        print "========================================"
        print "SparkContext created. Application ID: "
        print sc.applicationId
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    #  Set up a CRF tagger object:
    tagger = applyCrfSpark.ApplyCrfSpark(args.featlist, args.model, args.hybridJaccardConfig,
                                         inputPairs=args.inputPairs or args.pairs or args.inputSeq,
                                         inputKeyed=args.keyed, inputJustTokens=args.justTokens,
                                         extractFrom=args.extract, tagMap=args.tags, embedKey=args.embedKey,
                                         outputPairs=args.outputPairs or args.pairs or args.outputSeq,
                                         debug=args.debug, sumStatistics=args.statistics)
    if args.verbose:
        print "========================================"
        print "CRF++ tagger created."
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.statistics:
        # Convert statistics to Spark accumulators:
        tagger.initializeSparkStatistics(sc)

    if args.download:
        # Ask Spark to download the feature list and model files from the
        # driver to the clients.
        tagger.requestSparkDownload(sc)

    minPartitions = args.partitions
    if minPartitions == 0:
        minPartitions = None

    # We'll accept three types of input files: a Sequence file, a text file
    # with tab-separated key and JSON Lines data, or a text file of JSON Lines
    # data (with the output field embedded as an entry in the top-level
    # dictionary).
    if args.inputSeq:
        # This is the primary input path.
        if args.verbose:
            print "========================================"
            print "Opening the input sequence file:"
            print args.input
            print "========================================"
        inputRDD = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text",  "org.apache.hadoop.io.Text",
                                   minSplits=minPartitions)
    else:
        if args.verbose:
            print "========================================"
            print "Opening the input text file:"
            print args.input
            print "========================================"
        inputRDD = sc.textFile(args.input, minPartitions)
        if args.inputPairs or args.pairs:
            if args.verbose:
                print "========================================"
                print "Converting the text lines into input pairs by splitting on tab."
                print "========================================"
            inputRDD = inputRDD.map(lambda s: s.split('\t', 1))

    if args.verbose:
        print "========================================"
        print "inputRDD is ready to read from the input file."
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    # Which is better? coalescing before processing or after processing?
    if args.coalesceInput > 0:
        numPartitions = inputRDD.getNumPartitions()
        if args.coalesceInput < numPartitions:
            if args.verbose:
                print "========================================"
                print "Coalescing partitions on input %d ==> %d" % (numPartitions, args.coalesceInput)
                print "========================================"
            inputRDD = inputRDD.coalesce(args.coalesceInput)
            if args.verbose:
                print "========================================"
                # TODO: use time.monotonic() in Python >= 3.3
                duration = time.time() - startTime
                print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
                print "========================================"

    if args.cache:
        print "========================================"
        print "Caching the input data."
        inputRDD.cache()
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.count:
        print "========================================"
        print "Counting records..."
        localRecordCount = inputRDD.count()
        print "Record count: %d" % localRecordCount
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    # Perform the main RDD processing.
    if args.verbose:
        print "========================================"
        print "Requesting CRF++ tagging"
        print "========================================"
    resultsRDD = tagger.perform(inputRDD)

    # Which is better? coalescing before processing or after processing?
    if args.coalesceOutput > 0:
        numPartitions = resultsRDD.getNumPartitions()
        if args.coalesceOutput < numPartitions:
            if args.verbose:
                print "========================================"
                print "Coalescing partitions on output %d ==> %d" % (numPartitions, args.coalesceOutput)
                print "========================================"
            resultsRDD = resultsRDD.coalesce(args.coalesceOutput)
            if args.verbose:
                print "========================================"
                # TODO: use time.monotonic() in Python >= 3.3
                duration = time.time() - startTime
                print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
                print "========================================"

    # The output will either be a Sequence file or a text file.  If
    # it's a text file, it might be a tab-separated pair file, or just
    # JSON Lines data.  In either case, the main RDD processing took
    # care of all necessary formatting.  Actually, it "will take
    # care", because it won't really be executed until the save
    # action, below, takes place.
    if args.outputSeq:
        if args.verbose:
            print "========================================"
            print "Transforming data and saving the result as a Hadoop SEQ file."
            print args.output
            print "========================================"
        resultsRDD.saveAsNewAPIHadoopFile(args.output,
                                          outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                                          keyClass="org.apache.hadoop.io.Text",
                                          valueClass="org.apache.hadoop.io.Text")
    else:
        if args.verbose:
            print "========================================"
            print "Transforming data and saving the result as a text file."
            print args.output
            print "========================================"
        # Paired results will be converted automatically.
        resultsRDD.saveAsTextFile(args.output,
                                  compressionCodecClass=args.outputCompressionClass)

    if args.statistics:
        print "========================================"
        tagger.showStatistics()
        print "========================================"

    if args.verbose:
        print "========================================"
        print "Ending applyCrfSparkTest."
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"
    sc.stop()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
