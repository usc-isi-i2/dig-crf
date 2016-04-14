#!/usr/bin/env python

"""This program will use Apache Spark to read a keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), optionally convert it to a pair RDD,
process it with CRF++, and print detected attributes as pair RDD keyed JSON
files, formatted to Karma's liking. The keys in the input file will be passed
through to the output file, but the text and tokens will not.

"""

import argparse
import os
import sys
from pyspark import SparkContext, SparkFiles
import applyCrf

def sparkFilePathMapper(path):
    """When Spark forwards files from the driver to worker nodes, it may be necessary to map the filename path on a per-worker node basis."""
    return SparkFiles.get(os.path.basename(path))

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Optionally give debugging feedback.", required=False, action='store_true')
    parser.add_argument('--download', help="Optionally ask Spark to download the feature list and model files to the clients.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-k','--keyed', help="Optional: the input lines are keyed.", required=False, action='store_true')
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-j','--justTokens', help="Optional: the input JSON line data is just tokens.", required=False, action='store_true')
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    parser.add_argument('-o','--output', help="Required output file of phrases in keyed JSON Lines format.", required=True)
    parser.add_argument('--inputPairs', help="Optionally test the paired input data processing path.", required=False, action='store_true')
    parser.add_argument('--outputPairs', help="Optionally test the paired output data processing path.", required=False, action='store_true')
    parser.add_argument('--pairs', help="Optionally test the paired data processing path.", required=False, action='store_true')
    parser.add_argument('-p', '--partitions', help="Optional number of partitions.", required=False, type=int, default=1)
    parser.add_argument('-s','--statistics', help="Optionally report use statistics.", required=False, action='store_true')
    args = parser.parse_args()

    if args.debug:
        print "Starting applyCrfSparkTest."

    # Open a Spark context and set up a CRF tagger object.
    sc = SparkContext()
    tagger = applyCrf.ApplyCrf(args.featlist, args.model,
                               inputPairs=args.inputPairs or args.pairs,
                               inputKeyed=args.keyed, inputJustTokens=args.justTokens,
                               outputPairs=args.outputPairs or args.pairs,
                               debug=args.debug, showStatistics=args.statistics)

    if args.download:
        # Ask Spark to download the feature list and model files from the
        # driver to the clients.  This request must take place in the driver.
        sc.addFile(args.featlist)
        sc.addFile(args.model)
        tagger.setFilePathMapper(sparkFilePathMapper)

    inputRDD = sc.textFile(args.input, args.partitions)
    if args.inputPairs or args.pairs:
        # Convert the input text file into input pairs:
        inputRDD = inputRDD.map(lambda s: s.split('\t', 1))
    resultsRDD = tagger.perform(inputRDD)
    resultsRDD.saveAsTextFile(args.output) # Paired results will be converted automatically.

    if args.debug:
        print "Ending applyCrfSparkTest."

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
