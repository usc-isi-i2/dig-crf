#!/usr/bin/env python

"""This program will use Apache Spark to read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file, formatted to Karma's liking. The
keys in the input file will be passed through to the output file, but the text
and tokens will not.

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
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    parser.add_argument('-o','--output', help="Required output file of phrases in keyed JSON Lines format.", required=True)
    parser.add_argument('-p','--partitions', help="Optional number of partitions.", required=False, type=int, default=1)
    parser.add_argument('-s','--statistics', help="Optionally report use statistics.", required=False, action='store_true')
    args = parser.parse_args()

    if args.debug:
        print "Starting applyCrfKjSparkTest."

    # Open a Spark context and set up a CRF tagger object.
    sc = SparkContext()
    tagger = applyCrf.ApplyCrfKj(args.featlist, args.model, args.debug, args.statistics)

    if args.download:
        # Ask Spark to download the feature list and model files from the
        # driver to the clients.  This request must take place in the driver.
        sc.addFile(args.featlist)
        sc.addFile(args.model)
        tagger.setFilePathMapper(sparkFilePathMapper)

    inputRDD = sc.textFile(args.input, args.partitions)
    resultsRDD = tagger.perform(inputRDD)
    resultsRDD.saveAsTextFile(args.output)

    if args.debug:
        print "Ending applyCrfKjSparkTest."

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
