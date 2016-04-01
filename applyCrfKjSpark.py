#!/usr/bin/env python

"""This program will read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file, formatted to Karma's liking. The
keys in the input file will be passed through to the output file, but the
text and tokens will not.

"""

import argparse
import crf_sentences as crfs
import os
import sys
from pyspark import SparkContext
import applyCrfKj

class ApplyCrfKjSpark:
    def __init__(self, featureListFilePath, modelFilePath, debug, statistics):
        """Initialialize ApplyCrfKjSpark.

Note: we cannot initialize ApplyCrfKj at this time, because that ties to the
CRF++ code, which leads to a pickling error.

        """
        self.featureListFilePath = featureListFilePath
        self.modelFilePath = modelFilePath
        self.debug = debug
        self.statistics = statistics

    def process(self, sourceRDD): 
        """This is a filter for mapPartitions.  It takes an iterator as a source (RDDs
are iterators) and returns an iterator (more precisely, a generator).  It
maintains an iterator strategy internally, and avoids re-buffering the input
and output.

        """
        # Return a generator that takes the source iterator (an RDD) and
        # produces tagged phrases in keyed JSON Lines format for storage in an
        # RDD.  The returned generator is initialized with
        # featureLiesFilePath, modelFilePath, etc.
        p = applyCrfKj.ApplyCrfKj(self.featureListFilePath, self.modelFilePath, self.debug, self.statistics)
        return p.process(sourceRDD)


def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Optionallly give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    parser.add_argument('-o','--output', help="Required output file of phrases in keyed JSON Lines format.", required=True)
    parser.add_argument('-p','--partitions', help="Optional number of partitions.", required=False, type=int, default=1)
    parser.add_argument('-s','--statistics', help="Optionally report use statistics.", required=False, action='store_true')
    args = parser.parse_args()

    # if os.path.exists(args.output):
    #     os.remove(args.output)

    if args.debug:
        print "Starting applyCrfKjSpark."
    sc = SparkContext()
    inputRDD = sc.textFile(args.input, args.partitions)
    resultsRDD = inputRDD.mapPartitions(ApplyCrfKjSpark(args.featlist, args.model, args.debug, args.statistics).process)
    resultsRDD.saveAsTextFile(args.output)
    if args.debug:
        print "Ending applyCrfKjSpark."

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
