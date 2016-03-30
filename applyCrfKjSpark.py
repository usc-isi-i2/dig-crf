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

def applyCrfKjSpark(inputPath, outputPath, numPartitions, featureListFilePath, modelFilePath, debug, statistics):
    def doCrf(source): 
        sentences = crfs.CrfSentencesFromKeyedJsonLinesSource(source)
        return applyCrfKj.applyCrfKj(sentences, featureListFilePath, modelFilePath, debug, statistics)

    print "Starting."
    sc = SparkContext()
    inputSource = sc.textFile(inputPath, numPartitions)
    results = inputSource.mapPartitions(doCrf)
    results.saveAsTextFile(outputPath)

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

    applyCrfKjSpark(args.input, args.output, args.partitions, args.featlist, args.model, args.debug, args.statistics)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
