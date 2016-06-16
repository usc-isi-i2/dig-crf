#!/usr/bin/env python

"""This program will use Apache Spark to read a keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), optionally convert it to a pair RDD,
process it with CRF++, and print detected attributes as pair RDD keyed JSON
files, formatted to Karma's liking. The keys in the input file will be passed
through to the output file, but the text and tokens will not.

"""

import argparse
import json
import sys
from pyspark import SparkContext
import applyCrfSpark
from hybridJaccard import hybridJaccard

def getHybridJaccardResultFilter(hybridJaccardProcessors):
    """Return a hybrid Jaccard resultFilter with access to hybridJaccardProcessors."""
    def hybridJaccardResultFilter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Apply hibrid Jaccard filtering if a filter has been defined for the current
        tag.  Return true if HJ succeeds or is not applied, else return False."""
        if tagName in hybridJaccardProcessors:
            phrase = sentence.getTokens()[phraseFirstTokenIdx:(phraseFirstTokenIdx+phraseTokenCount)]
            hjResult = hybridJaccardProcessors[tagName].findBestMatchWordsCached(phrase)
            if hjResult is None:
                return False
            sentence.setFilteredPhrase(hjResult)
        return True
    return hybridJaccardResultFilter

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--coalesceInput', type=int, default=0, help="Reduce the number of partitions on input.", required=False)
    parser.add_argument('--coalesceOutput', type=int, default=0, help="Reduce the number of partitions on output.", required=False)
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
    parser.add_argument('-v','--verbose', help="Report progress.", required=False, action='store_true')
    parser.add_argument('-x','--extract', help="Name the field with text or tokens.", required=False)
    args = parser.parse_args()

    if args.verbose:
        print "========================================"
        print "Starting applyCrfSparkTest."
        print "========================================"

    # Open a Spark context and set up a CRF tagger object.
    sc = SparkContext()
    tagger = applyCrfSpark.ApplyCrfSpark(args.featlist, args.model,

                                         inputPairs=args.inputPairs or args.pairs or args.inputSeq,
                                         inputKeyed=args.keyed, inputJustTokens=args.justTokens,
                                         extractFrom=args.extract, embedKey=args.embedKey,
                                         outputPairs=args.outputPairs or args.pairs or args.outputSeq,
                                         debug=args.debug, showStatistics=args.statistics)

    # Request hybrid Jaccard processing?
    if args.hybridJaccardConfig:
        print "========================================"
        print "Preparing for hybrid Jaccard processing"
        # Read the hybrid Jaccard configuration file.  For each tag type
        # mentioned in the file, create a hybridJaccard processor.
        hybridJaccardProcessors = { }
        with open(args.hybridJaccardConfig) as hybridJaccardConfigFile:
            hybridJaccardConf = json.load(hybridJaccardConfigFile)
            for tagType in hybridJaccardConf:
                print "    %s" % tagType
                hj = hybridJaccard.HybridJaccard(method_type=tagType)
                hj.build_configuration(hybridJaccardConf)
                hybridJaccardProcessors[tagType] = hj
        # Tell the tagger to use hybrid Jaccard result filtering:
        tagger.setResultFilter(getHybridJaccardResultFilter(hybridJaccardProcessors))
        print "========================================"
        

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
        inputRDD = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text",  "org.apache.hadoop.io.Text",
                                   minSplits=minPartitions)
    else:
        # Read an input text file for testing.
        inputRDD = sc.textFile(args.input, minPartitions)
        if args.inputPairs or args.pairs:
            # Converting the text lines into input pairs:
            inputRDD = inputRDD.map(lambda s: s.split('\t', 1))

    # Which is better? coalescing before processing or after processing?
    if args.coalesceInput > 0:
        numPartitions = inputRDD.getNumPartitions()
        if args.coalesceInput < numPartitions:
            print "========================================"
            print "Coalescing %d ==> %d input partitions" % (numPartitions, args.coalesceInput)
            print "========================================"
            inputRDD = inputRDD.coalesce(args.coalesceInput)

    # Perform the main RDD processing.
    resultsRDD = tagger.perform(inputRDD)

    # Which is better? coalescing before processing or after processing?
    if args.coalesceOutput > 0:
        numPartitions = resultsRDD.getNumPartitions()
        if args.coalesceOutput < numPartitions:
            print "========================================"
            print "Coalescing %d ==> %d output partitions" % (numPartitions, args.coalesceOutput)
            print "========================================"
            resultsRDD = resultsRDD.coalesce(args.coalesceOutput)

    # The output will either be a Sequence file or a text file.  If it's a
    # text file, it might be a tab-separated pair file, or just JSON Lines
    # data.  In either case, the main RDD processing took care of all
    # necessary formatting.
    if args.outputSeq:
        if args.verbose:
            print "========================================"
            print "Saving data as a Hadoop SEQ file."
            print args.output
            print "========================================"
        resultsRDD.saveAsNewAPIHadoopFile(args.output,
                                          outputFormatClass="org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                                          keyClass="org.apache.hadoop.io.Text",
                                          valueClass="org.apache.hadoop.io.Text")
    else:
        if args.verbose:
            print "========================================"
            print "Saving data as a text file."
            print args.output
            print "========================================"
        # Paired results will be converted automatically.
        resultsRDD.saveAsTextFile(args.output,
                                  compressionCodecClass=args.outputCompressionClass)

    if args.verbose:
        print "========================================"
        print "Ending applyCrfSparkTest."
        print "========================================"
    sc.stop()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
