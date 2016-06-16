#!/usr/bin/env python

"""This program will read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file. The input data will optionally be
converted to a (key, jsonLine) pair before processing, to test the paired data
processing path.  The keys in the input file will be passed through to the
output file, but the text and tokens will not.

If the input data path simulated a pair file, the output data path will also
simulate a pair file. No path is provided to test reading unpaired lines and
returning paired lines, or vice versa.

"""

import argparse
import codecs
import json
import sys
import applyCrf
import crf_sentences as crfs
from hybridJaccard import hybridJaccard

def getHybridJaccardResultFilter(hybridJaccardProcessors):
    """Return a hybrid Jaccard resultFilter with access to hybridJaccardProcessors."""
    def hybridJaccardResultFilter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Apply hybrid Jaccard filtering if a filter has been defined for the current
        tag.  Return True if HJ succeeds or is not applied, else return False."""
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
    parser.add_argument('-d','--debug', help="Give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-e','--embedKey', help="Embed the key in the output.", required=False)
    parser.add_argument('-f','--featlist', help="Input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-k','--keyed', help="The input lines are keyed.", required=False, action='store_true')
    parser.add_argument('--hybridJaccardConfig', help="Configuration file for hybrid Jaccard processing.", required=False)
    parser.add_argument('-i','--input', help="Input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-j','--justTokens', help="The input JSON line data is just tokens.", required=False, action='store_true')
    parser.add_argument('-m','--model', help="Input model file.", required=True)
    parser.add_argument('-o','--output', help="Output file of phrases in keyed JSON Lines format.", required=False)
    parser.add_argument('-p','--pairs', help="Test the paired data processing path.", required=False, action='store_true')
    parser.add_argument('-s','--statistics', help="Report use statistics.", required=False, action='store_true')
    parser.add_argument('-v','--verbose', help="Report progress.", required=False, action='store_true')
    parser.add_argument('-x','--extract', help="Name the field with text or tokens.", required=False)
    args = parser.parse_args()

    outfile = sys.stdout
    if args.output != None:
        outfile = codecs.open(args.output, 'wb', 'utf-8')

    tagger = applyCrf.ApplyCrf(args.featlist, args.model,
                               inputPairs=args.pairs, inputKeyed=args.keyed,
                               inputJustTokens=args.justTokens, extractFrom=args.extract,
                               outputPairs=args.pairs, embedKey=args.embedKey,
                               debug=args.debug, sumStatistics=args.statistics)

    # Request hybrid Jaccard processing?
    if args.hybridJaccardConfig:
        if args.verbose:
            print "========================================"
            print "Preparing for hybrid Jaccard processing"
        # Read the hybrid Jaccard configuration file.  For each tag type
        # mentioned in the file, create a hybridJaccard tagger.
        hybridJaccardProcessors = { }
        with open(args.hybridJaccardConfig) as hybridJaccardConfigFile:
            hybridJaccardConf = json.load(hybridJaccardConfigFile)
            for tagType in hybridJaccardConf:
                if args.verbose:
                    print "    %s" % tagType
                hj = hybridJaccard.HybridJaccard(method_type=tagType)
                hj.build_configuration(hybridJaccardConf)
                hybridJaccardProcessors[tagType] = hj
        # Tell the tagger to use hybrid Jaccard result filtering:
        tagger.setResultFilter(getHybridJaccardResultFilter(hybridJaccardProcessors))
        if args.verbose:
            print "========================================"
        
    # Read the Web scrapings as keyed JSON Lines, optionally converting them
    # to pairs, handling justTokens, etc.:
    if args.pairs:
        source = crfs.CrfSentencesPairedJsonLinesReader(args.input)
        for key, result in tagger.process(source):
            outfile.write(key + '\t' + result + '\n')
    else:
        source = crfs.CrfSentencesJsonLinesReader(args.input)
        for result in tagger.process(source):
            outfile.write(result + '\n')


    if args.output != None:
        outfile.close()

    if args.statistics:
        tagger.showStatistics()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
