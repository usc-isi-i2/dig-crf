#!/usr/bin/env python

"""This program will read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file, formatted to with the pair RDD path. The
keys in the input file will be passed through to the output file, but the
text and tokens will not.

"""

import argparse
import codecs
import sys
import crf_sentences as crfs
import crf_features as crff
import CRFPP
import json
import applyCrfGenerator

def pairRDDJsonLinesResultFormatter(sentence, currentTagName, phrase):
    """Format the result as pairRDD Json Lines."""
    taggedPhrase = { }
    taggedPhrase[currentTagName] = phrase
    return sentence.getKey(), json.dumps(taggedPhrase, indent=None)

class ApplyCrfPj:
    def __init__(self, featureListFilePath, modelFilePath, debug=False, statistics=False):
        """Initialize the ApplyCrfKj object.

featureListFilePath is the path to the word and phrase-list control file used
by crf_features.

modelFilePath is the path to a trained CRF++ model.

debug, when True, causes the code to emit helpful debugging information on
standard output.

statistics, when True, emits a count of input sentences and tokens, and a
count of output phrases, when done.

        """

        self.featureListFilePath = featureListFilePath
        self.modelFilePath = modelFilePath
        self.debug = debug
        self.statistics = statistics

        # Create a CrfFeatures object.  This class provides a lot of services, but we'll use only a few.
        self.crfFeatures = crff.CrfFeatures(featureListFilePath)

        # Create a CRF++ processor object:
        self.tagger = CRFPP.Tagger("-m " + modelFilePath)


    def process(self, sentences):
        """Return a generator to process the sentences."""
        return applyCrfGenerator.applyCrfGenerator(sentences, self.crfFeatures, self.tagger, pairRDDJsonLinesResultFormatter, self.debug, self.statistics)

def keyedJsonLinesPairReader(keyedJsonFilename):
    """This generator reads a keyed JSON Lines file and yields the lines split into (key, jsonLine) pairs."""
    with codecs.open(keyedJsonFilename, 'rb', 'utf-8') as keyedJsonFile:
        for line in keyedJsonFile:
            key, jsonData = line.split('\t', 1)
            yield key, jsonData

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Optionallly give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    parser.add_argument('-o','--output', help="Optional output file of phrases in keyed JSON Lines format.", required=False)
    parser.add_argument('-s','--statistics', help="Optionally report use statistics.", required=False, action='store_true')
    args = parser.parse_args()

    outfile = sys.stdout
    if args.output != None:
        outfile = codecs.open(args.output, 'wb', 'utf-8')

    # Read the Web scrapings as keyed JSON Lines:
    pairSource = keyedJsonLinesPairReader(args.input)
    sentences = crfs.CrfSentencesFromKeyedJsonLinesPairSource(pairSource)

    processor = ApplyCrfPj(args.featlist, args.model, args.debug, args.statistics)
    for key, jsonData in processor.process(sentences):
        outfile.write(key + "\t" + jsonData + '\n')

    if args.output != None:
        outfile.close()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
