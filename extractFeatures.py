#!/usr/bin/env python

"""This program will read JSON file (such as adjudicated_modeled_live_eyehair_100_03.json) and print the tokens in it with features."""

import argparse
import sys
import crf_sentences as crfs
import crf_features as crff

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in JSON format.", required=True)
    args=parser.parse_args()

    # Read the Web scrapings:
    sentences = crfs.CrfSentencesFromJsonFile(args.input)

    # Create a CrfFeatures object.  This class provides a lot of services, but we'll use only a few.
    c = crff.CrfFeatures(args.featlist)

    for sentence in sentences:
        tokens = sentence.getAllTokens()
        fc = c.featurizeSentence(tokens)
        for idx, token in enumerate(tokens):
            features = fc[idx]
            print token, features

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
