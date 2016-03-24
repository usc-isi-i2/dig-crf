#!/usr/bin/env python

"""This program will read JSON file (such as adjudicated_modeled_live_eyehair_100_03.json) and print the tokens in it."""

import argparse
import sys
import scrapings
import crf_features as crff

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in JSON format.", required=True)
    args=parser.parse_args()

    # Read the Web scrapings:
    s = scrapings.Scrapings(args.input)

    # Create a CrfFeatures object.  This classs provides a lot of services, but we'll uses only a subset.
    c = crff.CrfFeatures(args.featlist)

    for sidx in range(0, s.sentenceCount()):
        tokens = s.getAllTokens(sidx)
        fc = c.featurizeSentence(tokens)
        for idx, token in enumerate(tokens):
            features = fc[idx]
            print token, features

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
