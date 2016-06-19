#!/usr/bin/env python

"""This program will read JSON file (such as adjudicated_modeled_live_eyehair_100_03.json) and print the tokens in it."""

import argparse
import sys
import crf_sentences

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    args=parser.parse_args()


    sentences = crf_sentences.CrfSentencesFromJsonFile(args.input)
    for sentence in sentences:
        print sentence.getAllTokens()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
