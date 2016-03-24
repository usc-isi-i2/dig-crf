#!/usr/bin/env python

"""This program will read JSON file (such as adjudicated_modeled_live_eyehair_100_03.json) and print the tokens in it."""

import argparse
import sys
import scrapings

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    args=parser.parse_args()


    s = scrapings.Scrapings(args.input)
    for sidx in range(0, s.sentenceCount()):
        print s.getAllTokens(sidx)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
