#!/usr/bin/env python

"""This program will read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file, formatted to Karma's liking. The
keys in the input file will be passed through to the output file, but the
text and tokens will not.

"""

import argparse
import codecs
import sys
import json
import applyCrf

# Is there a standard library way to do this?
def keyedJsonLinesReader(keyedJsonFilename):
    """This generator reads a keyed JSON Lines file and yields the lines."""
    with codecs.open(keyedJsonFilename, 'rb', 'utf-8') as keyedJsonFile:
        for line in keyedJsonFile:
            yield line

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

    # Read the Web scrapings as keyed JSON Lines and process them:
    source = keyedJsonLinesReader(args.input)
    processor = applyCrf.ApplyCrfKj(args.featlist, args.model, args.debug, args.statistics)
    for result in processor.process(source):
        outfile.write(result + '\n')

    if args.output != None:
        outfile.close()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
