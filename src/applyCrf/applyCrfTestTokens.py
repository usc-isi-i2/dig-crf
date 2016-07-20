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

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument(     '--fusePhrases', '--fusedPhrases', help="Join each result phrase", required=False, action='store_true')
    parser.add_argument('--hybridJaccardConfig', help="Configuration file for hybrid Jaccard processing.", required=False)
    parser.add_argument('-i','--input', help="Input file of phrases of tokens in JSON Lines format.", required=True)
    parser.add_argument('-m','--model', help="Input model file.", required=True)
    parser.add_argument('-o','--output', help="Output file of phrases in keyed JSON Lines format.", required=False)
    parser.add_argument('-s','--statistics', help="Report use statistics.", required=False, action='store_true')
    parser.add_argument('-t','--tags', help="Restrict the set of tags and optionally rename them: tagName,tagName:newTagName,...", required=False)
    parser.add_argument('-v','--verbose', help="Report progress.", required=False, action='store_true')
    args = parser.parse_args()

    outfile = sys.stdout
    if args.output != None:
        outfile = codecs.open(args.output, 'wb', 'utf-8')

    tagger = applyCrf.ApplyCrf(args.featlist, args.model, args.hybridJaccardConfig,
                               tagMap=args.tags, fusePhrases=args.fusePhrases,
                               debug=args.debug, sumStatistics=args.statistics)

    # Read the tokens as keyed JSON Lines:
    with codecs.open(args.input, 'rb', 'utf-8') as lineFile:
        for line in lineFile:
            tokens = json.loads(line)
            result = tagger.processTokens(tokens)
            jsonResult = json.dumps(result)
            if args.output:
                outfile.write(jsonResult + "\n")
            else:
                print(jsonResult)

    if args.output != None:
        outfile.close()

    if args.statistics:
        tagger.showStatistics()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
