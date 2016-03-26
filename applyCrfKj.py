#!/usr/bin/env python

"""This program will read keyed JSON Lines file (such as
adjudicated_modeled_live_eyehair_100.kjsonl), process it with CRF++, and print
detected attributes as a keyed JSON file, formatted to Karma's liking. The
keys in the input file will be passed through to the output file, but the
text and tokens will not.

"""

import argparse
import sys
import scrapings
import crf_features as crff
import CRFPP
import json

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Optional give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in keyed JSON Lines format.", required=True)
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    args=parser.parse_args()

    # Create a CrfFeatures object.  This classs provides a lot of services, but we'll use only a subset.
    c = crff.CrfFeatures(args.featlist)

    # Create a CRF++ processor.
    tagger = CRFPP.Tagger("-m " + args.model)

    # Read the Web scrapings as keyed JSON Lines:
    s = scrapings.Scrapings(args.input, kj=True)
    if args.debug:
        print "sencence count=%d" % s.sentenceCount()

    for sidx in range(0, s.sentenceCount()):
        tokens = s.getAllTokens(sidx)
        if args.debug:
            print "len(tokens)=%d" % len(tokens)
        fc = c.featurizeSentence(tokens)
        if args.debug:
            print "len(fc)=%d" % len(fc)
        tagger.clear()
        for idx, token in enumerate(tokens):
            features = fc[idx]
            if args.debug:
                print "token#%d (%s) has %d features" % (idx, token, len(features))
            tf = token + ' ' + ' '.join(features)
            tagger.add(tf.encode('utf8'))
        tagger.parse()
        # tagger.size() returns the number of tokens that were added.
        # tagger.xsize() returns the number of features plus 1 (for the token).
        if args.debug:
            print "size=%d" % tagger.size()
            print "xsize=%d" % tagger.xsize()
            print "ysize=%d" % tagger.ysize()
            print "dsize=%d" % tagger.dsize()
            print "vlevel=%d" % tagger.vlevel()
            print "nbest=%d" % tagger.nbest()

        ntokens = tagger.size()
        if ntokens != len(tokens):
            print "received %d tokens , expected %d" % (ntokens, len(tokens))
        nfeatures = tagger.xsize()

        # Accumulate interesting tags.  We'll use a dictionary to a list of tokens.
        # We'll use the tag name as the key to the dictionary.  Would it be significantly
        # faster to use the tag index, instead?
        tags = {}
        for tokenIdx in range(0, tagger.size()):
            if args.debug:
                for featureIdx in range (0, nfeatures):
                    print "x(%d, %d)=%s" % (tokenIdx, featureIdx, tagger.x(tokenIdx, featureIdx))
            # tagger.x(tokenIdx, 0) is the original token
            # tagger.y(tokenIdx) is the index of the tag assigned to that token.
            # tagger.yname(tagger.y(tokenIdx)) is the tag assigned to that token.
            #
            # Assume that tagger.y(tokenIdx) != 0 iff something interesting was found.
            tagIdx = tagger.y(tokenIdx)
            if args.debug:
                print "%s %s %d" % (tagger.x(tokenIdx, 0), tagger.yname(tagIdx), tagIdx)
            if tagIdx != 0:
                tagName = tagger.yname(tagIdx)
                if tagName not in tags:
                    tags[tagName] = []
                tags[tagName].append(tagger.x(tokenIdx, 0))

        # Write out the tags, nut only if we found any.
        if len(tags) > 0:
            print "%s\t%s" % (s.getKey(sidx), json.dumps(tags, indent=None))

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
