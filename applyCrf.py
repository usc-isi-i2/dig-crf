#!/usr/bin/env python

"""This program will read a JSON file (such as adjudicated_modeled_live_eyehair_100_03.json) and process it with CRF++. The labels assigned by CRF++ are printed."""

import argparse
import sys
import scrapings
import crf_features as crff
import CRFPP

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--debug', help="Optional give debugging feedback.", required=False, action='store_true')
    parser.add_argument('-f','--featlist', help="Required input file with features to be extracted, one feature entry per line.", required=True)
    parser.add_argument('-i','--input', help="Required input file with Web scraping sentences in JSON format.", required=True)
    parser.add_argument('-m','--model', help="Required input model file.", required=True)
    args=parser.parse_args()

    # Read the Web scrapings:
    s = scrapings.Scrapings(args.input)
    if args.debug:
        print "sencence count=%d" % s.sentenceCount()

    # Create a CrfFeatures object.  This classs provides a lot of services, but we'll use only a subset.
    c = crff.CrfFeatures(args.featlist)

    # Create a CRF++ processor.
    tagger = CRFPP.Tagger("-m " + args.model)

    # TODO: The model appears to have a saved copy of the features used to build it.
    # It would be nidec to extract those feature definitions for uses in featurizing
    # sentences.

    if args.debug:
        print "modelXsize=%d" % modelXsize

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
            tagger.add(str(tf))
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
        for tokenIdx in range(0, tagger.size()):
            if args.debug:
                for featureIdx in range (0, nfeatures):
                    print "x(%d, %d)=%s" % (tokenIdx, featureIdx, tagger.x(tokenIdx, featureIdx))
            # tagger.x(tokenIdx, 0) is the original token
            # tagger.yname(tagger.y(tokenIdx)) is the label assigned to that token.
            print "%s %s" % (tagger.x(tokenIdx, 0), tagger.yname(tagger.y(tokenIdx)))

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())