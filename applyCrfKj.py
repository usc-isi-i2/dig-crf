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
import crf_sentences as crfs
import crf_features as crff
import CRFPP
import json

def applyCrf(sentences, featlist, model, debug=False, statistics=False):
    """This is a generater for result phrases.  The primary input, 'sentences', is an iterator.  This model of an interator input and iterator results should work well with Spark."""

    # Create a CrfFeatures object.  This class provides a lot of services, but we'll use only a few.
    crfFeatures = crff.CrfFeatures(featlist)

    # Create a CRF++ processor.
    tagger = CRFPP.Tagger("-m " + model)

    # Clear the statistics:
    sentenceCount = 0 # Number of input "sentences" -- e.g., ads
    tokenCount = 0    # Number of input tokens -- words, punctuation, whatever
    phraseCount = 0   # Number of output phrases

    for sentence in sentences:
        tokens = sentence.getAllTokens()
        if debug:
            print "len(tokens)=%d" % len(tokens)
            sentenceCount += 1
            tokenCount += len(tokens)
            
        fc = crfFeatures.featurizeSentence(tokens)
        if debug:
            print "len(fc)=%d" % len(fc)
        tagger.clear()
        for idx, token in enumerate(tokens):
            features = fc[idx]
            if debug:
                print "token#%d (%s) has %d features" % (idx, token, len(features))
            tf = token + ' ' + ' '.join(features)
            tagger.add(tf.encode('utf-8'))
        tagger.parse()
        # tagger.size() returns the number of tokens that were added.
        # tagger.xsize() returns the number of features plus 1 (for the token).
        if debug:
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
        currentTagName = None
        tags = { } 
        for tokenIdx in range(0, tagger.size()):
            if debug:
                for featureIdx in range (0, nfeatures):
                    print "x(%d, %d)=%s" % (tokenIdx, featureIdx, tagger.x(tokenIdx, featureIdx))
            # tagger.x(tokenIdx, 0) is the original token
            # tagger.y(tokenIdx) is the index of the tag assigned to that token.
            # tagger.yname(tagger.y(tokenIdx)) is the tag assigned to that token.
            #
            # Assume that tagger.y(tokenIdx) != 0 iff something interesting was found.
            tagIdx = tagger.y(tokenIdx)
            if debug:
                print "%s %s %d" % (tagger.x(tokenIdx, 0), tagger.yname(tagIdx), tagIdx)
            if tagIdx != 0:
                tagName = tagger.yname(tagIdx)

                if tagName != currentTagName:
                    if currentTagName != None:
                        yield sentence.getKey() + '\t' + json.dumps(tags, indent=None)
                        phraseCount += 1
                        tags.clear()
                    currentTagName = tagName

                if tagName not in tags:
                    tags[tagName] = []
                tags[tagName].append(tagger.x(tokenIdx, 0))
            else:
                if currentTagName != None:
                    yield sentence.getKey() + '\t' + json.dumps(tags, indent=None)
                    phraseCount += 1
                    tags.clear()
                    currentTagName = None

        # Write out any remaining tags (boundary case):
        if currentTagName != None:
            yield sentence.getKey() + '\t' + json.dumps(tags, indent=None)
            phraseCount += 1
            tags.clear()
            currentTagName = None

    if statistics:
        print "input:  %d sentences, %d tokens" % (sentenceCount, tokenCount)
        print "output: %d phrases" % phraseCount


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
    sentences = crfs.CrfSentencesFromKeyedJsonLinesFile(args.input)

    for result in applyCrf(sentences, args.featlist, args.model, args.debug, args.statistics):
        outfile.write(result + '\n')

    if args.output != None:
        outfile.close()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
