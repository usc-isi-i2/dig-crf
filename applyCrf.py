#!/usr/bin/env python

"""The applyCrfGenerator generator processes sentences with CRF++.  Wrappers
are provided for various types of input and output.

"""

import crf_sentences as crfs
import crf_features as crff
import CRFPP
import json

def applyCrfGenerator(sentences, crfFeatures, tagger, resultFormatter, debug=False, statistics=False):
    """Apply CRF++ to a sequence of "sentences", generating tagged phrases
as output.  0 to N tagged phrases will generated as output for each input
sentence.

The primary input, 'sentences', is an iterator, which is used without internal
buffering.  The function applyCrfGenerator is a generator, which is equivalent
to producing an iterator as output.  This paradigm of an interator input,
iterator results, and no internal buffering should work well with Spark.

featureListFilePath is a crf_features object.

tagger is a CRF++ instance with a trained CRF++ model.

resultFormatter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount)
 converts tagged phrase tuples into the desired output format.
As a design alternative, the generator could be made to deliver tagged phrase
tuples directly, with an external generator or iterator converting them into
the desired format.

debug, when True, causes the code to emit helpful debugging information on
standard output.

statistics, when True, emits a count of input sentences and tokens, and a
count of output phrases, when done.

CRF++ is written in C++ code, which is accessed by a Python wrapper.  The
CRF++ code must be installed in the Python interpreter.  If Spark is used to
distribute processing among multiple systems, CRF++ must be installed on the
Python interpreter used by Spark on each system.

Opportunities for further optimization: The 'debug' and 'statistics' code
support could be removed.  crf_features could be restructured to isolate its
core routines.  crf_sentences defines a CrfSentence object with getter
methods; these could be interpolated into this code for efficiency, but that
might reduce maintainability.

Note: Python generators apparently cannot be class methods.  They effectively
create their own classes.

    """

    # Define the tag name that appears on words that have not been tagged by
    # CRF++.  So far, we don't know why this particular value is used.  This is
    # a potential source of future failures.
    UNTAGGED_TAG_NAME = "O"

    # Clear the statistics:
    sentenceCount = 0     # Number of input "sentences" -- e.g., ads
    tokenCount = 0        # Number of input tokens -- words, punctuation, whatever
    taggedPhraseCount = 0 # Number of tagged output phrases
    taggedTokenCount = 0  # Number of tagged output tokens

    for sentence in sentences:
        sentenceCount += 1
        tokens = sentence.getAllTokens()
        tokenCount += len(tokens)
        if debug:
            print "len(tokens)=%d" % len(tokens)
            
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

        # Accumulate interesting tokens into tagged phrases which are sent as results.
        currentTagName = UNTAGGED_TAG_NAME
        phraseFirstTokenIdx = 0 # Any value would do.
        phraseTokenCount = 0

        for tokenIdx in range(0, tagger.size()):
            if debug:
                for featureIdx in range (0, nfeatures):
                    print "x(%d, %d)=%s" % (tokenIdx, featureIdx, tagger.x(tokenIdx, featureIdx))
            # tagger.x(tokenIdx, 0) is the original token
            # tagger.y(tokenIdx) is the index of the tag assigned to that token.
            # tagger.yname(tagger.y(tokenIdx)) is the name of the tag assigned to that token.
            tagIdx = tagger.y(tokenIdx)
            tagName = tagger.yname(tagIdx)
            if debug:
                print "%s %s %d" % (tagger.x(tokenIdx, 0), tagger.yname(tagIdx), tagIdx)

            # If we are changing tag names, write out any queued tagged phrase:
            if tagName != currentTagName:
                if phraseTokenCount > 0:
                    yield resultFormatter(sentence, currentTagName, phraseFirstTokenIdx, phraseTokenCount)
                    taggedPhraseCount += 1
                    phraseTokenCount = 0
                currentTagName = tagName

            # Unless this token is untagged, append it to the current phrase.
            if tagName != UNTAGGED_TAG_NAME:
                if phraseTokenCount == 0:
                    phraseFirstTokenIdx = tokenIdx
                phraseTokenCount += 1
                taggedTokenCount += 1

        # Write out any remaining phrase (boundary case):
        if phraseTokenCount > 0:
            yield resultFormatter(sentence, currentTagName, phraseFirstTokenIdx, phraseTokenCount)
            taggedPhraseCount += 1
            # Don't need to do these as we're about to exit the loop:
            # phraseTokenCount = 0
            # currentTagName = UNTAGGED_TAG_NAME

    if statistics:
        print "input:  %d sentences, %d tokens" % (sentenceCount, tokenCount)
        print "output: %d phrases, %d tokens" % (taggedPhraseCount, taggedTokenCount)

class ApplyCrfToSentencesYieldingTaggedPhraseTuples(object):
    """Apply CRF++ to a source of sentences, returning a sequence of tagged phrase
    tuples.  The output tuples could be used to create something similar to the
    MTurk sentence output.

    (sentence, tagName, phraseFirstTokenIndex, phraseTokenCount)

    """

    def __init__(self, featureListFilePath, modelFilePath, debug=False, statistics=False):
        """Initialize the ApplyCrfToSentencesYieldingTaggedPhraseTuples object.

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

        # Defer creating these objects.  The benefit is better operation with
        # Spark (deferring creating the tagger may be necessary with Spark).
        # The downside is that problems opening the feature list file or the
        # model file are reported later.
        self.crfFeatures = None
        self.tagger = None

    def setup(self):
        """create the CRF Features and CRF tagger objects, if they haven't been created yet."""
        if self.crfFeatures == None:
            # Create a CrfFeatures object.  This class provides a lot of services, but we'll use only a few.
            self.crfFeatures = crff.CrfFeatures(self.featureListFilePath)

        if self.tagger == None:
            # Create a CRF++ processor object:
            self.tagger = CRFPP.Tagger("-m " + self.modelFilePath)

    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Pass the result tuples through."""
        return sentence, tagName, phraseFirstTokenIdx, phraseTokenCount

    def process(self, sentences):
        """Return a generator to process the sentences from the source.  This method may be called multiple times to process multiple sources."""
        self.setup() # Create the CRF Features and Tagger objects if necessary.
        return applyCrfGenerator(sentences, self.crfFeatures, self.tagger, self.resultFormatter, self.debug, self.statistics)

    def perform(self, sourceRDD):
        """Apply the process routine in an Apache Spark context."""
        return sourceRDD.mapPartitions(self.process)
        

class ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines (ApplyCrfToSentencesYieldingTaggedPhraseTuples):
    """Apply CRF++ to a source of sentences, returning a sequence of keys and
    tagged phrase structures.  The tagged phrase structures are encoded as JSON Lines.

    yields: (key, taggedPhraseJsonLine)

    """
    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Extract the tagged phrases and format the result as keyed Json Lines."""
        phrase = sentence.getAllTokens()[phraseFirstTokenIdx:(phraseFirstTokenIdx+phraseTokenCount)]
        taggedPhrase = { }
        taggedPhrase[tagName] = phrase
        return sentence.getKey(), json.dumps(taggedPhrase, indent=None)

class ApplyCrfKj (ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines):
    """Apply CRF++ to a source of sentences in keyed JSON Lines format, returning
a sequence of tagged phrases in keyed JSON Lines format.

    yields: keyedTaggedPhraseJsonLine
            key \t taggedPhraseJsonLine

    """
    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Format the result as keyed Json Lines."""
        key, taggedPhraseJsonLine = super(ApplyCrfKj, self).resultFormatter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount)
        return key + '\t' + taggedPhraseJsonLine

    def process(self, source):
        """Return a generator to process a sequence of sentences from the source.  The
source presents the sentences in keyed sentence JSON Lines format.  This
method may be called multiple times to process multiple sources.

        """
        return super(ApplyCrfKj, self).process(crfs.CrfSentencesFromKeyedJsonLinesSource(source))

class ApplyCrfPj (ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines):
    """Apply CRF++ to a source of sentences in paired (key, sentenceJsonLine) format,
    returning keys and tagged phrases in paired (key, taggedPhraseJsonLine) format.

    yields: (key, taggedPhraseJsonLine)

    """

    # The parent resultFormatter(...) method returns the proper pair.

    def process(self, pairSource):
        """Return a generator to process a sequence of sentences from the source.
The source presents the sentences in paired (key, sentenceJsonLine) format.
This method may be called multiple times to process multiple sources.

        """
        return super(ApplyCrfPj, self).process(crfs.CrfSentencesFromKeyedJsonLinesPairSource(pairSource))
