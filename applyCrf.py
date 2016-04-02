#!/usr/bin/env python

"""The applyCrfGenerator generator processes sentences with CRF++.  Wrappers
are provided for various types of input and output.

Code Structure
==== =========

class ApplyCrfKj
    instantiates class CrfSentencesFromKeyedJsonLinesSource (an iterator)
    extends class ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines
        extends class ApplyCrfToSentencesYieldingTaggedPhraseTuples
            instantiates crfFeatures and CRF++
            wraps generator function applyCrfGenerator

class ApplyCrfPj
    instantiates class CrfSentencesFromKeyedJsonLinesPairSource (an iterator)
    extends class ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines
        extends class ApplyCrfToSentencesYieldingTaggedPhraseTuples
            instantiates crfFeatures and CRF++
            wraps generator function applyCrfGenerator

The heart of this code is a generator, applyCrfGenerator(...).  It takes a
sequence of "sentence" objects and produces a sequence of tagged phrase
tuples.  It also requires a crfFeatures instance and a CRF++ tagger instance,
which are instantiated in higher-level code, and a couple of debugging
controls.

ApplyCrfToSentencesYieldingTaggedPhraseTuples is a class that prepares the
environment for applyCrfGenerator.  It is initialized with paths to the
feature list file for crfFeatures and the trained model file for CRF++.  It
creates instances of crfFeatures and the CRF++ tagger when needed, and starts
applyCrfGenerator.  It provides a perform(...) method for use in Apache Spark
scripts per USC/ISI's Dig project's conventions.

ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines is a class that
extends ApplyCrfToSentencesYieldingTaggedPhraseTuples.  It formats the output
tuples produced by the generator, applyCrfGenerator(...), into a form that it
close to what is needed in Dig's Apache Spark scripts.  The new output format
is a sequence of tuples of (key, taggedPhraseJsonLine).

ApplyCrfKj extends ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines to
integrate with Dig's Apache Spark processing scripts.  It reads a sequence of
keyed JSON lines, using an iterator in "crf_sentences.py" that converts the
lines into a sequence of "sentence" objects.  It reformats the output provided
by ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines into a sequence of
keyed JSON lines (i.e., "key\ttaggedPhraseJsonLine").

ApplyCrfPj also extends
ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines to integrate with
Dig's Apache Spark processing scripts.  It reads a sequence of (key,
sentenceJsonLine) pairs, using an iterator in "crf_sentences.py" that converts
the pairs into a sequence of "sentence" objects.  It does not need to
reformate the output provided by its parent, which is already a (key,
taggedPhraseJsonLine) pair.

Data Flow
==== ====

Data processed through ApplyCrfKj.perform(...) and ApplyCrfPj.perform(...)
(and the lower-level process(...) routines) is passed through an
iterator/generator cascade.  The code processes a "sentence" at a time,
instead of reading all the input before processing it, or buffering all output
before releasing it.  Thus, if the data sources are also iterators/generators
(as is the case for Apache Spark RDDs) and the data destinations are also
prepared to process data with a minimum of excess buffering (as is the case
with Apache Spark RDDs), then the data will be processed using a minimum of
memory.

Data records enter through one of the following iterator classes (located in
"cfr_sentences.py"):

CrfSentencesFromKeyedJsonLinesSource       processes keyed JSON Lines.

CrfSentencesFromKeyedJsonLinesPairSource   processes (key, jsonLines) pairs.

The JSON-formatted "sentence" data is loaded into Python data structures,
which are wrapped in a CrfSentence object. The primary purpose of CrfSentences
is to provide getter methods for specific portions of the sentence data. It
also provides a place to store and fetch the key associated with the sentence.

applyCrfGenerator accepts sentences, a record at a time, and begins processing
the sentence tokens ("allTokens").  Each token has features generatered for
it with crfFeatures (from "crf_features.py").  The token and its features are
loaded into a CRF++ instance, which was initialized with a trained model.

After all the tokens and features in the sentence have been loaded into CRF++,
it processes them and assigns a tag to each token, or uses the special tag "O"
to indicate that no special tag was assigned to the token.  applyCrfGenerator
scans the tokens, extracting sequences ("tagged phrases") of consecutive
tokens that have been assigned the same tag (excluding "O").  It records the
starting index of each phrase, the number of tokens in the phrase, and the tag
associated with the phrase.  Along with the sentence object, these form a
"tagged phrase tuple".  0 to N tagged phrase tuples may be produced for each
input sentence.

The tagged phrase tuples are formatted before they are emitted by
applyCrfGenerator to whatever outsid ecode is prepared to consume it.
Formatting takes place with the resultFormatter(...) method.  Which
resultFormatter(...) method(s) is/are used depends upon class inheritance.

If the outermost object that caused applyCrfGenerator to be instantiated is of
class ApplyCrfPj, then the resultFormatter(...) from class
ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines converts the
tagged phrase tuples into (key, taggedPhraseJsonLines) pairs, which can
be loaded into Apache Spark pair RDDs.

If the outermost object that caused applyCrfGenerator to be instantiated is of
class ApplyCrfKj, then the resultFormatter(...) from class
ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines converts the tagged
phrase tuples into (key, taggedPhraseJsonLine) pairs, which are the converted
into keyed JSON Lines (<key> "\t" <taggedPhraseJsonLine>) by the
resultFormatter(...) method from class ApplyCrfKj.  The result can be loaded
into an ordinary Apache Spark RDD.

Data Content
==== =======

Note that although this description talks about "sentence" records and data
structures, a single "sentence" record or data structure may contain the text
of multiple sentences in English or some other language.  The text is encoded
in Unicode, stored in UTF-8 format when serialized, and may contain HTML
entities as well as ordinary words.  Furthermore, due to limitations in the
tokenizer that processed the Web-based text before it reached the CFR++
processing stage, a single sentence token may contain a complete English (or
other human language) word, a partial word, multiple words, one or more word
fragments with embedded punctuation and/or HTML entities, or other
irregularities.

Usage
=====

        Here are code fragments that illustrate how the applyCrf code might be
used in Apache Spark processing scripts, along with sample output when
processed with the hair/eye CRF++ model and data from the
"adjudicated_modeled_live_eyehair_100_03.json" dataset (which was preprocessed
into keyed JSON Lines format).

Example: ApplyCrfKj

import applyCrf

sc = SparkContext()
inputRDD = sc.textFile(args.input, args.partitions)
tagger = applyCrf.ApplyCrfKj(args.featlist, args.model, args.debug, args.statistics)
resultsRDD = tagger.perform(inputRDD)
resultsRDD.saveAsTextFile(args.output)

Output:

http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691    {"hairType": ["strawberry", "blond", "hair"]}
http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691    {"eyeColor": ["blue", "eyes"]}
http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8    {"hairType": ["long", "blonde", "hair"]}
http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8    {"eyeColor": ["seductive", "blue", "eyes"]}


Example: ApplyCrfPj

import applyCrf

sc = SparkContext()
inputLinesRDD = sc.textFile(args.input, args.partitions)
inputPairsRDD = inputLinesRDD.map(lambda s: s.split('\t', 1))
tagger = applyCrf.ApplyCrfPj(args.featlist, args.model, args.debug, args.statistics)
resultsRDD = tagger.perform(inputPairsRDD)
resultsRDD.saveAsTextFile(args.output)

Output:

(u'http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691', '{"hairType": ["strawberry", "blond", "hair"]}')
(u'http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691', '{"eyeColor": ["blue", "eyes"]}')
(u'http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8', '{"hairType": ["long", "blonde", "hair"]}')
(u'http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8', '{"eyeColor": ["seductive", "blue", "eyes"]}')

Performance
===========

On typical development hardware, such as a Macbook Pro or an OpenSUSE Linux
system running on an AMD FX-8150 processor, we see about 100 sentences
processed per second using a single CPU core.  Using 8 logical CPU cores, we
see approx. 500 sentences processed per second.

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
        """Extract the tagged phrases and format the result as keys and tagged phrase Json Lines."""
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
