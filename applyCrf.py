#!/usr/bin/env python

"""The applyCrfGenerator generator processes sentences with CRF++.
Each word (token) in an input sentence may be assigned a tag. Wrappers
are provided for various types of input and output.

Code Structure
==== =========

class ApplyCrf
    instantiates class CrfSentencesFromsonLinesSource (an iterator)
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
applyCrfGenerator.

ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines is a class that
extends ApplyCrfToSentencesYieldingTaggedPhraseTuples.  It formats the output
tuples produced by the generator, applyCrfGenerator(...), into a form that it
close to what is needed in Dig's Apache Spark scripts.  The new output format
is a sequence of tuples of (key, taggedPhraseJsonLine).

ApplyCrf extends ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines to
integrate with Dig's Apache Spark processing scripts.  It reads a sequence of
keyed JSON lines, using an iterator in "crf_sentences.py" that converts the
lines into a sequence of "sentence" objects.  It reformats the output provided
by ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines into a sequence of
pairs (for a SequenceFile), JSON lines (with embedded key), or keyed JSON
lines (i.e., "key\ttaggedPhraseJsonLine").

Data Flow
==== ====

Data records enter through the CrfSentencesFromJsonLinesSource iterator class
(located in "cfr_sentences.py").

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
applyCrfGenerator to whatever outside code is prepared to consume it.
Formatting takes place with the resultFormatter(...) method.  Which
resultFormatter(...) method(s) is/are used depends upon class inheritance.

Depending on option settings, the results take the form of:

1)      (key, jsonLine) pairs, which can be saved to a Sequence File,

2)      a jsonLine (with internal key), which can be saved to a text file, or

3)      a keyed JSON line (<key> "\t" <jsonLine>), which can be saved to a
        text file.

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


File Path Mapper
==== ==== ======

This code has special support for accessing the feature list and model files.
When enabled, the location of the files is resolved just before they are
opened using:

path = self.filePathMapper(path)

where self.filePathMapper has been previously set with:

self.setFilePathMapper(filePathMapper)

Delaying this resolution until just befor ethe files are needed is important
in some instances, specifically for use with Apache Spark.

Performance
===========

On typical development hardware, such as a Macbook Pro or an OpenSUSE Linux
system running on an AMD FX-8150 processor, we see about 100 sentences
processed per second using a single CPU core.  Using 8 logical CPU cores, we
see approx. 500 sentences processed per second.

"""

import json

import crf_sentences as crfs
import crf_features as crff
import CRFPP
from hybridJaccard import hybridJaccard

def applyCrfGenerator(sentences, crfFeatures, tagger, resultFilter, resultFormatter,
                      debug=False, statistics=None):
    """Apply CRF++ to a sequence of "sentences", generating tagged phrases
as output.  0 to N tagged phrases will generated as output for each input
sentence.

The primary input, 'sentences', is an iterator, which is used without internal
buffering.  The function applyCrfGenerator is a generator, which is equivalent
to producing an iterator as output.  This paradigm of an interator input,
iterator results, and no internal buffering should work well with Spark.

sentences returns objects that have a "getTokens()" method.  Other than that,
these objects are opaque to this code, and is passed through to the
resultFormatter(...).

featureListFilePath is a crf_features object.

tagger is a CRF++ instance with a trained CRF++ model.

resultFilter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount)
provides an additional level of filtering (and perhaps data transformation) on
the tagged phrase tuples.  If resultFilter is not None, it will be called.  If
it returns False, the tagged tuples will be discarded.  CrfSentence provides
setFilteredPhrase(...) and getFilteredPhrase(...) as a convenient method for
passing data transformations between resultFilter and resultFormatter.

resultFormatter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount)
converts tagged phrase tuples into the desired output format.
As a design alternative, the generator could be made to deliver tagged phrase
tuples directly, with an external generator or iterator converting them into
the desired format.

debug, when True, causes the code to emit helpful debugging information on
standard output.

statistics: When not None, accumulate statistics into this dict when done.

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

    # CRF++ has an undocumented limit of approx. 8192 characters for the length
    # of a token+features string.
    MAX_TF_LEN = 8140

    # Clear the local statistics counters:
    sentenceCount = 0     # Number of input "sentences" -- e.g., ads
    sentenceTokenCount = 0 # Number of input tokens -- words, punctuation, whatever
    taggedPhraseCount = 0 # Number of tagged output phrases
    taggedTokenCount = 0  # Number of tagged output tokens
    filterInputCount = 0
    filterAcceptCount = 0
    filterRejectCount = 0

    for sentence in sentences:
        sentenceCount += 1

        try:
            tokens = sentence.getTokens()
        except ValueError:
            # TODO: Need better logging here. On a Spark worker it gets lost.
            print "Error getting tokens for sentence %d" % sentenceCount
            tokens = []
            pass # Try to keep on going

        if len(tokens) == 0:
            continue

        sentenceTokenCount += len(tokens)
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
            tfstr = tf.encode('utf-8')
            # TODO: complain when this limit is exceeded.
            if len(tfstr) < MAX_TF_LEN:
                tagger.add(tfstr)

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
                    if resultFilter is None:
                        accept = True
                    else:
                        filterInputCount += 1
                        accept = resultFilter(sentence, currentTagName, phraseFirstTokenIdx, phraseTokenCount)
                        if accept:
                            filterAcceptCount += 1
                        else:
                            filterRejectCount += 1
                    if accept:
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
            if resultFilter is None:
                accept = True
            else:
                filterInputCount += 1
                accept = resultFilter(sentence, currentTagName, phraseFirstTokenIdx, phraseTokenCount)
                if accept:
                    filterAcceptCount += 1
                else:
                    filterRejectCount += 1
            if accept:
                yield resultFormatter(sentence, currentTagName, phraseFirstTokenIdx, phraseTokenCount)
                taggedPhraseCount += 1
            # Don't need to do these as we're about to exit the loop:
            # phraseTokenCount = 0
        # currentTagName = UNTAGGED_TAG_NAME

    if statistics:
        statistics["sentenceCount"] += sentenceCount
        statistics["sentenceTokenCount"] += sentenceTokenCount
        statistics["taggedPhraseCount"] += taggedPhraseCount
        statistics["taggedTokenCount"] += taggedTokenCount
        statistics["filterInputCount"] += filterInputCount
        statistics["filterAcceptCount"] += filterAcceptCount
        statistics["filterRejectCount"] += filterRejectCount

class ApplyCrfToSentencesYieldingTaggedPhraseTuples(object):
    """Apply CRF++ to a source of sentences, returning a sequence of tagged phrase
    tuples.  The output tuples could be used to create something similar to the
    MTurk sentence output.

    (sentence, tagName, phraseFirstTokenIndex, phraseTokenCount)

    """

    def __init__(self, featureListFilePath, modelFilePath, debug=False, sumStatistics=False):
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
        self.sumStatistics = sumStatistics

        # Defer creating these objects.  The benefit is better operation with
        # Spark (deferring creating the tagger may be necessary with Spark).
        # The downside is that problems opening the feature list file or the
        # model file are reported later rather than sooner.
        self.crfFeatures = None
        self.tagger = None
        self.filePathMapper = None
        self.resultFilter = None

        self.initializeStatistics()

    def initializeStatistics(self):
        if not self.sumStatistics:
            self.statistics = None
        else:
            self.statistics = { }
            self.statisticNames = ["sentenceCount", "sentenceTokenCount",
                                   "filterInputCount", "filterAcceptCount", "filterRejectCount",
                                   "taggedPhraseCount", "taggedTokenCount"]
            for statName in self.self.statisticNames:
                self.statistics[statName] = 0

    def getStatistics(self):
        return self.statistics

    def setFilePathMapper(self, filePathMapper):
        self.filePathMapper = filePathMapper

    def setResultFilter(self, resultFilter):
        self.resultFilter = resultFilter

    def setupCrfFeatures(self):
        """Create the CRF Features object, if it hasn't been created yet."""
        if self.crfFeatures == None:
            path = self.featureListFilePath

            # Apply file path changes to the CRF Features file.  This may be
            # necessary when running under Spark, for example, where the file
            # path has to be obtained seperately by each worker,
            if self.filePathMapper != None:
                path = self.filePathMapper(path)

            # Create a CrfFeatures object.  This class provides a lot of
            # services, but we'll use only a few.
            if self.debug:
                print "Creating crfFeatures with path: " + path
            self.crfFeatures = crff.CrfFeatures(path)
            if self.debug:
                print "Created crfFeatures."

    def setupCrfTagger(self):
        """Create the CRF++ Tagger object, if it hasn't been created yet."""
        if self.tagger == None:
            path = self.modelFilePath

            # Apply file path changes to the CRF Model file.  This may be
            # necessary when running under Spark, for example, where the file
            # path has to be obtained seperately by each worker,
            if self.filePathMapper != None:
                path = self.filePathMapper(path)

            # Create a CRF++ processor object:
            if self.debug:
                print "Creating CRFPP tagger with path: " + path
            self.tagger = CRFPP.Tagger(str("-m " + path))
            if self.debug:
                print"Created CRFPP tagger."

    def setup(self):
        """Create the CRF Features and CRF++ Tagger objects, if they haven't been created yet."""
        self.setupCrfFeatures()
        self.setupCrfTagger()

    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Pass the result tuples through."""
        return sentence, tagName, phraseFirstTokenIdx, phraseTokenCount

    def process(self, sentences):
        """Return a generator to process the sentences from the source.  This method may be called multiple times to process multiple sources."""
        self.setup() # Create the CRF Features and Tagger objects if necessary.
        return applyCrfGenerator(sentences, crfFeatures=self.crfFeatures, tagger=self.tagger,
                                 resultFilter=self.resultFilter, resultFormatter=self.resultFormatter,
                                 debug=self.debug, statistics=self.statistics)

    def showStatistics(self):
        if self.statistics:
            for statName in self.statisticNames:
                print "%s = %d" % (statName, self.statistics[statName])

class ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines (ApplyCrfToSentencesYieldingTaggedPhraseTuples):
    """Apply CRF++ to a source of sentences, returning a sequence of keys and
    tagged phrase structures.  The tagged phrase structures are encoded as JSON Lines.

    yields: (key, taggedPhraseJsonLine)

    This class is separate from its parent to allow the easy
    introduction of alternate resultFormatter(...) routines.  For
    example, it may be desirable to carry embed the results in a more
    complex data structure.

    """
    def __init__(self, featureListFilePath, modelFilePath, hybridJaccardConfigPath,
                 embedKey=None, debug=False, sumStatistics=False):
        super(ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines, self).__init__(featureListFilePath, modelFilePath, debug, sumStatistics)
        self.embedKey = embedKey
        self.configureHybridJaccard(hybridJaccardConfigPath)

    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Extract the tagged phrases and format the result as keys and tagged phrase Json Lines."""
        phrase = sentence.getFilteredPhrase()
        if not phrase:
            phrase = sentence.getTokens()[phraseFirstTokenIdx:(phraseFirstTokenIdx+phraseTokenCount)]
        taggedPhrase = { }
        taggedPhrase[tagName] = phrase
        key = sentence.getKey()
        if self.embedKey:
            taggedPhrase[self.embedKey] = key
        return key, json.dumps(taggedPhrase, indent=None)

    def getHybridJaccardResultFilter(self, hybridJaccardProcessors):
        """Return a hybrid Jaccard resultFilter with access to hybridJaccardProcessors."""
        def hybridJaccardResultFilter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
            """Apply hybrid Jaccard filtering if a filter has been defined for the current
            tag.  Return True if HJ succeeds or is not applied, else return False."""
            if tagName in hybridJaccardProcessors:
                phrase = sentence.getTokens()[phraseFirstTokenIdx:(phraseFirstTokenIdx+phraseTokenCount)]
                hjResult = hybridJaccardProcessors[tagName].findBestMatchWordsCached(phrase)
                if hjResult is None:
                    return False
                sentence.setFilteredPhrase(hjResult)
            return True
        return hybridJaccardResultFilter

    def configureHybridJaccard(self, hybridJaccardConfigPath):
        # Read the hybrid Jaccard configuration file.  For each tag type
        # mentioned in the file, create a hybridJaccard tagger.
        hybridJaccardProcessors = { }
        with open(hybridJaccardConfigPath) as hybridJaccardConfigFile:
            hybridJaccardConf = json.load(hybridJaccardConfigFile)
            for tagType in hybridJaccardConf:
                hj = hybridJaccard.HybridJaccard(method_type=tagType)
                hj.build_configuration(hybridJaccardConf)
                hybridJaccardProcessors[tagType] = hj
        # Tell the tagger to use hybrid Jaccard result filtering:
        self.setResultFilter(self.getHybridJaccardResultFilter(hybridJaccardProcessors))


class ApplyCrf (ApplyCrfToSentencesYieldingKeysAndTaggedPhraseJsonLines):
    """Apply CRF++ to a source of sentences in keyed, unkeyed, or paired JSON Lines format, returning
a sequence of tagged phrases in keyed JSON Lines format or paired JSON Lines format.

    yields: "key\ttaggedPhraseJsonLine"
        or: (key, taggedPhraseJsonLine)

    """
    def __init__ (self, featureListFilePath, modelFilePath, hybridJaccardConfigPath,
                  inputPairs=False, inputKeyed=False, inputJustTokens=False, extractFrom=None,
                  outputPairs=False, embedKey=None,
                  debug=False, sumStatistics=False):
        super(ApplyCrf, self).__init__(featureListFilePath, modelFilePath, hybridJaccardConfigPath, embedKey, debug, sumStatistics)
        self.inputPairs = inputPairs
        self.inputKeyed = inputKeyed
        self.inputJustTokens = inputJustTokens
        self.outputPairs = outputPairs
        self.extractFrom = extractFrom

    def resultFormatter(self, sentence, tagName, phraseFirstTokenIdx, phraseTokenCount):
        """Format the result as keyed or paired Json Lines."""
        key, taggedPhraseJsonLine = super(ApplyCrf, self).resultFormatter(sentence, tagName, phraseFirstTokenIdx, phraseTokenCount)
        # TODO: Optimize, why perform this test on each record?
        if self.outputPairs:
            return key, taggedPhraseJsonLine
        elif self.embedKey != None:
            return taggedPhraseJsonLine
        else:
            return key + '\t' + taggedPhraseJsonLine

    def process(self, source):
        """Return a generator to process a sequence of sentences from the source.  The
source presents the sentences in keyed sentence JSON Lines format, paired
sentence JSON Lines format, or other choices.  This method may be called
multiple times to process multiple sources.

        """
        sentences = crfs.CrfSentencesFromJsonLinesSource(source, pairs=self.inputPairs, keyed=self.inputKeyed, justTokens=self.inputJustTokens, extractFrom=self.extractFrom)
        return super(ApplyCrf, self).process(sentences)
