#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
from hybridJaccard import HybridJaccard
import argparse
import json
import cgi
from htmltoken import tokenize
import crf_features
from base64 import b64encode, b64decode

# configDir = os.path.join(os.path.dirname(__file__), "data/config")
# def configPath(n):
#     return os.path.join(configDir, n)
# smHairColor = HybridJaccard(ref_path=configPath("hairColor_reference_wiki.txt"),
#                            config_path=configPath("hairColor_config.txt"))
# print smHairColor.findBestMatch("redhead")
# exit(0)

# import snakebite for doing hdfs file manipulations
from snakebite.client import Client
from snakebite.errors import FileNotFoundException

def extract_body(main_json):
    try:
        return main_json["hasBodyPart"]["text"]
    except:
        pass

def extract_title(main_json):
    try:
        return main_json["hasTitlePart"]["text"]
    except:
        pass

def textTokens(texts):
    # Turn None into empty text 
    texts = texts or ""
    # Allow for multiple texts
    texts = texts if isinstance(texts, list) else [texts]
    v = []
    for text in texts:
        try:
            for tok in genescaped(text):
                v.append([tok])
        except TypeError as e:
            print >> sys.stderr, "Error %s" % e
            print >> sys.stderr, type(text)
            print >> sys.stderr, "Computing textTokens of %s: %s" % (text, e)
        v.append("")
    return v

def genescaped(text, maxTokenLength=40):
    """All tokens in TEXT with any odd characters (such as <>&) encoded using HTML escaping"""
    for tok in tokenize(text, interpret=cgi.escape, keepTags=False):
        # Some ads have odd tokens like 1000 As in a row
        if len(tok) <= maxTokenLength:
            # yield tok
            yield tok.replace('\t', ' ')

### The URI + index mechanism isn't good enough to recover data when there are multiple sentences per URI
### which can occur from (a) title + body as separate documents (b) sentence breaking of body
### The intra-sentence space rows won't be maintained by reconstructTuple + groupBy
### Proposed future workarounds:
### (a) sentence-specific temporary URIs ending /processed/title or /processed/1
### (b) and/or sentence-specific indexing 1.1, 1.2, ... 1.N, 2.1, etc. could work

def reconstructTuple(tabsep):
    """Works only for old multi-line invocation of crf_test, not crf_test_b64"""
    fields = tabsep.split('\t')
    try:
        uri = fields[-3]
        idx = fields[-2]
    except:
        uri = "endOfDocument"
        idx = "0"
    return (uri, [idx] + fields)

def fakeFindBestMatch(words):
    if 'blue' in words:
        return 'blue'
    else:
        return 'NONE'

def alignToControlledVocab(harvested, vocabs):
    """harvested is a dict matching category 'eyeColor', 'hairType' to a function"""
    try:
        category = harvested['category']
        # f = vocabs[category]
        f = fakeFindBestMatch
        words = harvested['words']
        try:
            harvested['bestMatch'] = f(words)
        except Exception as e:
            return "sm.findBestMatch error:" + str(e) + ":" + str(words)
        return harvested
    except Exception as e:
        return str(e)
    return None

def vectorToUTF8(v, debug=False):
    "unicode only"

    def rowToUnicode(r):
        try:
            if isinstance(r, list):
                return u"\t".join([unicode(x) for x in r])
            else:
                return unicode(r)
        except:
            print >> sys.stderr, "error in rowToUnicode"
            return u""

    rows = []
    if v[-1] == u"":
        pass
    else:
        # print "appending1"
        v.append(u"")

    for r in v:
        rows.append(rowToUnicode(r))

    result = u"\n".join(rows)
    # result now a unicode object
    # here is the only place where we convert to UTF8
    return result.encode('utf-8')

### HISTORICAL
def computeSpans(v, verbose=False, indexed=False):
    # extract the data and write the result as vector
    currentLabel = None
    currentTokens = []
    spans = []
    def addSpan(u, l, words):
        spans.append( {"uri": u, "category": l, "words": " ".join(words) } )
        if verbose:
            print >> sys.stderr, "  Added %s" % (spans[-1],)

    uri = 'bogus'
    currentUri = None
    for row in v:
        if (len(row) <= 1):
            # blank/empty line: expecting "" but might be []
            if currentLabel and currentTokens:
                addSpan(currentUri, currentLabel, currentTokens)
            currentUri = None
            continue
        if (len(row) >= 4):
            # a typical row
            token = row[0]
            uri = row[-3] if indexed else row[-2]
            label = row[-1]
            if verbose:
                print >> sys.stderr, "Typical row: token %r uri %r: crflabel %r" % (token, uri, label)
            # now process this row
            if label == "O":
                # unlabeled row
                if currentLabel:
                    # so this concludes span in progress
                    addSpan(uri, currentLabel, currentTokens)
                    currentLabel = None
                    currentTokens = []
                else:
                    pass
            else:
                # Labeled row
                if label == currentLabel:
                    # continue span in progress
                    currentTokens.append(token)
                elif currentLabel and label != currentLabel:
                    # span/span boundary
                    # first conclude old one
                    addSpan(uri, currentLabel, currentTokens)
                    currentLabel = None
                    currentTokens = []
                    # then begin new one
                    currentLabel = label
                    currentTokens = [token]
                elif not currentLabel:
                    # begin novel span
                    currentLabel = label
                    currentTokens = [token]
                else:
                    raise Exception("Unexpected file structure")
        currentUri = uri

    if currentLabel and currentTokens:
        # Input ended without blank line after last marked span, so hallucinate one
        addSpan(uri, currentLabel, currentTokens)

    # Publish results
    return spans

def crfsmall(sc, input, output, 
            limit=None, location='hdfs', outputFormat="text", numPartitions=None):

    configDir = os.path.join(os.path.dirname(__file__), "data/config")
    def configPath(n):
        return os.path.join(configDir, n)
    binDir = os.path.join(os.path.dirname(__file__), "bin")
    def binPath(n):
        return os.path.join(binDir, n)

    featureListFilename = configPath("features.hair-eye")
    crfExecutable = binPath("crf_test_filter.sh")
    crfModelFilename = configPath("dig-hair-eye-train.model")
    sc.addFile(crfExecutable)
    sc.addFile(crfModelFilename)

    rdd_crfl = sc.sequenceFile(input)
    rdd_crfl.setName('rdd_crfl')

    if limit:
        rdd_crfl = sc.parallelize(rdd_crfl.take(limit))
    if numPartitions:
        rdd_crfl = rdd_crfl.repartition(numPartitions)

    rdd_json = rdd_crfl.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    # rdd_json.persist()

    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')

    c = crf_features.CrfFeatures(featureListFilename)
    SEPARATOR = '&amp;nbsp;',

    def makeMatrix(c, uri, bodyTokens, titleTokens):
        b = c.computeFeatMatrix(bodyTokens, False, addLabels=False, addIndex=False)
        s = c.computeFeatMatrix([SEPARATOR, ""], False, addLabels=False, addIndex=False)
        t = c.computeFeatMatrix(titleTokens, False, addLabels=False, addIndex=False)
        idx = 1
        for row in b:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (0, idx)
                row.append(label)
                idx += 1
        idx = 1
        for row in s:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (1, idx)
                row.append(label)
                idx += 1
        idx = 1
        for row in t:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (2, idx)
                row.append(label)
                idx += 1
        # might be b[0:-1] + s[0:-1] + t?
        return b[0:-1] + s[0:-1] + t

    rdd_features = rdd_texts.map(lambda x: makeMatrix(c, x[0], x[1][0], x[1][1]))
    rdd_features.setName('rdd_features')
    # rdd_features.persist()

    # unicode/string representation of the feature matrix
    rdd_vector = rdd_features.map(lambda x: vectorToUTF8(x))
    rdd_vector.setName('rdd_vector')

    # all strings concatenated together, then base64 encoded into one input for crf_test
    rdd_pipeinput = sc.parallelize([b64encode(rdd_vector.reduce(lambda a,b: a+b))])
    rdd_pipeinput.setName('rdd_pipeinput')

    executable = SparkFiles.get(os.path.basename(crfExecutable))
    model = SparkFiles.get(os.path.basename(crfModelFilename))
    
    cmd = "%s %s" % (executable, model)

    # this result is base64 encoded
    rdd_crfoutput = rdd_pipeinput.pipe(cmd)
    rdd_crfoutput.setName('rdd_crfoutput')

    rdd_base64decode = rdd_crfoutput.map(lambda x: b64decode(x))
    ### There may be a need to utf8 decode this data ###
    ### There are values like \xed\xa0\xbd which might be a broken emoji
    # 1. break into physical lines
    # 2. turn each line into its own spark row
    # 3. drop any inter-document empty string markers
    rdd_lines = rdd_base64decode.map(lambda x: x.split("\n")).flatMap(lambda l: l).filter(lambda x: len(x)>1)

    def processOneLine(l):
        return l.split("\t")

    rdd_triples = rdd_lines.map(lambda l: processOneLine(l))
    rdd_triples.saveAsTextFile('out_rdd_triples')

    def organizeByOrigDoc(triple):
        try:
            (word, uri, label) = triple
            (parentUri, docId, wordId) = uri.rsplit('/', 2)
            return ( (parentUri, docId), (wordId, word, label) )
        except Exception as e:
            print >> sys.stderr, "Can't destructure %r: %s" % (triple, e)
            return ()

    rdd_reorg = rdd_triples.map(lambda l: organizeByOrigDoc(l))
    rdd_reorg.saveAsTextFile('out_rdd_reorg')

    rdd_sorted = rdd_reorg.sortByKey()
    rdd_sorted.saveAsTextFile('out_rdd_sorted')

    # each (parentUri, docId) has a sequence of (wordId, word, label)
    # we want to consider them in order (by wordId)

    rdd_grouped = rdd_sorted.groupByKey()

    def harvest(seq):
        allSpans = []
        lastIndex = -2
        lastLabel = None
        currentSpan = []
        for (wordId, word, label) in seq:
            currentIndex = int(wordId)
            if lastIndex+1 == currentIndex and lastLabel == label:
                # continuing current span
                currentSpan.append( (currentIndex, word, label) )
            else:
                # end current span
                if currentSpan:
                    allSpans.append(currentSpan)
                # begin new span
                currentSpan = [ (currentIndex, word, label) ]
                lastLabel = label
            lastIndex = currentIndex

        # end current span
        if currentSpan:
            allSpans.append(currentSpan)
        
        result = []
        for span in allSpans:
            words = []
            spanLabel = None
            for (wordIdx, word, label) in span:
                spanLabel = label
                words.append(word)
            result.append( (' '.join(words), spanLabel) )
        return result
            
    # ( (parentUri, docId), [ (words1, category1), (words2, category2), ... ]
    rdd_harvest = rdd_grouped.mapValues(lambda s: harvest(s))
    rdd_harvest.saveAsTextFile('out_rdd_harvest')

    # rdd_flat = rdd_harvest.flatMap(lambda r: [ (e[0][0], e[1]) for e in r ])
    # rdd_flat = rdd_harvest.map(lambda r: (r[0][0], r[1]))

    # parentUri -> (words, category)
    # we use .distinct() because (e.g.) both title and body might have the same feature
    rdd_flat = rdd_harvest.map(lambda r: (r[0][0], r[1])).flatMapValues(lambda x: x).distinct()
    rdd_flat.saveAsTextFile('out_rdd_flat')

    smEyeColor = HybridJaccard(ref_path=configPath("eyeColor_reference_wiki.txt"),
                               config_path=configPath("eyeColor_config.txt"))
    smHairColor = HybridJaccard(ref_path=configPath("hairColor_reference_wiki.txt"),
                                config_path=configPath("hairColor_config.txt"))
    hybridJaccards = {"eyeColor": smEyeColor.findBestMatch, 
                      "hairType": smHairColor.findBestMatch}
    def jaccard(tpl):
        (words, category) = tpl
        return (category, hybridJaccards[category](words))

    rdd_aligned = rdd_flat.mapValues(lambda x: jaccard(x))
    rdd_aligned.saveAsTextFile('out_rdd_aligned')

    rdd_final = rdd_aligned

    if outputFormat == "sequence":
        rdd_final.saveAsSequenceFile(output)
    elif outputFormat == "text":
        rdd_final.saveAsTextFile(output)
    else:
        raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int)
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    args=parser.parse_args()

    location = "hdfs"
    try:
        if "avatar" in platform.node():
            location = "local"
    except:
        pass
    try:
        if "avatar" in socket.gethostname():
            location = "local"
    except:
        pass
    print "### location %s" % location

    if not args.numPartitions:
        if location == "local":
            args.numPartitions = 2
        elif location == "hdfs":
            args.numPartitions = 50

    sc = SparkContext(appName="crfsmall")
    crfsmall(sc, args.input, args.output, 
             limit=args.limit, 
             location=location,
             outputFormat="text",
             numPartitions=args.numPartitions)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
