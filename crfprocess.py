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
from random import randint
from collections import defaultdict
import pprint

"""We could build this (v1) ES JSON directly or provide enough info for Karma to do it:

         {
            "_index": "dig-ht-pilot-unfiltered04",
            "_type": "WebPage",
            "_id": "http://dig.isi.edu/ht/data/page/31ED1D4F8A85FBB6564602F5F76315A7AD5B7455/1415454378000/processed",
            "_score": 1,
            "_source": {
               "@context": "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/master/datasets/istr/context-for-istr-datasets.json",
               "dateModified": "2014-11-08T21:46:18",
               "a": "WebPage",
               "hasFeatureCollection": {
                  "a": "FeatureCollection",
                  "person_haircolor_feature": {
                     "featureName": "person_haircolor",
                     "person_haircolor": "brown",
                     "a": "Feature",
                     "wasGeneratedBy": {
                        "databaseId": "400319347",
                        "wasAttributedTo": "http://dig.isi.edu/ht/data/software/extractor/ist/attributes/version/unknown",
                        "a": "Activity",
                        "endedAtTime": "2014-11-08T21:46:18"
                     },
                     "wasDerivedFrom": "http://dig.isi.edu/ht/data/page/31ED1D4F8A85FBB6564602F5F76315A7AD5B7455/1415454378000/processed",
                     "uri": "http://dig.isi.edu/ht/data/page/31ED1D4F8A85FBB6564602F5F76315A7AD5B7455/1415454378000/processed/featurecollection/person_haircolor/brown",
                     "featureValue": "brown"
                  },
"""

def extract_body(main_json):
    try:
        text = main_json["hasBodyPart"]["text"]
        return text
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

configDir = os.getcwd() # os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

def crfprocess(sc, input, output, 
               featureListFilename=configPath('features.hair-eye'),
               modelFilename=configPath('dig-hair-eye-train.model'),
               jaccardSpecs=[],
               limit=None, debug=False, location='hdfs', outputFormat="text", numPartitions=None, hexDigits=3):

    binDir = os.getcwd() # os.path.join(os.path.dirname(__file__), "bin")
    def binPath(n):
        return os.path.join(binDir, n)

    if debug:
        debugOutput = output + '_debug'
    def debugDump(rdd):
        if debug:
            outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
            rdd.saveAsTextFile(outdir)

    crfFeatureListFilename = featureListFilename
    crfModelFilename = modelFilename
    crfExecutable = binPath("crf_test_filter.sh")
    sc.addFile(crfExecutable)
    sc.addFile(crfModelFilename)

    # pageUri -> content
    rdd_crfl = sc.sequenceFile(input)
    rdd_crfl.setName('rdd_crfl')
    if limit:
        rdd_crfl = sc.parallelize(rdd_crfl.take(limit))
    if numPartitions:
        rdd_crfl = rdd_crfl.repartition(numPartitions)
    # For debugging
    # If set, only those URIs so listed are used, everything else is rejected
    keepUris = []
    # keepUris = ['http://dig.isi.edu/ht/data/page/442EA3A8B9FF69D65BC8B0D205C8C85204A7C799/1433150174000/processed']
    if keepUris:
        rdd_crfl = rdd_crfl.filter(lambda (k,v): k in keepUris)
    debugDump(rdd_crfl)
    print "### Processing %d input pages, initially into %s partitions" % (rdd_crfl.count(), rdd_crfl.getNumPartitions())
    exit(0)
    
    # pageUri -> dict from json
    rdd_json = rdd_crfl.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    debugDump(rdd_json)

    # pageUri -> (body tokens, title tokens)
    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')
    debugDump(rdd_texts)

    # We use the following encoding for values for CRF++'s so-called
    # labels to reduce the data size and complexity of referring to
    # words.  Each word is assigned a URI constructed from the page
    # URI (Karma URI) plus a 5 digit zero-padded number for the
    # subdocument plus a 5 digit zero-padded number for the word
    # index (1-based).  By "subdocument" we mean the page body and
    # page title (for HT; there could be others in other domains).
    # Additionally, an artificial "separator" document is used to
    # generate a barrier to avoid inadvertent capture of spurious
    # spans between subdocuments.
    #
    # Example: the first word of the body of
    # http://dig.isi.edu/ht/data/page/0434CB3BDFE3839D6CAC6DBE0EBED0278D3634D8/1433149274000/processed
    # would be http://dig.isi.edu/ht/data/page/0434CB3BDFE3839D6CAC6DBE0EBED0278D3634D8/1433149274000/processed/00000/00001

    SEPARATOR = '&amp;nbsp;'
    BODY_SUBDOCUMENT = 0
    SEPARATOR_SUBDOCUMENT = 1
    TITLE_SUBDOCUMENT = 2
    c = crf_features.CrfFeatures(crfFeatureListFilename)

    def makeMatrix(c, uri, bodyTokens, titleTokens):
        b = c.computeFeatMatrix(bodyTokens, False, addLabels=False, addIndex=False)
        s = c.computeFeatMatrix([SEPARATOR, ""], False, addLabels=False, addIndex=False)
        t = c.computeFeatMatrix(titleTokens, False, addLabels=False, addIndex=False)
        idx = 1
        for row in b:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (BODY_SUBDOCUMENT, idx)
                row.append(label)
                idx += 1
        idx = 1
        for row in s:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (SEPARATOR_SUBDOCUMENT, idx)
                row.append(label)
                idx += 1
        idx = 1
        for row in t:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (TITLE_SUBDOCUMENT, idx)
                row.append(label)
                idx += 1
        # Keep the empty string semaphor from the title (last
        # component) for CRF++ purposes
        return b[0:-1] + s[0:-1] + t

    # page feature matrix including body, separator, title
    # (vector of vectors, includes separator rows)
    # rdd_features = rdd_texts.map(lambda x: makeMatrix(c, x[0], x[1][0], x[1][1]))
    # pageUri -> (python) vector of vectors
    rdd_features = rdd_texts.map(lambda (k,v): (k, makeMatrix(c, k, v[0], v[1])))
    rdd_features.setName('rdd_features')
    debugDump(rdd_features)

    # unicode UTF-8 representation of the feature matrix
    # pageUri -> unicode UTF-8 representation of the feature matrix
    rdd_vector = rdd_features.mapValues(lambda x: vectorToUTF8(x))
    rdd_vector.setName('rdd_vector')
    debugDump(rdd_vector)

    def generatePrefixKey(uri, hexDigits=hexDigits):
        """http://dig.isi.edu/ht/data/page/0040378735A6B350D3B2F639FF4EE72AE4956171/1433150471000/processed"""
        words = uri.split('/')
        # first 6 fields + prefix of 7th field using hexDigits
        # [u'http:', u'', u'dig.isi.edu', u'ht', u'data', u'page', u'004']
        return "/".join(words[0:6] + [words[6][0:hexDigits]])

    # prefixUri -> (<full word uri>, serialized representation of one document)
    rdd_prefixed_unsorted = rdd_vector.map(lambda (k,v): (generatePrefixKey(k), (k, v) ))
    rdd_prefixed_unsorted.setName('rdd_prefixed_unsorted')
    debugDump(rdd_prefixed_unsorted)

    # performing a full sort now will put group contiguous prefixes; within which order by word index
    # SORTED e.g. prefixUri -> (<full word uri>, serialized representation of one document)
    rdd_prefixed_sorted = rdd_prefixed_unsorted.sortBy(lambda x: x)
    rdd_prefixed_sorted.setName('rdd_prefixed_sorted')
    debugDump(rdd_prefixed_sorted)

    # aggregate scheme:
    # result type U is string
    # input type V is tuple
    # merge V into U is lambda v,u: u+v[1]
    # merge U1 and U2 us lambda u1,u2: u1+u2
    # prefixUri -> serialized representations of all documents with that prefix, in order, concatenated
    rdd_concatenated = rdd_prefixed_sorted.aggregateByKey("", lambda u,v: u+v[1], lambda u1,u2: u1+u2)
    rdd_concatenated.setName('rdd_concatenated')
    debugDump(rdd_concatenated)

    # Note: keys are stored here in master, not an RDD.  Is this scalable?
    prefixKeys = rdd_concatenated.keys().distinct().sortBy(lambda x: x).collect()
    prefixKeyCount = len(prefixKeys)
    prefixKeyMap = dict(zip(prefixKeys, range(prefixKeyCount)))

    print "### Repartitioning to %d prefix keys (of maximum %d) based on prefix size %d" % (prefixKeyCount, 16**hexDigits, hexDigits)
    def partitionPerPrefix(k):
        return prefixKeyMap[k]
    
    rdd_prefixedToPayload = rdd_concatenated.repartitionAndSortWithinPartitions(numPartitions=prefixKeyCount,
                                                                                partitionFunc=partitionPerPrefix)
    rdd_prefixedToPayload.setName('rdd_prefixedToPayload')
    debugDump(rdd_prefixedToPayload)

    # all strings concatenated together, then base64 encoded into one input for crf_test
    # rdd_pipeinput = sc.parallelize([b64encode(rdd_vector.reduce(lambda a,b: a+b))])
    # strip off the k, b64encode the v
    rdd_pipeinput = rdd_prefixedToPayload.map(lambda (k,v): b64encode(v))
    rdd_pipeinput.setName('rdd_pipeinput')
    debugDump(rdd_pipeinput)

    # base64 encoded result of running crf_test and filtering to
    # include only word, wordUri, non-null label
    # local
    executable = SparkFiles.get(os.path.basename(crfExecutable))
    # hdfs
    executable = os.path.basename(crfExecutable)
    # local
    model = SparkFiles.get(os.path.basename(crfModelFilename))
    # hdfs
    model = os.path.basename(crfModelFilename)
    cmd = "%s %s" % (executable, model)
    print >> sys.stderr, "Pipe cmd is %r" % cmd
    rdd_pipeoutput = rdd_pipeinput.pipe(cmd)
    rdd_pipeoutput.setName('rdd_pipeoutput')
    debugDump(rdd_pipeoutput)

    # base64 decoded to regular serialized string
    #### MIGHT INTRODUCE EXTRA NEWLINES WHEN INPUT IS EMPTY(?)
    rdd_base64decode = rdd_pipeoutput.map(lambda x: b64decode(x))
    rdd_base64decode.setName('rdd_base64decode')
    debugDump(rdd_base64decode)

    ### There may be a need to utf8 decode this data ###
    # 1. break into physical lines
    # 2. turn each line into its own spark row
    # 3. drop any inter-document empty string markers
    rdd_lines = rdd_base64decode.map(lambda x: x.split("\n")).flatMap(lambda l: l).filter(lambda x: len(x)>1)
    rdd_lines.setName('rdd_lines')
    debugDump(rdd_lines)

    def processOneLine(l):
        return l.split("\t")
    rdd_triples = rdd_lines.map(lambda l: processOneLine(l))
    rdd_triples.setName('rdd_triples')
    debugDump(rdd_triples)

    def organizeByOrigDoc(triple):
        try:
            (word, uri, label) = triple
            (parentUri, docId, wordId) = uri.rsplit('/', 2)
            return ( (parentUri, docId), (wordId, word, label) )
        except Exception as e:
            print >> sys.stderr, "Can't destructure %r: %s" % (triple, e)
            return ()

    rdd_reorg = rdd_triples.map(lambda l: organizeByOrigDoc(l))
    rdd_reorg.setName('rdd_reorg')
    debugDump(rdd_reorg)

    rdd_sorted = rdd_reorg.sortByKey()
    rdd_sorted.setName('rdd_sorted')
    debugDump(rdd_sorted)

    # each (parentUri, docId) has a sequence of (wordId, word, label)
    # we want to consider them in order (by wordId)

    rdd_grouped = rdd_sorted.groupByKey()
    rdd_grouped.setName('rdd_grouped')
    debugDump(rdd_grouped)

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
    rdd_harvest.setName('rdd_harvest')
    debugDump(rdd_harvest)

    # parentUri -> (words, category)
    # we use .distinct() because (e.g.) both title and body might have the same feature
    rdd_flat = rdd_harvest.map(lambda r: (r[0][0], r[1])).flatMapValues(lambda x: x).distinct()
    rdd_flat.setName('rdd_flat')
    debugDump(rdd_flat)

    ## We map from CRF output (category) to (potentially multiple) HJ handle(s)

    hjHandlers = defaultdict(list)
    for (category,digFeature,config,reference) in jaccardSpecs:
        # add one handler
        hjHandlers[category].append({"category": category,
                                     "featureName": digFeature,
                                     "hybridJaccardInterpreter": HybridJaccard(config_path=config, ref_path=reference).findBestMatch})

    def jaccard(tpl):
        results = []
        (words, category) = tpl
        for handler in hjHandlers[category]:
            results.append({"featureName": handler["featureName"],
                            "featureValue": handler["hybridJaccardInterpreter"](words),
                            # for debugging
                            "crfCategory": category,
                            "crfWordSpan": words,
                            # intended to support parametrization/provenance
                            "featureDefinitionFile": os.path.basename(crfFeatureListFilename),
                            "featureCrfModelFile": os.path.basename(crfModelFilename)})
        return results

    # potentially there could be more than one interpretation, e.g. hairColor + hairType
    # is this a problem
    rdd_aligned = rdd_flat.mapValues(lambda v: jaccard(v))
    rdd_aligned.setName('rdd_aligned')
    debugDump(rdd_aligned)

    rdd_final = rdd_aligned.mapValues(lambda v: json.dumps(v))
    rdd_final.setName('rdd_final')
    debugDump(rdd_final)

    if rdd_final.count() > 0:
        if outputFormat == "sequence":
            rdd_final.saveAsSequenceFile(output)
        elif outputFormat == "text":
            rdd_final.saveAsTextFile(output)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)
    else:
        print "### NO DATA TO WRITE"

def defaultJaccardSpec():
    l = [["eyeColor", "person_eyecolor", configPath("eyeColor_config.txt"), configPath("eyeColor_reference_wiki.txt")],
         ["hairType", "person_haircolor", configPath("hairColor_config.txt"), configPath("hairColor_reference_wiki.txt")]]
    return [",".join(x) for x in l]

def jaccardSpec(s):
    "pair of existing files separated by comma"
    try:
        (category,digFeature,cfg,ref) = s.split(',')
        if category and digFeature and os.path.exists(cfg) and os.path.exists(ref):
            return s
    except:
        pass
    raise argparse.ArgumentError("Unrecognized jaccard spec <category,digFeature,configFile,referenceFile> %r" % s)

def main(argv=None):
    '''this is called if run from command line'''
    pprint.pprint(os.listdir(os.getcwd()))
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-f','--featureListFilename', default=configPath('features.hair-eye'))
    parser.add_argument('-m','--modelFilename', default=configPath('dig-hair-eye-train.model'))
    parser.add_argument('-j','--jaccardSpec', action='append', default=[], type=jaccardSpec,
                        help='each value should be <category,featureName,config.json,reference.txt>')
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int)
    parser.add_argument('-d','--hexDigits', required=False, default=3, type=int)
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', action='store_true')
    args=parser.parse_args()

    if args.jaccardSpec == []:
        args.jaccardSpec = defaultJaccardSpec()
    pprint.pprint(args.jaccardSpec)

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

    sc = SparkContext(appName="crfprocess")
    crfprocess(sc, args.input, args.output, 
               featureListFilename=args.featureListFilename,
               modelFilename=args.modelFilename,
               jaccardSpecs=[j.split(',') for j in args.jaccardSpec],
               debug=args.debug,
               limit=args.limit,
               location=location,
               outputFormat="sequence",
               numPartitions=args.numPartitions,
               hexDigits=args.hexDigits)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# STATS with 4.2Mb input, featureFile=features.hair-eye, model=dig-hair-eye-train.model, featureCount=
# With hexDigits=1 (16 partition buckets), 1m33.653s
# With hexDigits=2 (256 partition buckets), 1m51.499s
# With hexDigits=3 (4096 partition buckets), 3m42.185s
