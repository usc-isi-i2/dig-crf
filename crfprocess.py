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
from itertools import izip_longest
import time
from datetime import timedelta

### from util.py

def iterChunks(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

### end from util.py

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

# Sniff for execution environment

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


configDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

binDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "bin")
def binPath(n):
    return os.path.join(binDir, n)

def crfprocess(sc, input, output, 
               featureListFilename=configPath('features.hair-eye'),
               modelFilename=configPath('dig-hair-eye-train.model'),
               jaccardSpecs=[],
               limit=None, debug=False, location='hdfs', outputFormat="text", numPartitions=None, chunksPerPartition=100, hexDigits=2):

    debugOutput = output + '_debug'
    def debugDump(rdd,keys=True,listElements=False):
        keys=False
        if debug:
            startTime = time.time()
            outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
            keyCount = None
            try:
                keyCount = rdd.keys().count() if keys else None
            except:
                pass
            rowCount = None
            try:
                rowCount = rdd.count()
            except:
                pass
            elementCount = None
            try:
                elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
            except:
                pass
            rdd.saveAsTextFile(outdir)
            endTime = time.time()
            elapsedTime = endTime - startTime
            print ### "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

    def goodbye(rdd):
        try:
            k = rdd.count()
        except:
            k = None
        print "Just finished %s with size %s" % (rdd.name(), k)
        exit(0)

    def showPartitioning(rdd):
        print "At %s, there are %d partitions" % (rdd.name(), rdd.getNumPartitions())

    crfFeatureListFilename = featureListFilename
    crfModelFilename = modelFilename
    crfExecutable = binPath("crf_test_filter.sh")
    crfExecutable = binPath("crf_test_filter_lines.sh")
    crfExecutable = "apply_crf_lines.py"
    sc.addFile(crfExecutable)
    sc.addFile(crfModelFilename)

    # layout: pageUri -> content
    rdd_crfl = sc.sequenceFile(input, minSplits=256)
    rdd_crfl.setName('rdd_crfl_input')
    showPartitioning(rdd_crfl)
    if limit==0:
        limit = None

    samplePrefix = False
    sampleSeed = 1234
    if limit:
        if samplePrefix:
            # Because this collects back to master, can create "task too large" condition
            rdd_crfl = sc.parallelize(rdd_crfl.take(limit))
        else:
            # Instead, generate approximately 'limit' rows
            ratio = float(limit) / rdd_crfl.count()
            rdd_crfl = rdd_crfl.sample(False, ratio, seed=sampleSeed)
        
# IGNORE -p for now, all partitioning is algorithmic
#     if numPartitions:
#         rdd_crfl = rdd_crfl.repartition(numPartitions)

    # For debugging, allow inclusion/exclusion of items with known behavior
    # If set, only those URIs so listed are used, everything else is rejected
    keepUris = []
    # contains both hair and eyes
    # keepUris.append('http://dig.isi.edu/ht/data/page/2384EBCB1DD4FCA505DD05AB15F386547D05B295/1429603739000/processed')
    # contains both hair and eyes
    # keepUris.append('http://dig.isi.edu/ht/data/page/18507EEC7DD0A94A3A00F46D8B976CDFDD258723/1429603859000/processed')
    # contains both hair and eyeys
    # keepUris.append('http://dig.isi.edu/ht/data/page/442EA3A8B9FF69D65BC8B0D205C8C85204A7C799/1433150174000/processed')
    # for testing 'curly hair'
    # keepUris.append('http://dig.isi.edu/ht/data/page/681A3E68456987B1EE11616280DC1DBBA5A6B754/1429606198000/processed')
    if keepUris:
        rdd_crfl = rdd_crfl.filter(lambda (k,v): k in keepUris)
    rdd_crfl.setName('rdd_crfl')
    debugDump(rdd_crfl)
    showPartitioning(rdd_crfl)

    # layout: pageUri -> dict (from json)
    rdd_json = rdd_crfl.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    debugDump(rdd_json)

#     rdd_json.repartition(256)

#     partitionWidth = hexDigits
#     print "partioning per %s" % partitionWidth
#     def partitionByUriSha1(k, width=partitionWidth):
#         """input e.g, http://dig.isi.edu/ht/data/page/001283889C1E211B50C81F7361457CC2C94E495F/1429603858000/processed, 3
# output 1"""
#         try:
#             # 37 seconds
#             # return int(k.split('/')[6][0:width], 16)
#             # 45 seconds
#             # return int(k.split('/')[6], 16)
#             # 40 seconds
#             return hash(k)
#         except:
#             return 0

#     # layout: pageUri -> sorted dict (from json) (partitioned based on prefix of sha1 portion of pageUri)
# #     rdd_partitioned = rdd_json.repartitionAndSortWithinPartitions(numPartitions=16**partitionWidth,
# #                                                                   partitionFunc=lambda k: partitionByUriSha1(k))
#     rdd_partitioned = rdd_json.repartition(256)
#     rdd_partitioned.setName('rdd_partitioned')
#     debugDump(rdd_partitioned)
#     showPartitioning(rdd_partitioned)

#     rdd_json = rdd_partitioned

    # print "### Processing %d input pages, initially into %s partitions" % (rdd_partitioned.count(), rdd_partitioned.getNumPartitions())
    # layout: pageUri -> (body tokens, title tokens)
    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')
    # rdd_texts.persist()
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
        # BODY
        idx = 1
        for row in b:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (BODY_SUBDOCUMENT, idx)
                row.append(label)
                idx += 1
        # SEPARATOR pseudo document
        idx = 1
        for row in s:
            if row == u"":
                pass
            else:
                label = uri + "/%05d/%05d" % (SEPARATOR_SUBDOCUMENT, idx)
                row.append(label)
                idx += 1
        # TITLE
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
    # layout: pageUri -> (python) vector of vectors
    rdd_features = rdd_texts.map(lambda (k,v): (k, makeMatrix(c, k, v[0], v[1])))
    rdd_features.setName('rdd_features')
    debugDump(rdd_features)

    # unicode UTF-8 representation of the feature matrix
    # layout: pageUri -> unicode UTF-8 representation of the feature matrix
    rdd_vector = rdd_features.mapValues(lambda x: vectorToUTF8(x))
    rdd_vector.setName('rdd_vector')
    debugDump(rdd_vector)

    # Disregard keys/partitioning considerations here
    # Drop keys put serialized vectors into lists of size chunksPerPartition, dropping any nulls, then concatenate

    # layout: lists of size up to chunksPerPartition of UTF8(feature vectors)
    rdd_chunked = rdd_vector.values().glom().map(lambda l: [filter(lambda e: e, x) for x in iterChunks(l, chunksPerPartition)]).map(lambda l: ["".join(x) for x in l])
    rdd_chunked.setName('rdd_chunked')
    debugDump(rdd_chunked, keys=False)

    rdd_pipeinput = rdd_chunked.flatMap(lambda x: x).map(lambda r: b64encode(r))
    rdd_pipeinput.setName('rdd_pipeinput')
    debugDump(rdd_pipeinput, keys=False)
    showPartitioning(rdd_pipeinput)

    # base64 encoded result of running crf_test and filtering to
    # include only word, wordUri, non-null label
    # local
    executable = SparkFiles.get(os.path.basename(crfExecutable)) if location=="local" else os.path.basename(crfExecutable)
    # local
    model = SparkFiles.get(os.path.basename(crfModelFilename)) if location=="local" else os.path.basename(crfModelFilename)
    cmd = "%s %s" % (executable, model)
    print "### Pipe cmd is %r" % cmd

    rdd_pipeoutput = rdd_pipeinput.pipe(cmd)
    rdd_pipeoutput.setName('rdd_pipeoutput')
    debugDump(rdd_pipeoutput)
    showPartitioning(rdd_pipeoutput)

    # base64 decoded to regular serialized string
    # beware newlines corresponding to empty CRr++ crf_test output 
    # There may be a need to utf8 decode this data upon reacquisition, but believed not
    rdd_base64decode = rdd_pipeoutput.map(lambda x: b64decode(x))
    rdd_base64decode.setName('rdd_base64decode')
    debugDump(rdd_base64decode)

    def reorg(tabsep):
        (word, uri, label) = tabsep.split('\t')
        return (uri, (word, label))

    # 1. break into physical lines
    # 2. drop any inter-document empty string markers
    # 3. destructure each line into its own word, wordUri, label row
    # wordUri -> (word,label)
    rdd_tabular = rdd_base64decode.map(lambda b: b.split('\n')).flatMap(lambda x: x).filter(lambda x: x).map(lambda l: reorg(l))
    rdd_tabular.setName('rdd_tabular')
    debugDump(rdd_tabular)

    def organizeByOrigDoc(uri, word, label):
        (parentUri, docId, wordId) = uri.rsplit('/', 2)
        return ( (parentUri, docId), (wordId, word, label) )

    # composite key (docUri, subdocId) -> (wordId, word, label)
    rdd_reorg = rdd_tabular.map(lambda (uri,tpl): organizeByOrigDoc(uri, tpl[0], tpl[1]))
    rdd_reorg.setName('rdd_reorg')
    debugDump(rdd_reorg)

    def seqFunc(s,c):
        s.add(c)
        return s

    def combFunc(s1, s2):
        s1.update(s2)
        return s1

    # composite key (docUri, subdocId) -> set of (wordId, word, label)
    rdd_agg = rdd_reorg.aggregateByKey(set(),
                                       lambda s,c: seqFunc(s,c),
                                       lambda s1,s2: combFunc(s1,s2))
    rdd_agg.setName('rdd_agg')
    debugDump(rdd_agg)
    showPartitioning(rdd_agg)

    # (docUri, subDocId) -> sorted list of (wordId, word, label)
    rdd_grouped = rdd_agg.mapValues(lambda s: sorted(s))
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
    # we use .distinct() because (e.g.) both title and body might mention the same feature
    rdd_flat = rdd_harvest.map(lambda r: (r[0][0], r[1])).flatMapValues(lambda x: x).distinct()
    rdd_flat.setName('rdd_flat')
    debugDump(rdd_flat)

    # We map from CRF output (category) to (potentially multiple) HJ handle(s)
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

    # there could be more than one interpretation, e.g. hairColor + hairType for a given CRF category
    # use flatMapValues to iterate over all
    rdd_aligned = rdd_flat.flatMapValues(lambda v: jaccard(v))
    rdd_aligned.setName('rdd_aligned')
    debugDump(rdd_aligned)


    rdd_aligned = rdd_pipeinput
    # docUri -> json
    rdd_final = rdd_aligned.mapValues(lambda v: json.dumps(v))
    rdd_final.setName('rdd_final')
    debugDump(rdd_final)

    if rdd_final.isEmpty():
        print "### NO DATA TO WRITE"
    else:
        if outputFormat == "sequence":
            rdd_final.saveAsSequenceFile(output)
        elif outputFormat == "text":
            rdd_final.saveAsTextFile(output)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def defaultJaccardSpec():
    l = [["eyeColor", "person_eyecolor", configPath("eyeColor_config.txt"), configPath("eyeColor_reference_wiki.txt")],
         ["hairType", "person_haircolor", configPath("hairColor_config.txt"), configPath("hairColor_reference_wiki.txt")],
         ["hairType", "person_hairtexture", configPath("hairTexture_config.txt"), configPath("hairTexture_reference_wiki.txt")]]
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
    # pprint.pprint(sorted(os.listdir(os.getcwd())))
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-f','--featureListFilename', default=configPath('features.hair-eye'))
    parser.add_argument('-m','--modelFilename', default=configPath('dig-hair-eye-train.model'))
    parser.add_argument('-j','--jaccardSpec', action='append', default=[], type=jaccardSpec,
                        help='each value should be <category,featureName,config.json,reference.txt>')
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int)
    parser.add_argument('-c','--chunksPerPartition', required=False, default=100, type=int)
    parser.add_argument('-d','--hexDigits', required=False, default=2, type=int)
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', type=int)
    args=parser.parse_args()

    if args.jaccardSpec == []:
        args.jaccardSpec = defaultJaccardSpec()
    # pprint.pprint(args.jaccardSpec)

    if not args.numPartitions:
        if location == "local":
            args.numPartitions = 3
        elif location == "hdfs":
            args.numPartitions = 50

    sc = SparkContext(appName="crfprocess")
    crfprocess(sc, args.input, args.output, 
               featureListFilename=args.featureListFilename,
               modelFilename=args.modelFilename,
               jaccardSpecs=[j.split(',') for j in args.jaccardSpec],
               debug=True if args.debug > 0 else False,
               limit=args.limit,
               location=location,
               outputFormat="sequence",
               numPartitions=args.numPartitions,
               chunksPerPartition=args.chunksPerPartition,
               hexDigits=args.hexDigits)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# STATS with 4.2Mb input, featureFile=features.hair-eye, model=dig-hair-eye-train.model, featureCount=
# With hexDigits=1 (16 partition buckets), 1m33.653s
# With hexDigits=2 (256 partition buckets), 1m51.499s
# With hexDigits=3 (4096 partition buckets), 3m42.185s
