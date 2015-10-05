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
from itertools import izip_longest
import time
from datetime import timedelta

### from trollchar.py

def asList(x):
    if isinstance(x, list):
        return x
    else:
        return [x]

### from util.py

def iterChunks(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

### end from util.py

DIG_HT = 1
DIG_GENERIC = 2

def extract_body(main_json, inputType=DIG_HT):
    if inputType == DIG_HT:
        try:
            text = main_json["hasBodyPart"]["text"]
            return text
        except:
            pass
    elif inputType == DIG_GENERIC:
        try:
            text = main_json["mainEntityOfPage"]["description"]
            return text
        except:
            return None
    else:
        raise RuntimeError("Unrecognized input type: %s" % inputType)

def extract_title(main_json, inputType=DIG_HT):
    if inputType == DIG_HT:
        try:
            return main_json["hasTitlePart"]["text"]
        except:
            pass
    elif inputType == DIG_GENERIC:
        try:
            text = main_json["title"]
            return text
        except:
            pass
    else:
        raise RuntimeError("Unrecognized input type: %s" % inputType)
    
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
               uriClass='Offer',
               featureListFilename=configPath('features.hair-eye'),
               modelFilename=configPath('dig-hair-eye-train.model'),
               jaccardSpecs=[],
               svebor=False,
               # minimum initial number of partitions
               numPartitions=None, 
               # number of documents to send to CRF in one call
               chunksPerPartition=100,
               # coalesce/down partition to this number after CRF
               coalescePartitions=None,
               # inputType
               inputType=DIG_HT,
               limit=None, sampleSeed=1234,
               debug=0, location='hdfs', outputFormat="text"):

    show = True if debug>=1 else False
    def showPartitioning(rdd):
        """Seems to be significantly more expensive on cluster than locally"""
        if show:
            partitionCount = rdd.getNumPartitions()
            try:
                valueCount = rdd.countApprox(1000, confidence=0.50)
            except:
                valueCount = -1
            print "At %s, there are %d partitions with on average %s values" % (rdd.name(), partitionCount, int(valueCount/float(partitionCount)))

    debugOutput = output + '_debug'
    def debugDump(rdd,keys=True,listElements=False):
        showPartitioning(rdd)
        keys=False
        if debug >= 2:
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
            print "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

    def showSizeAndExit(rdd):
        try:
            k = rdd.count()
        except:
            k = None
        print "Just finished %s with size %s" % (rdd.name(), k)
        exit(0)

    crfFeatureListFilename = featureListFilename
    crfModelFilename = modelFilename
    crfExecutable = binPath("crf_test_filter.sh")
    crfExecutable = binPath("crf_test_filter_lines.sh")
    crfExecutable = "apply_crf_lines.py"
    sc.addFile(crfExecutable)
    sc.addFile(crfModelFilename)

    # LOADING DATA
    if numPartitions:
        rdd_ingest = sc.sequenceFile(input, minSplits=numPartitions)
    else:
        rdd_ingest = sc.sequenceFile(input)
    rdd_ingest.setName('rdd_ingest_input')
    showPartitioning(rdd_ingest)

    # LIMIT/SAMPLE (OPTIONAL)
    if limit==0:
        limit = None
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_ingest.count()
        rdd_ingest = rdd_ingest.sample(False, ratio, seed=sampleSeed)
        
    # FILTER TO KNOWN INTERESTING URLS (DEBUG, OPTIONAL)
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
        rdd_ingest = rdd_ingest.filter(lambda (k,v): k in keepUris)
    # layout: pageUri -> content serialized JSON string
    rdd_ingest.setName('rdd_ingest_net')
    debugDump(rdd_ingest)

    # layout: pageUri -> dict (from json)
    rdd_json = rdd_ingest.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    debugDump(rdd_json)

    # RETAIN ONLY THOSE MATCHING URI CLASS
    if uriClass:
        rdd_relevant = rdd_json.filter(lambda (k,j): j.get("a", None)==uriClass)
    else:
        rdd_relevant = rdd_json
    rdd_relevant.setName('rdd_relevant')
    debugDump(rdd_relevant)

    def byIdentifier(k,v):
        result = []
        for i in asList(v.get("identifier", "missing")):
            result.append( (k + "-" + str(i), v) )
        return result

    # Add identifier to URI
    if svebor:
        rdd_altered = rdd_relevant.flatMap(lambda (uri, j): byIdentifier(uri, j))
    else:
        rdd_altered = rdd_relevant
    rdd_altered.setName('rdd_altered')
    debugDump(rdd_altered)

    # print "### Processing %d input pages, initially into %s partitions" % (rdd_partitioned.count(), rdd_partitioned.getNumPartitions())
    # layout: pageUri -> (body tokens, title tokens)
    rdd_texts = rdd_altered.mapValues(lambda x: (textTokens(extract_body(x, inputType=inputType)), 
                                                 textTokens(extract_title(x, inputType=inputType))))
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

    # base64 encoded result of running crf_test and filtering to
    # include only word, wordUri, non-null label
    # local
    executable = SparkFiles.get(os.path.basename(crfExecutable)) if location=="local" else os.path.basename(crfExecutable)
    # local
    model = SparkFiles.get(os.path.basename(crfModelFilename)) if location=="local" else os.path.basename(crfModelFilename)
    cmd = "%s %s" % (executable, model)
    print "### Pipe cmd is %r" % cmd

    rdd_pipeoutput = rdd_pipeinput.pipe(cmd)
    if coalescePartitions:
        rdd_pipeoutput = rdd_pipeoutput.coalesce(max(2, coalescePartitions))
    rdd_pipeoutput.setName('rdd_pipeoutput')
    debugDump(rdd_pipeoutput)

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
    rdd_jaccard = rdd_flat.flatMapValues(lambda v: jaccard(v))
    rdd_jaccard.setName('rdd_jaccard')
    debugDump(rdd_jaccard)

    def extendDict(d, key, value):
        d[key] = value
        return d

    # add in the URI for karma modeling purposes
    rdd_aligned = rdd_jaccard.map(lambda (uri,v): (uri, extendDict(v, "uri", uri)))
    rdd_aligned.setName('rdd_aligned')
    debugDump(rdd_aligned)

    # rdd_aligned = rdd_pipeinput
    # docUri -> json

    def recoverIdentifier(k):
        return k.rsplit('-',1)[-1]

    if svebor:
        rdd_final = rdd_aligned.map(lambda (k,v): (recoverIdentifier(k), (v.get("featureName"),
                                                                          v.get("featureValue")))).filter(lambda (k,p): p[1]!='NONE')
    else:
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
        elif outputFormat == "tsv":
            rdd_tsv = rdd_final.map(lambda (k,p): k + "\t" + p[0] + "\t" + p[1])
            rdd_tsv.saveAsTextFile(output)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def defaultJaccardSpec():
    l = [["eyeColor", "person_eyecolor", configPath("eyeColor_config.txt"), configPath("eyeColor_reference_wiki.txt")],
         ["hairType", "person_haircolor", configPath("hairColor_config.txt"), configPath("hairColor_reference_wiki.txt")]]
    return [",".join(x) for x in l]

def sveborJaccardSpec():
    l = [["eyeColor", "person_eyecolor", configPath("eyeColor_config.txt"), configPath("eyeColor_reference_wiki.txt")],
         ["hairType", "person_haircolor", configPath("hairColor_config.txt"), configPath("hairColor_reference_wiki.txt")],
         ["hairType", "person_hairtexture", configPath("hairTexture_config.txt"), configPath("hairTexture_reference_wiki.txt")],
         ["hairType", "person_hairlength", configPath("hairLength_config.txt"), configPath("hairLength_reference_wiki.txt")]]
    return [",".join(x) for x in l]

def jaccardSpec(s):
    "4-tuple of string,string,existing file,existing file separated by comma"
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
    parser.add_argument('-u','--uriClass', default='Offer')
    parser.add_argument('-f','--featureListFilename', default=configPath('features.hair-eye'))
    parser.add_argument('-m','--modelFilename', default=configPath('dig-hair-eye-train.model'))
    parser.add_argument('-j','--jaccardSpec', action='append', default=[], type=jaccardSpec,
                        help='each value should be <category,featureName,config.json,reference.txt>')
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int,
                        help='minimum initial number of partitions')
    parser.add_argument('-c','--chunksPerPartition', required=False, default=100, type=int,
                        help='number of CRF input documents presented to crf_test at a time')
    parser.add_argument('-k','--coalescePartitions', required=False, default=None, type=int,
                        help='number of partitions to coalesce down to after crf')
    parser.add_argument('-n','--name', required=False, default="", help='Added to name of spark job, for debugging')
    parser.add_argument('-t','--inputType', required=False, type=int, default=1, choices=(DIG_HT, DIG_GENERIC),
                        help='1: istr58m/HT format; 2: generic Karma format')
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', type=int)
    parser.add_argument('-s','--svebor', default=False, action='store_true')
    args=parser.parse_args()

    # might become an option
    outputFormat = 'sequence'

    # default HJ recognizers to any known special situation 
    if args.svebor:
        args.jaccardSpec = sveborJaccardSpec()
        outputFormat = 'tsv'
    elif args.jaccardSpec == []:
        args.jaccardSpec = defaultJaccardSpec()
    # pprint.pprint(args.jaccardSpec)

    if not args.numPartitions:
        if location == "local":
            args.numPartitions = 3
        elif location == "hdfs":
            args.numPartitions = 50

    sparkName = "crfprocess"
    if args.name:
        sparkName = sparkName + " " + str(args.name)

    sc = SparkContext(appName=sparkName)
    crfprocess(sc, args.input, args.output, 
               uriClass=args.uriClass,
               featureListFilename=args.featureListFilename,
               modelFilename=args.modelFilename,
               jaccardSpecs=[j.split(',') for j in args.jaccardSpec],
               svebor=args.svebor,
               debug=args.debug,
               limit=args.limit,
               location=location,
               outputFormat=outputFormat,
               numPartitions=args.numPartitions,
               chunksPerPartition=args.chunksPerPartition,
               coalescePartitions=args.coalescePartitions,
               inputType=args.inputType)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
