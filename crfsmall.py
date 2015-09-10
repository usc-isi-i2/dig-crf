#!/usr/bin/env python

try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
from harvestspans import computeSpans
from hybridJaccard import HybridJaccard
import argparse
import json
import cgi
from htmltoken import tokenize
import crf_features

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

def crfsmall(sc, input, output, 
            limit=None, location='hdfs', outputFormat="text", numPartitions=None):

    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    featureListFilename = os.path.join(crfConfigDir, "features.hair-eye")
    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    crfExecutable = "/usr/local/bin/crf_test_filter.sh"
    crfModelFilename = os.path.join(crfConfigDir, "dig-hair-eye-train.model")

    # crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    # def cpath(n):
    #     return os.path.join(crfConfigDir, n)

    # smEyeColor = HybridJaccard(ref_path=cpath("eyeColor_reference_wiki.txt"),
    #                            config_path=cpath("eyeColor_config.txt"))
    # smHairColor = HybridJaccard(ref_path=cpath("hairColor_reference_wiki.txt"),
    #                             config_path=cpath("hairColor_config.txt"))
    # print smEyeColor, smHairColor

    # hypothesis1: data fetched this way prompts the lzo compression error
    # hypothesis2: but it doesn't matter, error is just a warning

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

    rdd_features = rdd_texts.map(lambda x: (x[0], makeMatrix(c, x[0], x[1][0], x[1][1])))
    rdd_features.setName('rdd_features')
    # rdd_features.persist()

    rdd_pipeinput = rdd_features.mapValues(lambda x: vectorToUTF8(x)).values()
    rdd_pipeinput.setName('rdd_pipeinput')

    rdd_final = rdd_pipeinput

    if outputFormat == "sequence":
        rdd_final.saveAsSequenceFile(output)
    elif outputFormat == "text":
        rdd_final.saveAsTextFile(output)
    else:
        raise RuntimeError("Unrecognized output format: %s" % outputFormat)

# if __name__ == "__main__":

#     location = "hdfs"
#     try:
#         if "avatar" in platform.node():
#             location = "local"
#     except:
#         pass
#     try:
#         if "avatar" in socket.gethostname():
#             location = "local"
#     except:
#         pass
#     print "### location %s" % location

#     year = 2015
#     mode = sys.argv[1]
#     tag = sys.argv[2]
#     month = int(sys.argv[3])
#     day = int(sys.argv[4])
#     hour = int(sys.argv[5])
#     fromPartNum = None
#     try:
#         fromPartNum = int(sys.argv[6])
#     except:
#         pass
#     partNum = None
#     try:
#         partNum = int(sys.argv[7])
#     except:
#         pass
#     limit = None
#     try:
#         limit = int(sys.argv[8])
#     except:
#         pass
#     partitions = None
#     try:
#         partitions = int(sys.argv[9])
#     except:
#         pass
#     inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag, fromPartNum=fromPartNum, partNum=partNum)
#     outputDirectory = output(year=year, month=month, day=day, hour=hour, location=location, tag=tag, fromPartNum=fromPartNum, partNum=partNum)
#     print inputFilename, outputDirectory
#     docu =("year %s, mode %s, tag %s, month %s, day %s, hour %s, fromPartNum %s, partNum %s, limit %s, partitions %s" %
#            (year, mode, tag, month, day, hour, fromPartNum, partNum, limit, partitions))
#     print docu
#     sc = SparkContext(appName="crfsmall %s %s %s %s %s %s %s %s %s %s" % (year, mode, tag, month, day, hour, fromPartNum, partNum, limit, partitions))
#     crfsmall(sc, inputFilename, outputDirectory, 
#              limit=limit, location=location, outputFormat="text", partitions=partitions)

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
