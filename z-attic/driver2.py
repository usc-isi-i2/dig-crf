#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys, os
import json
import cgi
from htmltoken import tokenize
import crf_features
from harvestspans import computeSpans
from hybridJaccard import HybridJaccard

import snakebite
import platform
import socket

import codecs
from datetime import datetime
import shutil
import base64

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

def genescaped(text):
    """All tokens in TEXT with any odd characters (such as <>&) encoded using HTML escaping"""
    for tok in tokenize(text, interpret=cgi.escape):
        # yield tok
        yield tok.replace('\t', ' ')

### generate vector of vector of tokens, separated by ""

### HERE ARE SOME BROKEN ONES: 

'''
[u'I can treat you to a Wonderful Freaky Session... <br/>One that will leave you breathless.<br/>I\'m so good you\'ll leave, ready to comwute back for more... <br/>I\'m a simply stunning, "Big Booty Cutie"<br/>I\'lI give the VERY BEST services in town & GUARANTEE I can exceed your expectations.<br/>Pure UNrushed Pleasure & AWESOME SPECIALS.<br/>Independent. D/D Free. Fetish/Fantasy Friendly.<br/>702-418-5244 "CRYSTAL" (INCALL 24/7)<br/>NO TXT/NO PIMPS. (I WONT REPLY.)<br/>DONT MISS OUT YOU WILL REGRET IT..&nbsp;Call <u>702-418-5244</u>. See my menu of services on my profile BEFORE CALLING...', u'I can treat you to a Wonderful Freaky Session... <br/>One that will leave you breathless.<br/>I\'m so good you\'ll leave, ready to comwute back for more... <br/>I\'m a simply stunning, "Big Booty Cutie"<br/>I\'lI give the VERY BEST services in town & GUARANTEE I can exceed your expectations.<br/>Pure UNrushed Pleasure & AWESOME SPECIALS.<br/>Independent. D/D Free. Fetish/Fantasy Friendly.<br/>702-418-5244 "CRYSTAL" (INCALL 24/7)<br/>NO TXT/NO PIMPS. (I WONT REPLY.)<br/>DONT MISS OUT YOU WILL REGRET IT..&nbsp;Call <u>702-418-5244</u>.']
SE Trouble computing textTokens of None
SE Trouble computing textTokens of [u"You won't find ahere. I am the Mature Woman that you have been looking for. I enjoy my time with you. Your visits will never be rushed and you will leave wanting to come back for more.<br/>Call or txt me, 519-608-1850, Woodstock, minutes from the 401.<br/>Lisa XOXO.&nbsp;Call <u>519-608-1850</u>. I offer video/photo also, ask me on my profile before calling...", u"You won't find ahere. I am the Mature Woman that you have been looking for. I enjoy my time with you. Your visits will never be rushed and you will leave wanting to come back for more.<br/>Call or txt me, 519-608-1850, Woodstock, minutes from the 401.<br/>Lisa XOXO.&nbsp;Call <u>519-608-1850</u>.", u"You won't find ahere. I am the Mature Woman that you have been looking for. I enjoy my time with you. Your visits will never be rushed and you will leave wanting to come back for more.<br/>Call or txt me, 519-608-1850, Woodstock, minutes from the 401.<br/>Lisa XOXO.&nbsp;Call <u>519-608-1850</u>. See my menu of services on my profile BEFORE CALLING..."]
'''

z = [u'I can treat you to a Wonderful Freaky Session... <br/>One that will leave you breathless.<br/>I\'m so good you\'ll leave, ready to comwute back for more... <br/>I\'m a simply stunning, "Big Booty Cutie"<br/>I\'lI give the VERY BEST services in town & GUARANTEE I can exceed your expectations.<br/>Pure UNrushed Pleasure & AWESOME SPECIALS.<br/>Independent. D/D Free. Fetish/Fantasy Friendly.<br/>702-418-5244 "CRYSTAL" (INCALL 24/7)<br/>NO TXT/NO PIMPS. (I WONT REPLY.)<br/>DONT MISS OUT YOU WILL REGRET IT..&nbsp;Call <u>702-418-5244</u>. See my menu of services on my profile BEFORE CALLING...', u'I can treat you to a Wonderful Freaky Session... <br/>One that will leave you breathless.<br/>I\'m so good you\'ll leave, ready to comwute back for more... <br/>I\'m a simply stunning, "Big Booty Cutie"<br/>I\'lI give the VERY BEST services in town & GUARANTEE I can exceed your expectations.<br/>Pure UNrushed Pleasure & AWESOME SPECIALS.<br/>Independent. D/D Free. Fetish/Fantasy Friendly.<br/>702-418-5244 "CRYSTAL" (INCALL 24/7)<br/>NO TXT/NO PIMPS. (I WONT REPLY.)<br/>DONT MISS OUT YOU WILL REGRET IT..&nbsp;Call <u>702-418-5244</u>.']

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
            print >> sys.stderr, "SE Trouble computing textTokens of %s" % text
        v.append("")
    return v

def rowToString(r):
    if isinstance(r, list):
        return "\t".join(r)
    else:
        return str(r)

def vectorToString(v, debug=False):
    """Too baroque, too defensive"""
    rows = []
    try:
        if v[-1] == "":
            pass
        else:
            print "appending1"
            v.append("")
    except:
        print "appending2"
        v.append("")
    for r in v:
        try:
            row = rowToString(r).encode('utf-8')
            # # rows.append(rowToString(r).encode('ascii', 'ignore'))
            # row = rowToString(r).encode('ascii', 'ignore')
            if row:
                rows.append(row)
            else:
                # encode('ascii', 'ignore') may yield empty string
                # pass
                rows.append("")
        except:
            try:
                rows.append(rowToString(r).encode('ascii', 'ignore'))
                if debug:
                    try:
                        dt = datetime.now()
                        temp = '/tmp/encoding_error_' + str(dt).replace(' ', '_')
                        with codecs.open(temp, 'w', encoding='utf-8') as f:
                            print >> f, type(r), r
                    except:
                        pass
            except:
                pass
    return "\n".join(rows)

# def vectorToString(v, debug=False):
#     """Too baroque, too defensive"""
#     rows = []
#     for r in v:
#         try:
#             row = rowToString(r).encode('ascii', 'ignore')
#             # r="" is meaningful, row="" is an encoding error to be skipped
#             if r == "" or row:
#                 rows.append(row)
#         except:
#             pass
#     p = "\n".join(rows)
#     if p.endswith('\n'):
#         pass
#     else:
#         p = p + "\n"
#     return p


u='http://dig.isi.edu/ht/data/page/00349D472B67F3C96E372A3CDD1451E3A41FED5F/1433682543000/processed'

v=[
   ["my", "xx", "cc", "_NULL_", "_NULL_", "c", "cc", "_NULL_", "_NULL_", "my", "MY", "_NULL_", "_NULL_", "y", "my", "_NULL_", "_NULL_", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "1"],
   ["friend", "xxxxxx", "ccvvcc", "ccv", "ccvv", "c", "cc", "vcc", "vvcc", "friend", "FRIEND", "fri", "frie", "d", "nd", "end", "iend", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "2"],
   ["is", "xx", "vc", "_NULL_", "_NULL_", "c", "vc", "_NULL_", "_NULL_", "is", "IS", "_NULL_", "_NULL_", "s", "is", "_NULL_", "_NULL_", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "3"],
   ["blond", "xxxxx", "ccvcc", "ccv", "ccvc", "c", "cc", "vcc", "cvcc", "blond", "BLOND", "blo", "blon", "d", "nd", "ond", "lond", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "4"],
   ["with", "xxxx", "cvcc", "cvc", "cvcc", "c", "cc", "vcc", "cvcc", "with", "WITH", "wit", "with", "h", "th", "ith", "with", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "5"],
   ["hazel", "xxxxx", "cvcvc", "cvc", "cvcv", "c", "vc", "cvc", "vcvc", "hazel", "HAZEL", "haz", "haze", "l", "el", "zel", "azel", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "6"],
   ["eyes", "xxxx", "vcvc", "vcv", "vcvc", "c", "vc", "cvc", "vcvc", "eyes", "EYES", "eye", "eyes", "s", "es", "yes", "eyes", "false", "false", "false", "false", "false", "_NULL_", "http://dig.isi.edu/processed/12345", "7"]
   ]

# myTextTokens = textTokens(["My friend is blond and hazel eyed.",  
#                            "Hello there , I have blue eyes and brown hair.",
#                            "I like brunettes with orange eyes."])
# myUri = 'http://dig.isi.edu/processed/test'
# myCrfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
# myFeatureListFilename = os.path.join(myCrfConfigDir, "features.hair-eye")
# myC = crf_features.CrfFeatures(myFeatureListFilename)
# print myTextTokens
# myFeatures = [(myUri, myC.computeFeatMatrix(t, False, addLabels=myUri, addIndex=True)) for t in myTextTokens]

# myPipeinput = [vectorToString(v) for (k,v) in myFeatures.iteritems()]

# print myPipeinput
# exit(1)

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

def reconstructTuple(crfout):
    """Works only for new single-line invocation of crf_test_b64"""
    pairs = []
    lines = crfout.split('\n')
    for line in lines:
        fields = line.split('\t')
        try:
            uri = fields[-3]
            idx = fields[-2]
        except:
            uri = "endOfDocument"
            idx = "0"
        pairs.append( (uri, [idx] + fields) )
    return pairs

def alignToControlledVocab(harvested, vocabs):
    try:
        category = harvested['category']
        # print "category %s " % category
        sm = vocabs[category]
        # print "sm %s " % sm
        words = harvested['words']
        # print "words %s " % words
        try:
            harvested['bestMatch'] = sm.findBestMatch(words)
        except Exception as e:
            return "sm.findBestMatch error:" + str(e) + ":" + str(words)
        return harvested
    except Exception as e:
        return str(e)
    return None

def restore(x):
    EMPTY_ELEMENT = ['0', '']
    d = base64.b64decode(x)
    rec = reconstructTuple(d)
    try:
        uri = rec[0][0]
    except:
        uri = "broken_empty_CRF_output_no_uri"
    rows = ['' if r[1]==EMPTY_ELEMENT else r[1][1:] for r in rec]
    # rows = [r[1][1:] for r in rec]
    l = len(rows)
    if l >= 2 and rows[-1] == '' and rows[-2] == '':
        rows = rows[:-1]
    return (uri, rows)

limit = None

def ff(tag):
    return "/tmp/test/%s" % tag

def driver(sc, inputFilename, outputDirectory, 
           crfExecutable, crfScript, 
           featureListFilename, crfModelFilename, 
           eyeColorRef, eyeColorConfig, hairRef, hairConfig, 
           limit=limit, location='hdfs', outputFormat="text", partitions=None):
    dump = False
    partitions = 8

    # Program to compute CRF++
    c = crf_features.CrfFeatures(featureListFilename)
    # Add files to be downloaded with this Spark job on every node.
    sc.addFile(crfExecutable)
    sc.addFile(crfScript)
    sc.addFile(crfModelFilename)

    # Map to reference sets
    smEyeColor = HybridJaccard(ref_path=eyeColorRef, config_path=eyeColorConfig)
    smHairColor = HybridJaccard(ref_path=hairRef, config_path=hairConfig)

    if location == "hdfs":
        print "We want to do hdfs dfs -rm -r %s" % outputDirectory
    elif location == "local":
        try:
            shutil.rmtree(outputDirectory)
            print "rmtree %s" % outputDirectory
        except:
            pass
    else:
        raise RuntimeError("No such location: %s" % location)

    rdd_sequence_file_input = sc.sequenceFile(inputFilename)
    rdd_sequence_file_input.setName('rdd_sequence_file_input')
    # rdd_sequence_file_input.persist()
    
    origSize = rdd_sequence_file_input.count()
#     if limit:
#         rdd = sc.parallelize(rdd_sequence_file_input.take(limit))
    if partitions:
        rdd_sequence_file_input = rdd_sequence_file_input.repartition(partitions)
    print "### input %s: %d ads (orig %s, limit was %s), %d partitions" % (inputFilename, rdd_sequence_file_input.count(), origSize, limit, rdd_sequence_file_input.getNumPartitions())

    if location == 'hdfs':
        cmd = "%s %s" % (os.path.basename(crfScript), os.path.basename(crfModelFilename))
    elif location == 'local':
        cmd = "%s %s" % (SparkFiles.get(os.path.basename(crfScript)), SparkFiles.get(os.path.basename(crfModelFilename)))
    print "### %s" % cmd

    # ### WE NO LONGER HAVE TO GROUPBY
    # ### BUT WE MUST TREAT EACH LINE INDIVIDUALLY NOW
    # rdd_withuri = sc.parallelize(rdd_withuri.take(10))

    rdd_final = rdd_sequence_file_input.mapValues(lambda x: json.loads(x)).mapValues(lambda x: extract_body(x)).mapValues(lambda x: textTokens(x)).map(lambda x: (x[0], c.computeFeatMatrix(x[1], False, addLabels=[x[0]], addIndex=True))).mapValues(lambda x: base64.b64encode(vectorToString(x))).values().pipe(cmd).map(lambda x: restore(x)).mapValues(lambda x: computeSpans(x, indexed=True)).filter(lambda p: p[1]).flatMapValues(lambda x: list(x)).mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": smEyeColor, "hairType": smHairColor})).mapValues(lambda x: json.dumps(x))

    empty = rdd_final.isEmpty()
    if not empty:
        l = "unknown>1"
        print "### writing %s output (%s records) to %s" % (outputFormat, l, outputDirectory)
        print len(rdd_final.collect())
#         if outputFormat == "sequence":
#             rdd_final.saveAsSequenceFile(outputDirectory)
#         elif outputFormat == "text":
#             rdd_final.saveAsTextFile(outputDirectory)
#         else:
#             raise RuntimeError("Unrecognized output format: %s" % outputFormat)
    else:
        print "### No records: no output into %s" % (outputDirectory)

def input(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental'):
    """##Assumes only one part-00000 per incremental"""
    if location == 'hdfs':
        return ("/user/worker/process/incremental/pilot/refactor/ads_main/%04d-%02d-%02d-%02d-00/part-r-00000"
                % (year, month, day, hour))
    elif location == 'local':
        return ("/Users/philpot/Documents/project/dig-crf/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-00000"
                % (tag, year, month, day, hour))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def flatlist(*args):
    result = []
    for arg in args:
        if isinstance(arg,list):
            result.extend(arg)
        else:
            result.append(arg)
    return result

def output(input):
    head, tail = os.path.split(input)
    dirs = head.split(os.sep)
    o = os.path.join(*flatlist("/",dirs[0:-2],"ads_attrs_crfextraction",dirs[-1]))
    return o


def d (sc, day, hour):
    "old pathnames, does not work"
    i = input(day, hour)
    o = output(i)
    print i,o
    driver(sc, i, o, "/usr/local/bin/crf_test",
           "/home/aphilpot/project/dig-crf/data/config/features.hair-eye",
           "/home/aphilpot/project/dig-crf/data/config/dig-hair-eye-train.model",
           "/home/aphilpot/project/dig-crf/data/config/eyeColor_reference_wiki.txt",
           "/home/aphilpot/project/dig-crf/data/config/eyeColor_config.txt",
           "/home/aphilpot/project/dig-crf/data/config/hairColor_reference_wiki.txt",
           "/home/aphilpot/project/dig-crf/data/config/hairColor_config.txt")

if __name__ == "__main__":

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

    sc = SparkContext(appName="CRF")
    year = 2015
    month = int(sys.argv[1])
    day = int(sys.argv[2])
    hour = int(sys.argv[3])
    limit = None
    tag = 'incremental'
    partitions = 8
    partitions = 1
    try:
        limit = int(sys.argv[4])
    except:
        try:
            tag = sys.argv[4]
            partitions = 1
        except:
            pass
    inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag)
    outputDirectory = output(inputFilename)
    crfExecutable = "/usr/local/bin/crf_test"
    crfScript = "/usr/local/bin/crf_test_b64"

    if location == "local":
        hjConfigDir = "/Users/philpot/project/hybrid-jaccard"
    elif location == "hdfs":
        hjConfigDir = "/vagrant/project/hybrid-jaccard"
    eyeColorRef = os.path.join(hjConfigDir, "eyeColor_reference_wiki.txt")
    eyeColorConfig = os.path.join(hjConfigDir, "eyeColor_config.txt")
    hairColorRef = os.path.join(hjConfigDir, "hairColor_reference_wiki.txt")
    hairColorConfig = os.path.join(hjConfigDir, "hairColor_config.txt")

    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    featureListFilename = os.path.join(crfConfigDir, "features.hair-eye")
    crfModelFilename = os.path.join(crfConfigDir, "dig-hair-eye-train.model")


    driver(sc, inputFilename, outputDirectory, crfExecutable, crfScript,
           featureListFilename, crfModelFilename, eyeColorRef, eyeColorConfig, hairColorRef, hairColorConfig, 
           limit=limit, location=location, partitions=partitions)
