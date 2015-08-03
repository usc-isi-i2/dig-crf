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
from util import ensureDirectoriesExist, echo

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
    for tok in tokenize(text, interpret=cgi.escape, keepTags=False):
        # yield tok
        yield tok.replace('\t', ' ')

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
            # print "appending1"
            v.append("")
    except:
        # print "appending2"
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

u='http://dig.isi.edu/ht/data/page/00349D472B67F3C96E372A3CDD1451E3A41FED5F/1433682543000/processed'

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

@echo
def ff(tag, location='hdfs'):
    if location == 'hdfs':
        return "/tmp/dig/spark/mturk/%s" % tag
    else:
        return "/tmp/dig/spark/mturk/%s" % tag

def driver(sc, mode, *args, **kwargs):
    if mode == 'total':
        return totalDriver(sc, *args, **kwargs)
    elif mode == 'prep':
        return prepDriver(sc, *args, **kwargs)
    else:
        print >> sys.stderr, "Unrecognized mode %s" % mode

def prepDriver(sc, inputFilename, outputDirectory, 
               crfExecutable, crfScript, 
               featureListFilename, crfModelFilename, 
               eyeColorRef, eyeColorConfig, hairRef, hairConfig, 
               limit=limit, location='hdfs', outputFormat="text", partitions=None):
    dump = False
    partitions = None
    limit = 3

    # Program to compute CRF++
    c = crf_features.CrfFeatures(featureListFilename)
    # Add files to be downloaded with this Spark job on every node.
    sc.addFile(crfExecutable)
    sc.addFile(crfScript)
    sc.addFile(crfModelFilename)

    # Map to reference sets
    smEyeColor = HybridJaccard(ref_path=eyeColorRef, config_path=eyeColorConfig)
    smHairColor = HybridJaccard(ref_path=hairRef, config_path=hairConfig)

    print location
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
    if limit:
        rdd_sequence_file_input = sc.parallelize(rdd_sequence_file_input.take(limit))
    if partitions:
        rdd_sequence_file_input = rdd_sequence_file_input.repartition(partitions)
    print "### input %s: %d ads (orig %s, limit was %s), %d partitions" % (inputFilename, rdd_sequence_file_input.count(), origSize, limit, rdd_sequence_file_input.getNumPartitions())

    rdd_json = rdd_sequence_file_input.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    # rdd_json.persist()

    # all below should also be done for title
    rdd_texts = rdd_json.mapValues(lambda x: (extract_body(x), extract_title(x)))
    rdd_texts.setName('rdd_texts')
    # rdd_texts.persist()
    if dump:
        rdd_texts.saveAsTextFile(ff("texts"))
        
    rdd_texts_tokens = rdd_texts.mapValues(lambda x: (textTokens(x[0]), textTokens(x[1])))
    rdd_texts_tokens.setName('rdd_texts_tokens')
    # rdd_texts_tokens.persist()
    if True:
        rdd_texts_tokens.saveAsTextFile(ff("texts_tokens"))

    # This separator could have appeared in original text, and should serve to cleanly delimit the body from the title
    SEPARATOR = '&amp;nbsp;',
    rdd_texts_features = rdd_texts_tokens.map(lambda x: (x[0], 
                                                         c.computeFeatMatrix(list(x[1][0]) + [SEPARATOR] + list(x[1][1]),
                                                                             False,
                                                                             addLabels=[x[0]],
                                                                             addIndex=True)))
    rdd_texts_features.setName('rdd_features')
    # rdd_features.persist()
    if True:
        rdd_texts_features.saveAsTextFile(ff("texts_features"))
    exit(0)
   
    # rdd_pipeinput = rdd_features.mapValues(lambda x: base64.b64encode(vectorToString(x)))
    rdd_pipeinput = rdd_texts_features.mapValues(lambda x: vectorToString(x))
    rdd_pipeinput.setName('rdd_pipeinput')
    # rdd_pipeinput.persist()
    if dump:
        rdd_pipeinput.values().saveAsTextFile(ff("pi"))

    rdd_final = rdd_pipeinput.values()
    empty = rdd_final.isEmpty()
    if not empty:
        l = "unknown>1"
        print >> sys.stderr, "### writing %s output (%s records) to %s" % (outputFormat, l, outputDirectory)
        # print len(rdd_final.collect())
        if outputFormat == "sequence":
            rdd_final.saveAsSequenceFile(outputDirectory)
        elif outputFormat == "text":
            rdd_final.saveAsTextFile(outputDirectory)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)
    else:
        print >> sys.stderr, "### No records: no output into %s" % (outputDirectory)

# def totalDriver(sc, inputFilename, outputDirectory, 
#                 crfExecutable, crfScript, 
#                 featureListFilename, crfModelFilename, 
#                 eyeColorRef, eyeColorConfig, hairRef, hairConfig, 
#                 limit=limit, location='hdfs', outputFormat="text", partitions=None):
#     dump = False
#     partitions = None

#     # Program to compute CRF++
#     c = crf_features.CrfFeatures(featureListFilename)
#     # Add files to be downloaded with this Spark job on every node.
#     sc.addFile(crfExecutable)
#     sc.addFile(crfScript)
#     sc.addFile(crfModelFilename)

#     # Map to reference sets
#     smEyeColor = HybridJaccard(ref_path=eyeColorRef, config_path=eyeColorConfig)
#     smHairColor = HybridJaccard(ref_path=hairRef, config_path=hairConfig)

#     if location == "hdfs":
#         print "We want to do hdfs dfs -rm -r %s" % outputDirectory
#     elif location == "local":
#         try:
#             shutil.rmtree(outputDirectory)
#             print "rmtree %s" % outputDirectory
#         except:
#             pass
#     else:
#         raise RuntimeError("No such location: %s" % location)

#     rdd_sequence_file_input = sc.sequenceFile(inputFilename)
#     rdd_sequence_file_input.setName('rdd_sequence_file_input')
#     # rdd_sequence_file_input.persist()
    
#     origSize = rdd_sequence_file_input.count()
# #     if limit:
# #         rdd = sc.parallelize(rdd_sequence_file_input.take(limit))
#     if partitions:
#         rdd_sequence_file_input = rdd_sequence_file_input.repartition(partitions)
#     print "### input %s: %d ads (orig %s, limit was %s), %d partitions" % (inputFilename, rdd_sequence_file_input.count(), origSize, limit, rdd_sequence_file_input.getNumPartitions())

#     rdd_json = rdd_sequence_file_input.mapValues(lambda x: json.loads(x))
#     rdd_json.setName('rdd_json')
#     # rdd_json.persist()

#     # all below should also be done for title
#     rdd_body = rdd_json.mapValues(lambda x: extract_body(x))
#     rdd_body.setName('rdd_body')
#     # rdd_body.persist()
#     if dump:
#         rdd_body.saveAsTextFile(ff("body"))
        
#     rdd_body_tokens = rdd_body.mapValues(lambda x: textTokens(x))
#     rdd_body_tokens.setName('rdd_body_tokens')
#     # rdd_body_tokens.persist()
#     if dump:
#         rdd_body_tokens.saveAsTextFile(ff("body_tokens"))

#     rdd_features = rdd_body_tokens.map(lambda x: (x[0], c.computeFeatMatrix(x[1], False, addLabels=[x[0]], addIndex=True)))
#     rdd_features.setName('rdd_features')
#     # rdd_features.persist()
#     if dump:
#         rdd_features.saveAsTextFile(ff("features"))
    
#     # rdd_pipeinput = rdd_features.mapValues(lambda x: base64.b64encode(vectorToString(x)))
#     rdd_pipeinput = rdd_features.mapValues(lambda x: vectorToString(x))
#     rdd_pipeinput.setName('rdd_pipeinput')
#     # rdd_pipeinput.persist()
#     if dump:
#         rdd_pipeinput.values().saveAsTextFile(ff("pi"))
#     # This caused a cannot concatenate string + None error
#     # rdd_pipeinput.saveAsTextFile(outputDirectory + "-pipeinput")

#     # DON'T USE SparkFiles.get to fetch the crf_test or model
#     # This only works with local Spark (--master local[*])
#     if location == 'hdfs':
#         cmd = "%s %s" % (os.path.basename(crfScript), os.path.basename(crfModelFilename))
#     elif location == 'local':
#         cmd = "%s %s" % (SparkFiles.get(os.path.basename(crfScript)), SparkFiles.get(os.path.basename(crfModelFilename)))
#     print "### %s" % cmd
#     rdd_pipeinput.saveAsTextFile(ff("before"))
#     exit(0)

#     rdd_crf_b64 = rdd_pipeinput.values().pipe(cmd)
#     rdd_crf_b64.setName('rdd_crf_b64')
#     # rdd_crf_b64.persist()
#     if dump:
#         rdd_crf_b64.saveAsTextFile(ff("po"))

#     # Go directly from base64 output to a reconstructed tuple format mapping URI to vector of vectors, 
#     # with empty string suffix indicating blank line
#     # This is key for avoiding the groupBy step
#     rdd_restore = rdd_crf_b64.map(lambda x: restore(x))
#     rdd_restore.setName('rdd_restore')
#     # rdd_restore.persist()
#     if dump:
#         rdd_restore.saveAsTextFile(ff("restore"))

#     # ### WE NO LONGER HAVE TO GROUPBY
#     # ### BUT WE MUST TREAT EACH LINE INDIVIDUALLY NOW
#     # rdd_withuri = sc.parallelize(rdd_withuri.take(10))

#     rdd_harvested = rdd_restore.mapValues(lambda x: computeSpans(x, indexed=True)).filter(lambda p: p[1])
#     rdd_harvested.setName('rdd_harvested')
#     # rdd_harvested.persist()
#     if dump:
#         rdd_harvested.saveAsTextFile(ff("harvested"))

#     # This has the effect of generating 0, 1, 2, ... lines according to the number of spans
#     rdd_controlled = rdd_harvested.flatMapValues(lambda x: list(x))
#     rdd_controlled.setName('rdd_controlled')
#     # rdd_controlled.persist()

#     # map any eyeColor spans using smEyeColor, hairType spans using smHairColor
#     rdd_aligned = rdd_controlled.mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": smEyeColor, "hairType": smHairColor}))
#     rdd_aligned.setName('rdd_aligned')
#     # rdd_aligned.persist()
#     if dump:
#         rdd_aligned.saveAsTextFile(ff("aligned"))

#     rdd_aligned_json = rdd_aligned.mapValues(lambda x: json.dumps(x))
#     rdd_aligned_json.setName('rdd_aligned_json')
#     # rdd_aligned_json.persist()
#     if dump:
#         rdd_aligned_json.saveAsTextFile(ff("aligned_json"))

#     rdd_final = rdd_aligned_json
#     empty = rdd_final.isEmpty()
#     if not empty:
#         l = "unknown>1"
#         print "### writing %s output (%s records) to %s" % (outputFormat, l, outputDirectory)
#         # print len(rdd_final.collect())
#         if outputFormat == "sequence":
#             rdd_final.saveAsSequenceFile(outputDirectory)
#         elif outputFormat == "text":
#             rdd_final.saveAsTextFile(outputDirectory)
#         else:
#             raise RuntimeError("Unrecognized output format: %s" % outputFormat)
#     else:
#         print "### No records: no output into %s" % (outputDirectory)

def input(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    """##Assumes only one part-00000 per incremental"""
    if location == 'hdfs':
        if tag == 'incremental':
            return ("/user/worker/process/incremental/pilot/refactor/ads_main/%04d-%02d-%02d-%02d-00/part-r-%05d"
                    % (year, month, day, hour, int(partNum)))
        elif tag == 'istr58m':
            return ("/user/worker/process/istr58m/pilot01/ads_main/part-r-%05d" % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        return ("/Users/philpot/Documents/project/dig-mturk/spark/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def output(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    if location == 'hdfs':
        if tag == 'incremental':
            return ("/user/worker/process/incremental/pilot/refactor/ads_attrs_crfinput/%04d-%02d-%02d-%02d-00/part-r-%05d"
                    % (year, month, day, hour, int(partNum)))
        elif tag == 'istr58m':
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crfinput/%04d-%02d-%02d-%02d-00/part-r-%05d"
                    % (year, month, day, hour, int(partNum)))
        else:
            raise RuntimeError("Unknown tag %r" % rag)
    elif location == 'local':
        return ("/Users/philpot/Documents/project/dig-mturk/spark/data/output/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
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

def d (sc, day, hour):
    "old pathnames, does not work"
    i = input(day, hour)
    o = output(i)
    print i,o
    driver(sc, i, o, "/usr/local/bin/crf_test",
           "/home/aphilpot/project/dig-mturk/spark/data/config/features.hair-eye",
           "/home/aphilpot/project/dig-mturk/spark/data/config/dig-hair-eye-train.model",
           "/home/aphilpot/project/dig-mturk/spark/data/config/eyeColor_reference_wiki.txt",
           "/home/aphilpot/project/dig-mturk/spark/data/config/eyeColor_config.txt",
           "/home/aphilpot/project/dig-mturk/spark/data/config/hairColor_reference_wiki.txt",
           "/home/aphilpot/project/dig-mturk/spark/data/config/hairColor_config.txt")

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
    sc.setCheckpointDir("/tmp")
    year = 2015
    mode = sys.argv[1]
    tag = sys.argv[2]
    month = int(sys.argv[3])
    day = int(sys.argv[4])
    hour = int(sys.argv[5])
    limit = None
    try:
        limit = int(sys.argv[6])
    except:
        pass
    partitions = None
    try:
        partitions = int(sys.argv[7])
    except:
        pass
    inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag)
    outputDirectory = output(year=year, month=month, day=day, hour=hour, location=location, tag=tag)
    print "read from %s and write to %s" % (inputFilename, outputDirectory)
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

    driver(sc, mode, inputFilename, outputDirectory, crfExecutable, crfScript,
           featureListFilename, crfModelFilename, eyeColorRef, eyeColorConfig, hairColorRef, hairColorConfig, 
           limit=limit, location=location, partitions=partitions)
