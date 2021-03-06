#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import json
import cgi
import platform
import socket
import codecs
from datetime import datetime

from htmltoken import tokenize
import crf_features

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

def rowToString(r):
    if isinstance(r, list):
        return u"\t".join(r)
    else:
        return unicode(r)

def vectorToString(v, debug=False):
    """Too baroque, too defensive"""
    rows = []
    try:
        if v[-1] == u"":
            pass
        else:
            # print "appending1"
            v.append(u"")
    except:
        # print "appending2"
        v.append("")
    for r in v:
        try:
            row = rowToString(r)
            if row:
                rows.append(row)
            else:
                rows.append(u"")
        except:
            try:
                rows.append(rowToString(r))
            except:
                pass
    result = u"\n".join(rows)
    return result.encode('utf-8')

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

LIMIT = None

def crfprep(sc, inputFilename, outputDirectory, 
            limit=LIMIT, location='hdfs', outputFormat="text", partitions=None):
    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    featureListFilename = os.path.join(crfConfigDir, "features.hair-eye")
    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    crfExecutable = "/usr/local/bin/crf_test_filter.sh"
    crfModelFilename = os.path.join(crfConfigDir, "dig-hair-eye-train.model")

    rdd_sequence_file_input = sc.sequenceFile(inputFilename)
    rdd_sequence_file_input.setName('rdd_sequence_file_input')
    # rdd_sequence_file_input.persist()
    
    if limit:
        rdd_sequence_file_input = sc.parallelize(rdd_sequence_file_input.take(limit))
    if partitions:
        rdd_sequence_file_input = rdd_sequence_file_input.repartition(partitions)

    rdd_json = rdd_sequence_file_input.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    # rdd_json.persist()

    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')
    # data format issue?
    # rdd_texts.saveAsSequenceFile(outputDirectory + "_texts")

    # This separator could have appeared in original text, and should serve to cleanly delimit the body from the title
    # Not perfect, it could have appeared between real tokens

    # Needs to have single labels+index feature
    # former code was lost

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

    if location == 'hdfs':
        cmd = "%s %s" % (os.path.basename(crfExecutable), os.path.basename(crfModelFilename))
    elif location == 'local':
        cmd = "%s %s" % (SparkFiles.get(os.path.basename(crfExecutable)), SparkFiles.get(os.path.basename(crfModelFilename)))
    print "###CMD %s" % cmd
    rdd_crfoutput = rdd_pipeinput.pipe(cmd)
    rdd_crfoutput.setName('rdd_crfoutput')
    # rdd_features.persist()
    
    rdd_final = rdd_crfoutput
    if outputFormat == "sequence":
        rdd_final.saveAsSequenceFile(outputDirectory)
    elif outputFormat == "text":
        rdd_final.saveAsTextFile(outputDirectory)
    else:
        raise RuntimeError("Unrecognized output format: %s" % outputFormat)

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
        return ("/Users/philpot/Documents/project/dig-crf/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def output(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    if location == 'hdfs':
        if tag == 'incremental':
            # For HDFS incrementals, there is always a single part-r-00000
            # So we can safely drop this when creating an output directory
            return ("/user/worker/process/incremental/pilot/refactor/ads_attrs_crff/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            # An HDFS batch file such as istr58m will generally generate multiple output files
            # We key them to a directory using the input file part index
            # /user/worker/process/istr58m/pilot01/ads_main
            # /user/worker/process/istr58m/pilot01/ads_addrs_crff
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crff/from-part-r-%05d"
                    % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        if tag == 'incremental':
            # Let's mirror the HDFS convention for local as well
            return ("/Users/philpot/Documents/project/dig-crf/data/output/incremental/pilot/refactor/ads_attrs_crff/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            return ("/Users/philpot/Documents/project/dig-crf/data/output/istr58m/pilot/ads_attrs_crff/from-part-r-%05d"
                    % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    else:
        raise RuntimeError("Unknown location: %s" % location)

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

    year = 2015
    mode = sys.argv[1]
    tag = sys.argv[2]
    month = int(sys.argv[3])
    day = int(sys.argv[4])
    hour = int(sys.argv[5])
    partNum = None
    try:
        partNum = int(sys.argv[6])
    except:
        pass
    limit = None
    try:
        limit = int(sys.argv[7])
    except:
        pass
    partitions = None
    try:
        partitions = int(sys.argv[8])
    except:
        pass
    inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag, partNum=partNum)
    outputDirectory = output(year=year, month=month, day=day, hour=hour, location=location, tag=tag, partNum=partNum)
    docu =("year %s, mode %s, tag %s, month %s, day %s, hour %s, partNum %s, limit %s, partitions %s" %
           (year, mode, tag, month, day, hour, partNum, limit, partitions))
    print docu
    sc = SparkContext(appName="crfprep %s %s %s %s %s %s %s %s %s" % (year, mode, tag, month, day, hour, partNum, limit, partitions))
    crfprep(sc, inputFilename, outputDirectory, 
            limit=limit, location=location, outputFormat="text", partitions=partitions)
