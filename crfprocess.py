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
    except
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

def crfprocess(sc, input, output, 
               limit=None, location='hdfs', outputFormat="text", numPartitions=None, hexDigits=3):

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

    # pageUri -> content
    rdd_crfl = sc.sequenceFile(input)
    rdd_crfl.setName('rdd_crfl')
    if limit:
        rdd_crfl = sc.parallelize(rdd_crfl.take(limit))
    if numPartitions:
        rdd_crfl = rdd_crfl.repartition(numPartitions)

    extraUrls = ['http://dig.isi.edu/ht/data/page/0040378735A6B350D3B2F639FF4EE72AE4956171/1433150471000/processed',
                 'http://dig.isi.edu/ht/data/page/0049B75090747D2C4DCB61802DF0734FEE9C8B83/1433149330000/processed',
                 'http://dig.isi.edu/ht/data/page/00825019215AF69CAF79C6D363E824B2434EFC56/1433151190000/processed',
                 'http://dig.isi.edu/ht/data/page/00973747A0EBB56310766C2F92AB4B541C29216D/1433150471000/processed',
                 'http://dig.isi.edu/ht/data/page/00AC34D8256E5382857287225CC0EED9ACB6D626/1433151011000/processed',
                 'http://dig.isi.edu/ht/data/page/00BBA909AF6BE8B6D34E06D4FA1AB5FDC1E8A369/1433149749000/processed',
                 'http://dig.isi.edu/ht/data/page/00D5038C466F0E3DD35476FA9000D8E542E0F3A7/1433149210000/processed',
                 'http://dig.isi.edu/ht/data/page/011AE8C92F55995225B04BFE9F49DE55B63A7535/1433151914000/processed',
                 'http://dig.isi.edu/ht/data/page/0130497AAB35FB352769802FCFF38F4B35CDADD7/1433151731000/processed',
                 'http://dig.isi.edu/ht/data/page/0136BAA641B5C9218022E8F4BA64CC8E6C4AD936/1433150650000/processed',
                 'http://dig.isi.edu/ht/data/page/019C49526649EFFD17D846BF483AD80AC976ACA9/1433150836000/processed',
                 'http://dig.isi.edu/ht/data/page/01C6C9F7A6F11E82F0072AD5146569C985DDC2C7/1433152630000/processed',
                 'http://dig.isi.edu/ht/data/page/01EC0B37FC43F7ED0852C7DFD9E9F26CCA2B50A4/1433151551000/processed',
                 'http://dig.isi.edu/ht/data/page/01F8BD3AFC358683833438EA8A799B86D443B8AC/1433150949000/processed',
                 'http://dig.isi.edu/ht/data/page/020920C28559E3762CD10ABDFFACC96DAF73498F/1433152453000/processed',
                 'http://dig.isi.edu/ht/data/page/0251BCB1E15FFEF314EA038C45DBB8EC70EA312B/1433150533000/processed',
                 'http://dig.isi.edu/ht/data/page/02980AED176056E108CB780BF8A3E91AC255C3D2/1433151195000/processed',
                 'http://dig.isi.edu/ht/data/page/029AC5CD904994E92A2F8C575A260E7171EE8EA5/1433150470000/processed',
                 'http://dig.isi.edu/ht/data/page/02A5E15A77974FF781F65D4D2B0E63BAD6411EE1/1433151678000/processed',
                 'http://dig.isi.edu/ht/data/page/02EC2BDF097A3A92A82ED53921425E281DAE94BE/1433151913000/processed',
                 'http://dig.isi.edu/ht/data/page/02F7576D833CD8A4F348A81068E504440BBBAD5B/1433151789000/processed',
                 'http://dig.isi.edu/ht/data/page/02F7BA08731EB0B2D31EC0CED2CD74934B65504D/1433150050000/processed',
                 # 'http://dig.isi.edu/ht/data/page/032B6FAE3A382B52B2B2F0EC7209FC2BDCBEA76D/1433150470000/processed',
                 'http://dig.isi.edu/ht/data/page/034755278BE92A4A9143884D65BFA8D28914FE75/1433152510000/processed',
                 'http://dig.isi.edu/ht/data/page/0349852C753B08608FC8AD55CF37717D795013CC/1433149450000/processed',
                 'http://dig.isi.edu/ht/data/page/0373600584CD458D37032C11551AFF9DAB5AF57C/1433152512000/processed',
                 'http://dig.isi.edu/ht/data/page/03C7576CE8F6E9E833147F68BFE943580B92C971/1433149749000/processed',
                 'http://dig.isi.edu/ht/data/page/04068BF0B7C159D30E7AD2CB852DC78C795566B4/1433151370000/processed',
                 'http://dig.isi.edu/ht/data/page/040C117CC97E1A938221F5BF740BAB8B492924A6/1433149932000/processed',
                 'http://dig.isi.edu/ht/data/page/042DB9654B09A27DAE364B1268BC2AE87A55EE11/1433152751000/processed',
                 'http://dig.isi.edu/ht/data/page/0434CB3BDFE3839D6CAC6DBE0EBED0278D3634D8/1433149274000/processed',
                 'http://dig.isi.edu/ht/data/page/0494BCEC507C5D690B956820CDE73A109B9592A3/1433151130000/processed',
                 'http://dig.isi.edu/ht/data/page/025AB9A386008C3547A502CEA06850CE719EBA48/1433150233000/processed',
                 'http://dig.isi.edu/ht/data/page/0274CD41F6B4785D6B37238AA9977DD82134ED2C/1433151493000/processed',
                 'http://dig.isi.edu/ht/data/page/027E0B61F593F53862A4F4AE72228CFE7113BDC6/1433150230000/processed',
                 'http://dig.isi.edu/ht/data/page/029671BA207576490534B88AF0404AEC6A144B8D/1433150591000/processed',
                 'http://dig.isi.edu/ht/data/page/0497CC0202C603A25DF1979879333F40730B1FA4/1433151970000/processed',
                 'http://dig.isi.edu/ht/data/page/04A478ED6F06826CED34D1273005E6D7E50A49F2/1433152454000/processed',
                 # 'http://dig.isi.edu/ht/data/page/04B4CD61A7E87236B6246DB6CE05B30859661E36/1433151375000/processed'
                 ]
    keepUrls = [# contains utf-8/unicode
                'http://dig.isi.edu/ht/data/page/04B4CD61A7E87236B6246DB6CE05B30859661E36/1433151375000/processed',
                # generates output
                'http://dig.isi.edu/ht/data/page/032B6FAE3A382B52B2B2F0EC7209FC2BDCBEA76D/1433150470000/processed',
                # adding this one seems necessary to cause error
                # 'http://dig.isi.edu/ht/data/page/00825019215AF69CAF79C6D363E824B2434EFC56/1433151190000/processed'
                ]
    # keepUrls.extend(extraUrls[0:37])
    keepUrls.extend(extraUrls[0:3])
    keepUrls = []
    # this one has odd unicode issues
    keepUrls = ["http://dig.isi.edu/ht/data/page/442EA3A8B9FF69D65BC8B0D205C8C85204A7C799/1433150174000/processed"]
    # print keepUrls
    if keepUrls:
        rdd_crfl = rdd_crfl.filter(lambda (k,v): k in keepUrls)
    # rdd_crfl.saveAsTextFile('out_rdd_crfl')
    print "%d input pages" % rdd_crfl.count()
    
    # pageUri -> dict from json
    rdd_json = rdd_crfl.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    # rdd_json.saveAsTextFile('out_rdd_json')

    # pageUri -> (body tokens, title tokens)
    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')
    rdd_texts.saveAsTextFile('out_rdd_texts')

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
    c = crf_features.CrfFeatures(featureListFilename)

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
    # rdd_features.saveAsTextFile('out_rdd_features')

    # unicode UTF-8 representation of the feature matrix
    # pageUri -> unicode UTF-8 representation of the feature matrix
    rdd_vector = rdd_features.mapValues(lambda x: vectorToUTF8(x))
    rdd_vector.setName('rdd_vector')
    # rdd_vector.saveAsTextFile('out_rdd_vector')

    # NEW
    # rdd_partition01 = rdd_vector.sortByKey()

    def generatePrefixKey(uri, hexDigits=hexDigits):
        """http://dig.isi.edu/ht/data/page/0040378735A6B350D3B2F639FF4EE72AE4956171/1433150471000/processed"""
        words = uri.split('/')
        # first 6 fields + prefix of 7th field using hexDigits
        # [u'http:', u'', u'dig.isi.edu', u'ht', u'data', u'page', u'004']
        return "/".join(words[0:6] + [words[6][0:hexDigits]])

    # e.g. http://dig.isi.edu/ht/data/page/004 -> serialized representation of one document
    # rdd_partition02 = rdd_vector.map(lambda (k,v): (generatePrefixKey(k), v) )
    # e.g. http://dig.isi.edu/ht/data/page/004 -> (<full word uri>, serialized representation of one document)
    rdd_partition02 = rdd_vector.map(lambda (k,v): (generatePrefixKey(k), (k, v) ))
    # rdd_partition02.saveAsTextFile('out_rdd_partition02')

    # performing a full sort now will put group contiguous prefixes; within which order by word index
    # SORTED e.g. http://dig.isi.edu/ht/data/page/004 -> (<full word uri>, serialized representation of one document)
    rdd_partition03 = rdd_partition02.sortBy(lambda x: x)
    # rdd_partition03.saveAsTextFile('out_rdd_partition03')

    # rdd_partition04 = rdd_partition02.reduceByKey(lambda a,b: a+b)
    # a, b are now tuples
    # discard the first, concat only the second
    # prefixUri -> serialized representations of all documents with that prefix, in order, concatenated
    # rdd_partition04 = rdd_partition03.reduceByKey(lambda ta,tb: ta[1]+tb[1])
    # aggregate:
    # result type U is string
    # input type V is tuple
    # merge V into U is lambda v,u: u+v[1]
    # merge U1 and U2 us lambda u1,u2: u1+u2
    rdd_partition04 = rdd_partition03.aggregateByKey("", lambda u,v: u+v[1], lambda u1,u2: u1+u2)
    # rdd_partition04.saveAsTextFile('out_rdd_partition04')

    # rdd_partition05 = rdd_partition04.mapValues(lambda u: type(u))
    # rdd_partition05 = rdd_partition04.mapValues(lambda u: b64encode(u))
    # rdd_partition05 = rdd_partition04.map(lambda (k,v): b64encode(v))
    # rdd_partition05.saveAsTextFile('out_rdd_partition05')

    keys = rdd_partition04.keys().distinct().sortBy(lambda x: x).collect()
    keyCount = len(keys)
    keyMap = dict(zip(keys, range(keyCount)))
    rdd_keys = sc.parallelize(keyMap)

    print "There are %d keys" % keyCount

    print "repartition to %d" % keyCount

    def myPartitionFunc(k):
        # print "Partition for %r" % k
        return keyMap[k]
    
    rdd_partition06 = rdd_partition04.repartitionAndSortWithinPartitions(numPartitions=keyCount,
                                                                         partitionFunc=myPartitionFunc)
    print rdd_partition06.getNumPartitions()
    # rdd_partition06.saveAsTextFile('out_rdd_partition06')

    # all strings concatenated together, then base64 encoded into one input for crf_test
    # rdd_pipeinput = sc.parallelize([b64encode(rdd_vector.reduce(lambda a,b: a+b))])
    # strip off the k, b64encode the v
    rdd_partition07 = rdd_partition06.map(lambda (k,v): b64encode(v))
    rdd_pipeinput = rdd_partition07
    rdd_pipeinput.setName('rdd_pipeinput')
    rdd_pipeinput.saveAsTextFile('out_rdd_pipeinput')

    # base64 encoded result of running crf_test and filtering to
    # include only word, wordUri, non-null label
    executable = SparkFiles.get(os.path.basename(crfExecutable))
    model = SparkFiles.get(os.path.basename(crfModelFilename))
    cmd = "%s %s" % (executable, model)
    rdd_crfoutput = rdd_pipeinput.pipe(cmd)
    rdd_crfoutput.setName('rdd_crfoutput')
    # rdd_crfoutput.saveAsTextFile('out_rdd_crfoutput')

    # base64 decoded to regular serialized string
    #### MIGHT INTRODUCE EXTRA NEWLINES WHEN INPUT IS EMPTY(?)
    rdd_base64decode = rdd_crfoutput.map(lambda x: b64decode(x))
    rdd_base64decode.saveAsTextFile('out_rdd_base64decode')

    #rdd_base64decodeUnicode = rdd_base64decode.map(lambda x: x.decode('utf8'))
    #rdd_base64decodeUnicode.saveAsTextFile('out_rdd_base64decodeUnicode')

    ### There may be a need to utf8 decode this data ###
    # 1. break into physical lines
    # 2. turn each line into its own spark row
    # 3. drop any inter-document empty string markers
    rdd_lines = rdd_base64decode.map(lambda x: x.split("\n")).flatMap(lambda l: l).filter(lambda x: len(x)>1)
    rdd_lines.saveAsTextFile('out_rdd_lines')

    def processOneLine(l):
        return l.split("\t")

    rdd_triples = rdd_lines.map(lambda l: processOneLine(l))

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
    featureNames = {"eyeColor": "person_eyecolor",
                    "hairType": "person_haircolor"}
    def jaccard(tpl):
        (words, category) = tpl
        return {"featureName": featureNames[category], 
                "featureValue": hybridJaccards[category](words)}

    rdd_aligned = rdd_flat.mapValues(lambda v: jaccard(v))
    rdd_aligned.saveAsTextFile('out_rdd_aligned')

    rdd_final = rdd_aligned.mapValues(lambda v: json.dumps(v))

    if rdd_final.count() > 0:
        if outputFormat == "sequence":
            rdd_final.saveAsSequenceFile(output)
        elif outputFormat == "text":
            rdd_final.saveAsTextFile(output)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)
    else:
        print "### NO DATA TO WRITE"

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int)
    parser.add_argument('-d','--hexDigits', required=False, default=3, type=int)
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

    sc = SparkContext(appName="crfprocess")
    crfprocess(sc, args.input, args.output, 
               limit=args.limit, 
               location=location,
               outputFormat="sequence",
               numPartitions=args.numPartitions,
               hexDigits=args.hexDigits)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# STATS
# With hexDigits=3 (4096 partition buckets), used 1:55 on 4.2 Mb input
# With hexDigits=2 (256 partition buckets), used 1:18
