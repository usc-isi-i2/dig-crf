#!/usr/bin/env python

from pyspark import SparkContext, SparkFiles
import sys
import json
import cgi
from htmltoken import tokenize
import crf_features
from harvestspans import computeSpans
from hybridJaccard import HybridJaccard

import codecs
from datetime import datetime

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
        yield tok

### generate vector of vector of tokens, separated by ""

def textTokens(text):
    v = []
    for tok in genescaped(text):
        v.append([tok])
    v.append("")
    return v

def rowToString(r):
    if isinstance(r, list):
        return "\t".join(r)
    else:
        return str(r)

def vectorToString(v):
    rows = []
    for r in v:
        try:
            # rows.append(rowToString(r).encode('utf-8'))
            # rows.append(rowToString(r).encode('ascii', 'ignore'))
            row = rowToString(r).encode('ascii', 'ignore')
            if row:
                rows.append(row)
            else:
                # encode('ascii', 'ignore') may yield empty string
                pass
        except:
            try:
                rows.append(rowToString(r).encode('ascii', 'ignore'))
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

def reconstructTuple(tabsep):
    fields = tabsep.split('\t')
    try:
        uri = fields[-3]
        idx = fields[-2]
    except:
        uri = "endOfDocument"
        idx = "0"
    return (uri, [idx] + fields)

def alignToControlledVocab(harvested, vocabs):
    try:
        category = harvested['category']
        sm = vocabs[category]
        words = harvested['words']
        try:
            harvested['bestMatch'] = sm.findBestMatch(words)
        except Exception as e:
            return "smfbm error" + str(e)
        return harvested
    except Exception as e:
        return str(e)
    return None

#     raw = "\n".join([rowToString(r) for r in v])
#     try:
#         x = raw.encode('ascii')
#     except:
#         dt = datetime.now()
#         temp = '/tmp/encoding_error_' + str(dt).replace(' ', '_')
#         with codecs.open(temp, 'w', encoding='utf-8') as f:
#             print >> f, type(raw), raw
#     x = raw.encode('ascii', 'ignore')
#     # print "Returning:", x
#     return x

uu = [[u'brown', u'xxxxx', u'ccvcc', u'ccv', u'ccvc', u'c', u'cc', u'vcc', u'cvcc', u'brown', u'BROWN', u'bro', u'brow', u'n', u'wn', u'own', u'rown', 'false', 'false', 'false', 'false', 'false', '_NULL_', u'http://dig.isi.edu/url2'], 
      [u'eyes', u'xxxx', u'vcvc', u'vcv', u'vcvc', u'c', u'vc', u'cvc', u'vcvc', u'eyes', u'EYES', u'eye', u'eyes', u's', u'es', u'yes', u'eyes', 'false', 'false', 'false', 'false', 'false', '_NULL_', u'http://dig.isi.edu/url2'], 
      [u'and', u'xxx', u'vcc', u'vcc', '_NULL_', u'c', u'cc', u'vcc', '_NULL_', u'and', u'AND', u'and', '_NULL_', u'd', u'nd', u'and', '_NULL_', 'false', 'false', 'false', 'false', 'false', '_NULL_', u'http://dig.isi.edu/url2'], 
      [u'blonde', u'xxxxxx', u'ccvccv', u'ccv', u'ccvc', u'v', u'cv', u'ccv', u'vccv', u'blonde', u'BLONDE', u'blo', u'blon', u'e', u'de', u'nde', u'onde', 'false', 'false', 'false', 'false', 'false', '_NULL_', u'http://dig.isi.edu/url2'], 
      [u'hair', u'xxxx', u'cvvc', u'cvv', u'cvvc', u'c', u'vc', u'vvc', u'cvvc', u'hair', u'HAIR', u'hai', u'hair', u'r', u'ir', u'air', u'hair', 'false', 'false', 'false', 'false', 'false', '_NULL_', u'http://dig.isi.edu/url2'],
      ""]

limit = 50

if __name__ == "__main__":
    sc = SparkContext(appName="MTurk")
    inputFilename = sys.argv[1]
    outputDirectory = sys.argv[2]
    featureListFilename = sys.argv[3]
    crfModelFilename = sys.argv[4]
    eyeRef = sys.argv[5]
    eyeConfig = sys.argv[6]
    hairRef = sys.argv[7]
    hairConfig = sys.argv[8]
    # Program to compute CRF++
    c = crf_features.CrfFeatures(featureListFilename)
    # Add files to be downloaded with this Spark job on every node.
    sc.addFile("/usr/local/bin/crf_test")
    sc.addFile(crfModelFilename)

    # Map to reference sets
    smEye = HybridJaccard(ref_path=eyeRef, config_path=eyeConfig)
    smHair = HybridJaccard(ref_path=hairRef, config_path=hairConfig)

    rdd = sc.sequenceFile(inputFilename)
    if limit:
        rdd = sc.parallelize(rdd.take(limit))

    rdd_json = rdd.mapValues(lambda x: json.loads(x))

    rdd_body = rdd_json.mapValues(lambda x: extract_body(x))
    rdd_body_tokens = rdd_body.mapValues(lambda x: textTokens(x))

    # TBD
    # rdd_title = rdd_json.mapValues(lambda x: extract_title(x))
    # rdd_title_tokens = rdd.title.mapValues(lambda x: textTokens(x))
    # all below should also be done for title

    # not a pair RDD?
    rdd_features = rdd_body_tokens.map(lambda x: (x[0], c.computeFeatMatrix(x[1], False, addLabels=[x[0]], addIndex=True)))
    rdd_pipeinput = rdd_features.mapValues(lambda x: vectorToString(x))

    cmd = SparkFiles.get("crf_test") + " -m " + SparkFiles.get(crfModelFilename)
    rdd_crf = rdd_pipeinput.values().pipe(cmd)
    # not a pair RDD
    # but we have the URI in the -3 position
    # and the index in the -2 position
    rdd_withuri = rdd_crf.map(lambda x: reconstructTuple(x))

    rdd_grouped = rdd_withuri.groupByKey()
    rdd_flat = rdd_grouped.mapValues(lambda x: [l[1:] for l in sorted(x, key=lambda r: int(r[0]))])
    rdd_harvested = rdd_flat.mapValues(lambda x: computeSpans(x, indexed=True))

    # This has the effect of generating 0, 1, 2, ... lines according to the number of spans
    rdd_controlled = rdd_harvested.flatMapValues(lambda x: list(x))
    # map any eyeColor spans using smEye, hairType spans using smHair
    rdd_aligned = rdd_controlled.mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": smEye, "hairType": smHair}))

    rdd_final = rdd_aligned
    rdd_final.saveAsTextFile(outputDirectory)
