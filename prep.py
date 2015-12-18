#!/usr/bin/env python

from __future__ import print_function

try:
    from pyspark import SparkContext, SparkFiles
except:
    print("### NO PYSPARK")
import sys
import platform
import socket
from base64 import b64encode

import crf_features

from digSparkUtil.dictUtil import as_dict
from digSparkUtil.fileUtil import FileUtil
from digSparkUtil.listUtil import iter_chunks
from digTokenizer.tokenizer import Tokenizer

# Sniff for execution environment
# this should be made obsolete

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
print("### location %s" % location)

default_options = {"tokenizer_config": "https://raw.githubusercontent.com/usc-isi-i2/dig-crf/devel/config/tokenizer_config.json",
                   # minimum initial number of partitions
                   "num_partitions": None, 
                   # number of documents to send to CRF in one call
                   "chunks_per_partition": 100,
                   # coalesce/down partition to this number after CRF
                   "coalesce_partitions": None,
                   # input_type is now encoded in the tokenization path only
                   # "input_type": DIG_WEBPAGE
                   # any joining with identifiers for Svebor will be done elsewhere
                   "limit": None,
                   "crf_features_filename": "config/features.hair-eye"}
"""


#     show = True if debug>=1 else False
#     def showPartitioning(rdd):
#         "Seems to be significantly more expensive on cluster than locally"
#         if show:
#             partitionCount = rdd.getNumPartitions()
#             try:
#                 valueCount = rdd.countApprox(1000, confidence=0.50)
#             except:
#                 valueCount = -1
#             print "At %s, there are %d partitions with on average %s values" % (rdd.name(), partitionCount, int(valueCount/float(partitionCount)))
#             if valueCount == 0:
#                 showSizeAndExit(rdd)

#     debugOutput = output + '_debug'
#     def debugDump(rdd,keys=True,listElements=False):
#         showPartitioning(rdd)
#         keys=False
#         if debug >= 2:
#             startTime = time.time()
#             outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
#             keyCount = None
#             try:
#                 keyCount = rdd.keys().count() if keys else None
#             except:
#                 pass
#             rowCount = None
#             try:
#                 rowCount = rdd.count()
#             except:
#                 pass
#             elementCount = None
#             try:
#                 elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
#             except:
#                 pass
#             rdd.saveAsTextFile(outdir)
#             endTime = time.time()
#             elapsedTime = endTime - startTime
#             print "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

#     def showSizeAndExit(rdd):
#         try:
#             k = rdd.count()
#         except:
#             k = None
#         print "Just finished %s with size %s" % (rdd.name(), k)
#         exit(0)

configDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

binDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "bin")
def binPath(n):
    return os.path.join(binDir, n)

might need these for config

               num_partitions=None, 
               # number of documents to send to CRF in one call
               chunks_per_partition=100,
               # coalesce/down partition to this number after CRF
               coalesce_partitions=None,
               # inputType
               inputType=DIG_WEBPAGE,
               outputFormat="text",
               limit=None, 
               sampleSeed=1234,
               debug=0, 
               location='hdfs'):
"""

class Prep(object):
    def __init__(self, config, **options):
        self.verbose = options.get('verbose', False)
        self.options = as_dict(options)
        self.config = FileUtil.get_json_config(config)
        if self.verbose:
            print("Prep {} config with {} using options {}".format(self, self.config, self.options),
                  file=sys.stderr)

    @staticmethod
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

    def perform(self, rdd_json):
        """Given an RDD, compute the RDD of CRF features."""

        tokenizer = Tokenizer({"config": self.options["tokenizer_config"]},
                              data_type="json")
        rdd_tokenized = tokenizer.perform(rdd_json)
        rdd_tokenized.setName('rdd_tokenized')

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

        #SEPARATOR = '&amp;nbsp;'
        BODY_SUBDOCUMENT = 0
        #SEPARATOR_SUBDOCUMENT = 1
        #TITLE_SUBDOCUMENT = 2
        c = crf_features.CrfFeatures(self.options['crf_features_filename'])

        def makeMatrix(c, uri, tokens):
            """Simpler than the version in crfprocess"""
            b = c.computeFeatMatrix(tokens, False, addLabels=False, addIndex=False)
            # BODY
            idx = 1
            for row in b:
                if row == u"":
                    pass
                else:
                    label = uri + "/%05d/%05d" % (BODY_SUBDOCUMENT, idx)
                    row.append(label)
                    idx += 1
            # Keep the empty string semaphor from the tokens (last
            # component) for CRF++ purposes (???)
            return b

        # page feature matrix including body, separator, title
        # (vector of vectors, includes separator rows)
        # layout: pageUri -> (python) vector of vectors
        rdd_features = rdd_tokenized.map(lambda (k,v): (k, makeMatrix(c, k, v)))
        rdd_features.setName('rdd_features')

        # unicode UTF-8 representation of the feature matrix
        # layout: pageUri -> unicode UTF-8 representation of the feature matrix
        rdd_vector = rdd_features.mapValues(lambda x: Prep.vectorToUTF8(x))
        rdd_vector.setName('rdd_vector')

        # Disregard keys/partitioning considerations here
        # Drop keys put serialized vectors into lists of size chunks_per_partition, dropping any nulls, then concatenate

        # layout: lists of size up to chunks_per_partition of UTF8(feature vectors)
        chunks_per_partition = self.options['chunks_per_partition']
        rdd_chunked = rdd_vector.values().glom().map(lambda l: [filter(lambda e: e, x) for x in iter_chunks(l, chunks_per_partition)]).map(lambda l: ["".join(x) for x in l])
        rdd_chunked.setName('rdd_chunked')

        # base64 input
        rdd_pipeinput = rdd_chunked.flatMap(lambda x: x).map(lambda r: b64encode(r))
        rdd_pipeinput.setName('rdd_pipeinput')

        return rdd_pipeinput
