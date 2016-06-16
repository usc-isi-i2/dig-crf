#!/usr/bin/env python

"""Provide applyCrf support for Spark.

Data Flow
==== ====

Data processed through ApplyCrfSpark.perform(...)  (and the lower-level
process(...) routines) is passed through an iterator/generator cascade.  The
code processes a "sentence" at a time, instead of reading all the input before
processing it, or buffering all output before releasing it.  Thus, if the data
sources are also iterators/generators (as is the case for Apache Spark RDDs)
and the data destinations are also prepared to process data with a minimum of
excess buffering (as is the case with Apache Spark RDDs), then the data will
be processed using a minimum of memory.

Depending on option settings, the results take the form of:

1)      a pair RDD (key, taggedPhraseJsonLine), which can be saved to a
        SequenceFile,

2)      a taggedPhraseJsonLine (perhaps with internal key), which can be
        saved to a text file, or

3)      a keyed JSON line (<key> "\t" <taggedPhraseJsonLine>), which can be
        saved to a text file.

Spark Downloads
===== =========

This code has special support for downloading the feature list and model files
to Apache Spark client nodes.  When enabled, the location of the files is
resolved just before they are opened using:

path = self.filePathMapper(path)

where self.filePathMapper has been previously set with:

self.setFilePathMapper(filePathMapper)

You may ask, why couldn't that be done at a higher level, and passed into this
code?  The reason is that calling SparkFiles.get(...) on the driver thread
will not necessarily yield a valid value on worker threads running on other
systems.  This constraint is not stated in the Spark documentation seen so
far, but has been experimentally verified.  Due to the structure of
applyCrfGenerator and perofm(...) methods defined below, it seems safest to
make the call to filePathMapper(...) at a low level to ensure that each worker
thread gets the proper file path.

Usage
=====

        Here are code fragments that illustrate how the applyCrf code might be
used in Apache Spark processing scripts, along with sample output when
processed with the hair/eye CRF++ model and data from the
"adjudicated_modeled_live_eyehair_100_03.json" dataset (which was preprocessed
into keyed JSON Lines format).

Example: keyed JSON Line RDD output

import applyCrfSpark

sc = SparkContext()
tagger = applyCrfSpark.ApplyCrfSpark(args.featlist, args.model,
                                     debug=args.debug, statistics=args.statistics)
inputRDD = sc.textFile(args.input, args.partitions)
resultsRDD = tagger.perform(inputRDD)
resultsRDD.saveAsTextFile(args.output)

Output:

http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691    {"hairType": ["strawberry", "blond", "hair"]}
http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691    {"eyeColor": ["blue", "eyes"]}
http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8    {"hairType": ["long", "blonde", "hair"]}
http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8    {"eyeColor": ["seductive", "blue", "eyes"]}


Example: pair RDD output (SequenceFile)

import applyCrfSpark

sc = SparkContext()
tagger = applyCrfSpark.ApplyCrfSpark(args.featlist, args.model,
                                     inputPairs=True, outputPairs=True,
                                     debug=args.debug, statistics=args.statistics)
inputLinesRDD = sc.textFile(args.input, args.partitions)
inputPairsRDD = inputLinesRDD.map(lambda s: s.split('\t', 1))
resultsRDD = tagger.perform(inputPairsRDD)
resultsRDD.saveAsTextFile(args.output)

Output:

(u'http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691', '{"hairType": ["strawberry", "blond", "hair"]}')
(u'http://dig.isi.edu/sentence/253D8FF7A55A226FDBBC53939DBB90D763E77691', '{"eyeColor": ["blue", "eyes"]}')
(u'http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8', '{"hairType": ["long", "blonde", "hair"]}')
(u'http://dig.isi.edu/sentence/028269F87330E727ACE0A8A39855325C5DD60FF8', '{"eyeColor": ["seductive", "blue", "eyes"]}')
"""

import os
from pyspark import SparkFiles
import applyCrf

class ApplyCrfSpark (applyCrf.ApplyCrf):
    """Apply the CRF++ processor with special support for Apache Spark processing."""

    def perform(self, sourceRDD):
        """Apply the process routine in an Apache Spark context.  Returns an RDD."""
        return sourceRDD.mapPartitions(self.process)

    def sparkFilePathMapper(self, path):
        """When Spark forwards files from the driver to worker nodes, it may be
        necessary to map the filename path on a per-worker node basis."""
        # Note the implication in this code that the feature list file and
        # model file must have unique basenames.
        return SparkFiles.get(os.path.basename(path))
            
    def requestSparkDownload(self, sc):
        """Ask Spark to download the feature list and model files from the driver to
        the clients.  This request must take place in the driver. However, the
        file names must be mapped in the clients for the clients to access
        them."""
        sc.addFile(self.featureListFilePath)
        sc.addFile(self.modelFilePath)
        self.setFilePathMapper(self.sparkFilePathMapper)

    def initializeSparkStatistics(self, sc):
        """Replace the statistics counters with Spark accumulators."""
        if self.statistics:
            for statName in self.statistics:
                self.statistics[statName] = sc.accumulator(0)

    def showStatistics(self):
        if self.statistics:
            for statName in self.statisticNames:
                print "%s = %d" % (statName, self.statistics[statName].value)
