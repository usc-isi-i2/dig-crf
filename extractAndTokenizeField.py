"""Given a SEQ(Text, Text) input file to use as a pair RDD (key1, value1),
where the SEQ value field (value1) is supposed to be a (key2, value2)
dictionary in JSON, extract all dictionary values (value2) for a specific
value key (key2).  Optionally, select an additional value (newRddKey) from each
dictionary to use as a replacement for key1 in the output.  Return a new pair
RDD of (key1, value2) (or (newKey1, value2)).  Print the count of records extracted.

"""

import argparse
import json
import sys
from pyspark import SparkContext
import crf_tokenizer as crft

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--count', help="Optionally report a count of records extracted.", required=False, action='store_true')
    parser.add_argument('--cache', help="Optionally cache the RDD in memory.", required=False, action='store_true')
    parser.add_argument('-k','--key', help="The key for the value being extracted.", required=True)
    parser.add_argument('-K','--newRddKeyKey', help="The key for the value to use as the new RDD key.", required=False)
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    parser.add_argument('-n','--notokenize', help="Optionally do not tokenize.", required=False, action='store_true')
    parser.add_argument('-o','--output', help="Optionally save in an output file.", required=False)
    parser.add_argument('--outputSeq', help="Optionally use a SEQ file for output.", required=False, action='store_true')
    parser.add_argument('--prune', help="Optionally remove records without the extraction key.", required=False, action='store_true')
    parser.add_argument('-r','--repartition', type=int, default=0, help="Optionally repartition or coalesce.", required=False)
    parser.add_argument('-s','--show', help="Optionally print the results.", required=False, action='store_true')
    parser.add_argument('-t','--take', type=int, default=0, help="Optionally subset to the first n input records.", required=False)
    args = parser.parse_args()

    extractionKey = args.key
    pruning = args.prune
    newRddKeyKey = args.newRddKeyKey

    tok = crft.CrfTokenizer()
    tok.setGroupPunctuation(True)
    tok.setRecognizeHtmlTags(True)
    tok.setRecognizeHtmlEntities(True)

    def extractStringValues(value):
        try:
            d = json.loads(value)
            if extractionKey in d:
                return iter([d[extractionKey]])
            else:
                if pruning:
                    return iter(())
                else:
                    return iter("")

        except:
            # TODO: optionally count these failures or die
            if pruning:
                return iter(())
            else:
                return iter("")

    def extractTokenValues(value):
        try:
            d = json.loads(value)
            if extractionKey in d:
                return iter([tok.tokenize(d[extractionKey])])
            else:
                if pruning:
                    return iter(())
                else:
                    return iter([])
        except:
            # TODO: optionally count these failures or die
            if pruning:
                return iter(())
            else:
                return iter([])

    def extractNewRddKey(value, oldKey):
        try:
            d = json.loads(value)
            if newRddKeyKey in d:
                return iter([d[newRddKeyKey]])
            else:
                return iter(oldKey)

        except:
            # TODO: optionally count these failures or die
            return iter(oldKey)

    sc = SparkContext()
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    if args.take > 0:
        data = sc.parallelize(data.take(args.take))

    if args.notokenize:
        print "========================================"
        print "Extracting string values"
        print "========================================"
        extractedValuePairs = data.flatMapValues(extractStringValues)
    else:
        print "========================================"
        print "Extracting and tokenizing"
        print "========================================"
        extractedValuePairs = data.flatMapValues(extractTokenValues)

    if newRddKeyKey != None:
        print "========================================"
        print "Extracting new RDD keys."
        print "========================================"
        extractedValuePairs = extractedValuePairs.map(lambda x: (extractNewRddKey(x[1], [x0]), x[1]))

    if args.repartition > 0:
        # Repartition if increasing the number of partitions.
        # Coalesce if reducing the number of partitions.
        # Do nothing if the number of partitions won't change.
        numPartitions = extractedValuePairs.getNumPartitions()
        if args.repartition > numPartitions:
            print "========================================"
            print "Repartitioning %d ==> %d" % (numPartitions, args.repartition)
            print "========================================"
            extractedValuePairs = extractedValuePairs.repartition(args.repartition)
        elif args.repartition < numPartitions:
            print "========================================"
            print "Coalescing %d ==> %d" % (numPartitions, args.repartition)
            print "========================================"
            extractedValuePairs = extractedValuePairs.coalesce(args.repartition)

    if args.cache:
        extractedValuePairs.cache()

    if args.count:
        print "(Counting records)"
        recordCount = extractedValuePairs.count()
        print "========================================"
        print "Record count: %d" % recordCount
        print "========================================"

    if args.show:
        print "========================================"
        for record in extractedValuePairs.collect():
            print record
        print "========================================"

    if args.output != None and len(args.output) > 0:
        encodedValuePairs = extractedValuePairs.mapValues(lambda x: json.dumps(x))

        if args.outputSeq:
            print "========================================"
            print "Saving the results in a Hadoop SEQ file."
            print args.output
            print "========================================"
            encodedValuePairs.saveAsNewAPIHadoopFile(args.output,
                                                     "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                                                     "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
        else:
            print "========================================"
            print "Saving the results in a keyed JSON Lines text file."
            print args.output
            print "========================================"
            keyValueTextRDD = encodedValuePairs.map(lambda x: x[0] + '\t' + x[1])
            keyValueTextRDD.saveAsTextFile(args.output)
    print "========================================"
    print "All done."
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

