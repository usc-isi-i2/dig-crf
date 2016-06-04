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

    sc = SparkContext()

    global tokenCount, noTokenCount, emptyTokenCount, exceptionNoTokenCount, exceptionEmptyTokenCount
    tokenCount = sc.accumulator(0)
    noTokenCount = sc.accumulator(0)
    emptyTokenCount = sc.accumulator(0)
    exceptionNoTokenCount = sc.accumulator(0)
    exceptionEmptyTokenCount = sc.accumulator(0)   

    def extractStringValues(jsonData):
        """Extract a string field from the JSON-encoded data. Returns an iterator for flatMapValues(...), so pruning can cause a record to be skipped."""
        global tokenCount, noTokenCount, emptyTokenCount, exceptionNoTokenCount, exceptionEmptyTokenCount
        try:
            d = json.loads(jsonData)
            if extractionKey in d:
                result = iter([d[extractionKey]])
                tokenCount += 1
                return result
            else:
                if pruning:
                    noTokenCount += 1
                    return iter(())
                else:
                    emptyTokenCount += 1
                    return iter("")

        except:
            # TODO: optionally count these failures or die
            if pruning:
                exceptionNoTokenCount += 1
                return iter(())
            else:
                exceptionEmptyTokenCount += 1
                return iter("")

    def extractTokenValues(jsonData):
        """Extract tokens from a string field from the JSON-encoded data. Returns an iterator for flatMapValues(...), so pruning can cause a record to be skipped."""
        global tokenCount, noTokenCount, emptyTokenCount, exceptionNoTokenCount, exceptionEmptyTokenCount
        try:
            d = json.loads(jsonData)
            if extractionKey in d:
                result = iter([tok.tokenize(d[extractionKey])])
                tokenCount += 1
                return result
            else:
                if pruning:
                    noTokenCount += 1
                    return iter(())
                else:
                    emptyTokenCount += 1
                    return iter([])
        except:
            # TODO: optionally count these failures or die
            if pruning:
                exceptionNoTokenCount += 1
                return iter(())
            else:
                exceptionEmptyTokenCount += 1
                return iter([])

    global newRddCount, noNewRddCount, extractNewRddExceptionCount
    newRddCount = sc.accumulator(0)
    noNewRddCount = sc.accumulator(0)
    extractNewRddExceptionCount = sc.accumulator(0)

    def extractNewRddKey(pairData):
        """Extract a new RDD key from the JSON-encoded data in pair data. Returns an iterator for flatMap(...), so records without ren RDD keys can be skipped."""
        global newRddCount, noNewRddCount, extractNewRddExceptionCount
        try:
            d = json.loads(pairData[1])
            if newRddKeyKey in d:
                result = iter([(d[newRddKeyKey], pairData[1])])
                newRddCount += 1
                return result
            else:
                noNewRddCount += 1
                return iter(())

        except:
            # TODO: optionally count these failures or die
            extractNewRddExceptionCount += 1
            return iter(())

    # Open the input file, a HadoopFS sequence file.
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    if args.take > 0:
        data = sc.parallelize(data.take(args.take))

    if newRddKeyKey != None:
        print "========================================"
        print "Extracting new RDD keys."
        print "========================================"
        data = data.flatMap(extractNewRddKey)

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
    if newRddKeyKey != None:
        print "newRddCount = %d" % newRddCount.value
        print "noNewRddCount = %d" % noNewRddCount.value
        print "extractNewRddExceptionCount = %d" % extractNewRddExceptionCount.value
    print "tokenCount = %d" % tokenCount.value
    print "noTokenCount = %d" % noTokenCount.value
    print "emptyTokenCount = %d" % emptyTokenCount.value
    print "exceptionNoTokenCount = %d" % exceptionNoTokenCount.value
    print "exceptionEmptyTokenCount = %d" % exceptionEmptyTokenCount.value
    print "========================================"

    print "========================================"
    print "All done."
    print "========================================"
    sc.stop()

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

