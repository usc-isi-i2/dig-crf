"""Given a SEQ(Text, Text) input file to use as a pair RDD (key1, value1),
where the SEQ value field (value1) is supposed to be a (key2, value2)
dictionary in JSON, extract all dictionary values (value2) for a specific
value key (key2).  Optionally, select an additional value (newRddKey) from each
dictionary to use as a replacement for key1 in the output.  Return a new pair
RDD of (key1, value2) (or (newKey1, value2)).  Print the count of records extracted.

The key extraction behavior has been made more complicated.

-k supplies the name of the key to extract (key2, above).  It may be a list of
keys sepearated by commas (","), and the keys may give a path through nested
dictionaries, separated by colons (":").  The separators (",", ":") are fixed
at the moment.  Examples:

-k extractions:text:results,extractions:text:title

When -k supplies a list of keys, the code will consider the extraction
successful if at least one key path in the list succeeds.  When multiple paths
succeed, the results are appended (see below).  The value at the end of a key
path may be a list of strings instead of a single string; if so, the results
are appended.

When appending results and not tokenizing:

1) empty strings are ignored, and

2) a space is inserted a s a seperator between non-empty strings.

When appending results and tokenizing, each string is tokenized independently
and the resulting lists of tokens are concatenated (more precisely, the result
list is extended).

You may ask, why not concatenate strings (with a space separator) and then, if
requested, tokenize?  This would indeed simplify the code, but there's an
advantage to the tokenize-then-concatenate approach: the tokenizer is reset at
hard boundaries, thus limiting possible runaway effects, such as an unclosed
HTML entity.

newRddKey (-K) does not yet support this complex behavior.  It expects
the newRDDKey to be a single string field in the outermost dictionary
of the input object.

"""

import argparse
import datetime
import json
import sys
import time
from pyspark import SparkContext
import crf_tokenizer as crft

def main(argv=None):
    '''this is called if run from command line'''
    print "========================================"
    print "Starting extractAndTokenizeField"
    print "========================================"

    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--count', help="Report a count of records extracted.", required=False, action='store_true')
    parser.add_argument('--cache', help="Cache the RDD in memory.", required=False, action='store_true')
    parser.add_argument('-k','--key', help="The key for the value being extracted.", required=True)
    parser.add_argument('-K','--newRddKeyKey', help="The key for the value to use as the new RDD key.", required=False)
    parser.add_argument('-i','--input', help="Seq or tuple input file on cluster.", required=True)
    parser.add_argument(     '--inputTuples', help="The input file is in tuple format.", required=False, action='store_true')
    parser.add_argument('-n','--notokenize', help="Do not tokenize.", required=False, action='store_true')
    parser.add_argument('-o','--output', help="Save in an output file.", required=False)
    parser.add_argument(     '--outputSeq', help="Use a SEQ file for output.", required=False, action='store_true')
    parser.add_argument(     '--outputTuples', help="Use a tuple file for output.", required=False, action='store_true')
    parser.add_argument(     '--prune', help="Remove records without the extraction key.", required=False, action='store_true')
    parser.add_argument(     '--recognizeHtmlTags', help="Recognize HTML tags.", required=False, action='store_true')
    parser.add_argument('-r','--repartition', type=int, default=0, help="Optionally repartition or coalesce.", required=False)
    parser.add_argument('-s','--show', help="Print the results.", required=False, action='store_true')
    parser.add_argument(     '--skipHtmlTags', help="Skip HTML tags.", required=False, action='store_true')
    parser.add_argument('-t','--take', type=int, default=0, help="Optionally subset to the first n input records.", required=False)
    args = parser.parse_args()

    extractionKey = args.key
    pruning = args.prune
    newRddKeyKey = args.newRddKeyKey

    tok = crft.CrfTokenizer()
    tok.setGroupPunctuation(True)
    tok.setRecognizeHtmlTags(args.recognizeHtmlTags)
    tok.setSkipHtmlTags(args.skipHtmlTags)
    tok.setRecognizeHtmlEntities(True)

    print "========================================"
    print "Creating SparkContext."
    print "========================================"
    # TODO: Use time.monotonic() in python >= 3.3
    startTime = time.time() # Start timing here.
    sc = SparkContext()

    print "========================================"
    print "SparkContext created. Application ID: "
    print sc.applicationId
    # TODO: use time.monotonic() in Python >= 3.3
    duration = time.time() - startTime
    print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
    print "========================================"

    global recordCount, valueCount, noValueCount, emptyValueCount
    global exceptionNoValueCount, exceptionEmptyValueCount, extractionKeyPathHitCounts
    recordCount = sc.accumulator(0)
    valueCount = sc.accumulator(0)
    noValueCount = sc.accumulator(0)
    emptyValueCount = sc.accumulator(0)
    exceptionNoValueCount = sc.accumulator(0)
    exceptionEmptyValueCount = sc.accumulator(0)

    extractionKeyPaths = extractionKey.split(",");
    extractionKeyPathComponents = []
    extractionKeyPathHitCounts = []
    for extractionKeyPath in extractionKeyPaths:
        extractionKeyPathComponents.append(extractionKeyPath.split(":"))
        extractionKeyPathHitCounts.append(sc.accumulator(0))

    def extractStringValues(jsonDataString):
        """Extract one or more string fields from the JSON-encoded data. Returns an iterator for flatMapValues(...), so pruning can cause a record to be skipped."""
        global recordCount, valueCount, noValueCount, emptyValueCount, exceptionNoValueCount, exceptionEmptyValueCount, extractionKeyPathHitCounts
        try:
            recordCount += 1
            gotResult = False
            result = ""
            jsonData = json.loads(jsonDataString)
            keyPathIndex = 0
            for keyComponents in extractionKeyPathComponents:
                value = jsonData
                goodKeyPath = True
                for keyComponent in keyComponents:
                    if keyComponent in value:
                        value  = value[keyComponent]
                    else:
                        goodKeyPath = False
                        break
                if goodKeyPath:
                    if isinstance(value, basestring):
                        gotResult = True
                        if len(value) > 0:
                            if len(result) > 0:
                                result += " " # Join multiple results with a space.
                            result += value
                            extractionKeyPathHitCounts[keyPathIndex] += 1
                    elif isinstance(value, list):
                        gotNonEmptyValue = False
                        for val in value:
                            if isinstance(val, basestring):
                                gotResult = True
                                if len(val) > 0:
                                    gotNonEmptyValue = True
                                    if len(result) > 0:
                                        result += " " # Join multiple results with a space.
                                    result += val
                        if gotNonEmptyValue:
                            extractionKeyPathHitCounts[keyPathIndex] += 1
                keyPathIndex += 1

            if gotResult:
                valueCount += 1
                return iter([result])
            if pruning:
                noValueCount += 1
                return iter(())
            else:
                emptyValueCount += 1
                return iter([""])

        except:
            # TODO: optionally count these failures or die
            if pruning:
                exceptionNoValueCount += 1
                return iter(())
            else:
                exceptionEmptyValueCount += 1
                return iter([""])

    def extractTokenValues(jsonDataString):
        """Extract one or more string fields from the JSON-encoded data and tokenize.  Returns an iterator for flatMapValues(...), so pruning can cause a record to be skipped."""
        global recordCount, valueCount, noValueCount, emptyValueCount, exceptionNoValueCount, exceptionEmptyValueCount, extractionKeyPathHitCounts
        try:
            recordCount += 1
            gotResult = False
            result = []
            jsonData = json.loads(jsonDataString)
            keyPathIndex = 0
            for keyComponents in extractionKeyPathComponents:
                value = jsonData
                goodKeyPath = True
                for keyComponent in keyComponents:
                    if keyComponent in value:
                        value = value[keyComponent]
                    else:
                        goodKeyPath = False
                        break
                if goodKeyPath:
                    if isinstance(value, basestring):
                        gotResult = True
                        if len(value) > 0:
                            tokens = tok.tokenize(value)
                            if len(tokens) > 0:
                                result.extend(tokens)
                                extractionKeyPathHitCounts[keyPathIndex] += 1
                    elif isinstance(value, list):
                        gotNonEmptyValue = False
                        for val in value:
                            if isinstance(val, basestring):
                                gotResult = True
                                if len(val) > 0:
                                    tokens = tok.tokenize(val)
                                    if len(tokens) > 0:
                                        result.extend(tokens)
                                        gotNonEmptyValue = True
                        if gotNonEmptyValue:
                            extractionKeyPathHitCounts[keyPathIndex] += 1
                keyPathIndex += 1

            if gotResult:
                valueCount += 1
                return iter([result])
            if pruning:
                noValueCount += 1
                return iter(())
            else:
                emptyValueCount += 1
                return iter([[]])

        except:
            # TODO: optionally count these failures or die
            if pruning:
                exceptionNoValueCount += 1
                return iter(())
            else:
                exceptionEmptyValueCount += 1
                return iter([""])

    global newRddCount, noNewRddCount, extractNewRddExceptionCount
    newRddCount = sc.accumulator(0)
    noNewRddCount = sc.accumulator(0)
    extractNewRddExceptionCount = sc.accumulator(0)

    def extractNewRddKey(pairData):
        """Extract a new RDD key from the JSON-encoded data in pair data. Returns an iterator for flatMap(...), so records without new RDD keys can be skipped."""
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

    if args.inputTuples:
        data = sc.textFile(args.input).map(lambda x: eval(x))
    else:
        # Open the input file, a HadoopFS sequence file.
        data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

    if args.take > 0:
        data = sc.parallelize(data.take(args.take))

    if newRddKeyKey != None:
        print "========================================"
        print "Extracting new RDD keys."
        print "========================================"
        data = data.flatMap(extractNewRddKey)
        print "========================================"
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

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

    print "========================================"
    # TODO: use time.monotonic() in Python >= 3.3
    duration = time.time() - startTime
    print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
    print "========================================"

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
        print "========================================"
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.cache:
        print "========================================"
        print "Caching the extracted value pairs."
        print "========================================"
        extractedValuePairs.cache()
        print "========================================"
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.count:
        print "========================================"
        print "Counting records..."
        print "========================================"
        localRecordCount = extractedValuePairs.count()
        print "========================================"
        print "Record count: %d" % localRecordCount
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.show:
        print "========================================"
        for record in extractedValuePairs.collect():
            print record
        print "========================================"
        print "========================================"
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"

    if args.output != None and len(args.output) > 0:
        # JSON encode the extracted values:
        encodedValuePairs = extractedValuePairs.mapValues(lambda x: json.dumps(x))
        print "========================================"
        # TODO: use time.monotonic() in Python >= 3.3
        duration = time.time() - startTime
        print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
        print "========================================"


        if args.outputSeq:
            print "========================================"
            print "Saving the results in a Hadoop SEQ file."
            print args.output
            print "========================================"
            encodedValuePairs.saveAsNewAPIHadoopFile(args.output,
                                                     "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                                                     "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
        elif args.outputTuples:
            print "========================================"
            print "Saving the results as tuples in a text file."
            print args.output
            print "========================================"
            # Save the result as a text file:
            encodedValuePairs.saveAsTextFile(args.output)
        else:
            print "========================================"
            print "Saving the results in a keyed JSON Lines text file."
            print args.output
            print "========================================"

            # Join the key and JSON-encoded value, using a tab as the separator:
            keyValueTextRDD = encodedValuePairs.map(lambda x: x[0] + '\t' + x[1])
            print "========================================"
            # TODO: use time.monotonic() in Python >= 3.3
            duration = time.time() - startTime
            print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
            print "========================================"

            # Save the result as a text file:
            keyValueTextRDD.saveAsTextFile(args.output)

    print "========================================"
    # TODO: use time.monotonic() in Python >= 3.3
    duration = time.time() - startTime
    print "Elapsed time: %s" % str(datetime.timedelta(seconds=duration))
    print "recordCount = %d" % recordCount.value
    if newRddKeyKey != None:
        print "newRddCount = %d" % newRddCount.value
        print "noNewRddCount = %d" % noNewRddCount.value
        print "extractNewRddExceptionCount = %d" % extractNewRddExceptionCount.value
    print "valueCount = %d" % valueCount.value
    print "noValueCount = %d" % noValueCount.value
    print "emptyValueCount = %d" % emptyValueCount.value
    print "exceptionNoValueCount = %d" % exceptionNoValueCount.value
    print "exceptionEmptyValueCount = %d" % exceptionEmptyValueCount.value

    for keyPath, hitCount in zip(extractionKeyPaths, extractionKeyPathHitCounts):
        print "hitCount[\"%s\"] = %d" % (keyPath, hitCount.value)

    print "========================================"

    print "========================================"
    print "All done."
    print "========================================"
    sc.stop()

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())
