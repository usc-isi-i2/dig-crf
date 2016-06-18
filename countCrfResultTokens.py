"""Count the occurances of tokens in the CRF results, by tag."""

# TODO: Add a command line flag listing the tags to extract,
# perhaps also one listing tags to exclude.

import argparse
import codecs
import json
import sys
from pyspark import SparkContext

def getTokensMaker(includeArg, excludeArg):
    includeTags = None
    if includeArg:
        # TODO: Would a dictionary be more efficient later?  Depends on the length.
        includeTags = includeArg.split(",")
    excludeTags = None
    if excludeArg:
        # TODO: Would a dictionary be more efficient later?  Depends on the length.
        excludeTags = excludeArg.split(",")
    def getTokens(value):
        global goodJsonRecords, badJsonRecords, excludedTagCount, includedTagCount, tokenCount
        try:
            d = json.loads(value)
            goodJsonRecords += 1
        except:
            badJsonRecords += 1
            return iter([])

        results = []
        for tag in d.keys():
            if (includeTags and tag not in includeTags) or (excludeTags and tag in excludeTags):
                excludedTagCount += 1
                continue;
            includedTagCount += 1
            tokenCount += len(d[tag])
            results.extend([tag + ': ' + token for token in d[tag]])
        return iter(results)
    return getTokens

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-e','--excludeTags', help="Comma-separated list of tags to exclude.", required=False)
    parser.add_argument(     '--includeTags', help="Comma-separated list of tags to include.", required=False)
    parser.add_argument('-i','--input', help="Seq input file on cluster.", required=True)
    parser.add_argument('-o','--output', help="UTF-8 output file on cluster.", required=False)
    parser.add_argument('-p','--printToLog', help="Print results to log.", required=False, action='store_true')
    args = parser.parse_args()

    if args.excludeTags and args.includeTags:
        print "Pick either --excludeTags or --includeTags, not both."
        return 1

    sc = SparkContext()

    global goodJsonRecords, badJsonRecords, excludedTagCount, includedTagCount, tokenCount
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    excludedTagCount = sc.accumulator(0)
    includedTagCount = sc.accumulator(0)
    tokenCount = sc.accumulator(0)

    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    tagTokenCounts = data.values().flatMap(getTokensMaker(args.includeTags, args.excludeTags)).countByValue()
    sc.stop()

    # So far, this code isn't useful.  The output fiile is written by the
    # master node into an isolated folder, and I don't know of a way to
    # retrieve it.
    if args.output != None:
        with codecs.open(args.output, 'wb', 'utf-8') as f:
            for k in sorted(tagTokenCounts):
                f.write(k + " " + str(tagTokenCounts[k]) + "\n")

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "excludedTagCount = %d" % excludedTagCount.value
    print "includedTagCount = %d" % includedTagCount.value
    print "tokenCount = %d" % tokenCount.value
    if args.printToLog:
        for k in sorted(tagTokenCounts):
            print json.dumps(k), tagTokenCounts[k]
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

