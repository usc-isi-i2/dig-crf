"""Count the occurances of tokens in the CRF results, by tag."""

# TODO: Add a command line flag listing the tags to extract,
# perhaps also one listing tags to exclude.

import argparse
import codecs
import json
import sys
from pyspark import SparkContext

def getTokens(value, includeTags, excludeTags):
    global goodJsonRecords, badJsonRecords
    try:
        d = json.loads(value)
        goodJsonRecords += 1
        for tag in d.keys:
            if (includeTags and tag not in includeTags) or (excludeTags and tag in excludeTags):
                return iter([])
            return (tag + ': ' + token for token in d[tag])
    except:
        badJsonRecords += 1
        return iter([])

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-e','--exclude', help="Comma-separated list of tags to exclude.", required=False)
    parser.add_argument('-i','--input', help="Seq input file on cluster.", required=True)
    parser.add_argument('-o','--output', help="UTF-8 output file on cluster.", required=False)
    parser.add_argument('-p','--printToLog', help="Print results to log.", required=False, action='store_true')
    parser.add_argument('-t','--tags', help="Comma-separated list of tags to include.", required=False)
    args = parser.parse_args()

    if args.exclude and args.tags:
        print "Pick either --exclude or --tags, not both."
        return 1

    sc = SparkContext()
    global goodJsonRecords, badJsonRecords
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    tagCounts = data.values().flatMap(lambda x: getTokens(x, args.tags, args.exclude)).countByValue()

    # So far, this code isn't useful.  The output fiile is written by the
    # master node into an isolated folder, and I don't know of a way to
    # retrieve it.
    if args.output != None:
        with codecs.open(args.output, 'wb', 'utf-8') as f:
            for k in sorted(tagCounts):
                f.write(k + " " + str(tagCounts[k]) + "\n")

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    if args.printToLog:
        for k in sorted(tagCounts):
            print json.dumps(k), tagCounts[k]
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

