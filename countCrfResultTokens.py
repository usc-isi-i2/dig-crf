"""Count the occurances of tokens in the CRF results, by classifier."""

import argparse
import codecs
import json
import sys
from pyspark import SparkContext

def getTokens(value):
    global goodJsonRecords, badJsonRecords
    try:
        d = json.loads(value)
        goodJsonRecords += 1
        return (key + ': ' + token for key in d.keys() for token in d[key])
    except:
        badJsonRecords += 1
        return iter([])

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Seq input file on cluster.", required=True)
    parser.add_argument('-o','--output', help="UTF-8 output file on cluster.", required=False)
    parser.add_argument('-p','--printToLog', help="Print results to log.", required=False, action='store_true')
    args = parser.parse_args()

    sc = SparkContext()
    global goodJsonRecords, badJsonRecords
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    keyCounts = data.values().flatMap(getTokens).countByValue()

    if args.output != None:
        with codecs.open(args.output, 'wb', 'utf-8') as f:
            for k in sorted(keyCounts):
                f.write(k + " " + str(keyCounts[k]) + "\n")

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    if args.printToLog:
        for k in sorted(keyCounts):
            print json.dumps(k), keyCounts[k]
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

