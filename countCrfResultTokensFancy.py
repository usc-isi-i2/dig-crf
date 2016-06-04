"""Count the occurances of tokens in the CRF results, by classifier."""

import argparse
import json
import sys
from pyspark import SparkContext

def getTokens(value):
    global goodJsonRecords, badJsonRecords
    try:
        d = json.loads(value)
        goodJsonRecords += 1
        return (key + ':' + token for key in d.keys() for token in d[key])
    except:
        badJsonRecords += 1
        return iter([])

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()
    global goodJsonRecords, badJsonRecords
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    keyTokenCounts = data.values().flatMap(getTokens).countByValue()
    sc.stop()

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "========================================"

    # Restructure the data, grouping by key (token type indicator).
    splitKeyTokenCounts = {}
    for keyToken in keyTokenCounts.keys():
        (key, token) = keyToken.split(":", 1)
        count = keyTokenCounts[keyToken]
        splitKeyTokenCounts[key][token]["count"] = count
        splitKeyTokenCounts[key][token]["token"] = token # for multilevel sort

    # Process each key seperately
    for key in splitKeyTokenCounts.keys():
        tokenCounts = splitKeyTokenCounts[key]

        # Sort the tokens by count and token value.
        #
        # TODO: descending on count, ascending sort on token value.
        tokensSortedByCount = sorted(tokenCounts, attrgetter=("count", "token"), reverse=True)

        # Calculate the cumulative token count for each token in sorted order:
        totalTokens = 0
        for token in tokensSortedByCount:
            totalTokens += tokenCounts[token]["count"]
            tokenCounts[token]["cumulativeCount"] = totalTokens

        # Print the sorted tokens with cumulative counts, fraction of total, and index.
        print "========================================"
        tokenIndex = 0
        floatTotalTokens = float(totalTokens)
        for token in tokensSortedByCount:
            tokenIndex += 1
            count = tokenCounts[token]["count"]
            cumulativeCount = tokenCounts[token]["cumulativeCount"]
            fractionOfTotal = floatTotalTokens / cumulativeCount
            print("{0:8d} {1:40} {2:10d} {3:10d} {4:.5f}".format(tokenIndex, json.dumps(key + ": " + token), count, cumulativeCount, fractionOfTotal)
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

