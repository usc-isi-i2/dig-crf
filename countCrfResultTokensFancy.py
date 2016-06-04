"""Count the occurances of tokens in the CRF results, by classifier."""

import argparse
import json
from operator import attrgetter
import sys
from pyspark import SparkContext

class Token:
    def __init__(self, name, count):
        self.name = name
        self.count = count
        self.cumulativeCount = 0

    def __repr__(self):
        return repr((self.name, self.count, self.cumulativeCount))

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
    keyTokenLists = {}
    for keyToken in keyTokenCounts.keys():
        (key, tokenName) = keyToken.split(":", 1)
        count = keyTokenCounts[keyToken]
        if key not in keyTokenLists:
            keyTokenLists[key] = []
        keyTokenLists[key].append(Token(tokenName, count))

    # Process each key seperately
    for key in keyTokenLists.keys():
        tokenList = keyTokenLists[key]

        # Sort the tokens by descending count and ascending token value:
        sortedTokenList = sorted(tokenList, key=attrgetter("name"))
        sortedTokenList = sorted(sortedTokenList, key=attrgetter("count"), reverse=True)

        # Calculate the cumulative token count for each token in sorted order:
        totalTokens = 0
        for token in sortedTokenList:
            totalTokens += token.count
            token.cumulativeCount = totalTokens

        # Print the sorted tokens with cumulative counts, fraction of total, and index.
        print "========================================"
        tokenIndex = 0
        floatTotalTokens = float(totalTokens)
        for token in sortedTokenList:
            tokenIndex += 1
            count = token.count
            cumulativeCount = token.cumulativeCount
            fractionOfTotal = cumulativeCount / floatTotalTokens
            print("{0:8d} {1:50} {2:10d} {3:10d} {4:.5f}".format(tokenIndex, json.dumps(key + ": " + token.name), count, cumulativeCount, fractionOfTotal))
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

