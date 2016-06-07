"""Count the occurances of tokens in the CRF results, by classifier."""

import argparse
import json
from operator import attrgetter
import sys
from pyspark import SparkContext

class Token:
    def __init__(self, value, count):
        self.value = value
        self.count = count
        self.cumulativeCount = 0

    # for debugging output:
    def __repr__(self):
        return repr((self.value, self.count, self.cumulativeCount))

def getTokens(value):
    global goodJsonRecords, badJsonRecords
    try:
        d = json.loads(value)
        goodJsonRecords += 1
        return (tag + ':' + token for tag in d.keys() for token in d[tag])
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
    tagTokenCounts = data.values().flatMap(getTokens).countByValue()
    sc.stop()

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "========================================"

    # Restructure the data, grouping by tag (token type indicator):
    tagTokenLists = {}
    for tagToken in tagTokenCounts.keys():
        (tag, tokenValue) = tagToken.split(":", 1)
        count = tagTokenCounts[tagToken]
        if tag not in tagTokenLists:
            tagTokenLists[tag] = []
        tagTokenLists[tag].append(Token(tokenValue, count))

    # Process each tag seperately:
    for tag in tagTokenLists.keys():
        tokenList = tagTokenLists[tag]

        # Sort the tokens by descending count and ascending token value:
        sortedTokenList = sorted(tokenList, key=attrgetter("value"))
        sortedTokenList = sorted(sortedTokenList, key=attrgetter("count"), reverse=True)

        # Calculate the cumulative token count for each token in sorted order:
        totalTokens = 0
        for token in sortedTokenList:
            totalTokens += token.count
            token.cumulativeCount = totalTokens

        # We'll use the final total later, but we need it as a float to ensure
        # floating point division is used:
        floatTotalTokens = float(totalTokens)

        # Print the sorted tokens with cumulative counts, fraction of
        # total (cunumative distribution function), and index
        # (enumerate the tokens per tag, starting with 1).
        print "========================================"
        tokenIndex = 0
        for token in sortedTokenList:
            tokenIndex += 1
            fractionOfTotal = token.cumulativeCount / floatTotalTokens
            print("{0:8d} {1:50} {2:10d} {3:10d} {4:.5f}".format(tokenIndex, json.dumps(tag + ": " + token.value),
                                                                 token.count, token.cumulativeCount, fractionOfTotal))
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

