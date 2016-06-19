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

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "excludedTagCount = %d" % excludedTagCount.value
    print "includedTagCount = %d" % includedTagCount.value
    print "tokenCount = %d" % tokenCount.value
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

        # Print the sorted tokens with counts, fraction of total,
        # cumulative counts, cumulative distribution function, and
        # index (enumerate the tokens per tag, starting with 1).
        print "========================================"
        tokenIndex = 0
        for token in sortedTokenList:
            tokenIndex += 1
            fractionOfTotal = token.count / floatTotalTokens
            cumulativeFractionOfTotal = token.cumulativeCount / floatTotalTokens
            print("{0:8d} {1:50} {2:10d} {3:.5f} {4:10d} {5:.5f}".format(tokenIndex, json.dumps(tag + ": " + token.value),
                                                                         token.count, fractionOfTotal,
                                                                         token.cumulativeCount, cumulativeFractionOfTotal))
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

