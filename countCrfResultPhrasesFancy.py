"""Count the occurances of phrases in the CRF results, by classifier tag."""

import argparse
import json
from operator import attrgetter
import sys
from pyspark import SparkContext

class Phrase:
    def __init__(self, value, count):
        self.value = value
        self.count = count
        self.cumulativeCount = 0

    # for debugging output:
    def __repr__(self):
        return repr((self.value, self.count, self.cumulativeCount))

def getPhrasesMaker(includeArg, excludeArg):
    includeTags = None
    if includeArg:
        # TODO: Would a dictionary be more efficient later?  Depends on the length.
        includeTags = includeArg.split(",")
    excludeTags = None
    if excludeArg:
        # TODO: Would a dictionary be more efficient later?  Depends on the length.
        excludeTags = excludeArg.split(",")
    def getPhrases(value):
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
                continue

            includedTagCount += 1
            tokenCount += len(d[tag])
            results.append(tag + ': ' + " ".join(d[tag]))
        return iter(results)
    return getPhrases

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
    tagPhraseCounts = data.values().flatMap(getPhrasesMaker(args.includeTags, args.excludeTags)).countByValue()
    sc.stop()

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "excludedTagCount = %d" % excludedTagCount.value
    print "includedTagCount = %d" % includedTagCount.value
    print "tokenCount = %d" % tokenCount.value
    print "========================================"

    # Restructure the data, grouping by tag (phrase type indicator):
    tagPhraseLists = {}
    for tagPhrase in tagPhraseCounts.keys():
        (tag, phraseValue) = tagPhrase.split(":", 1)
        count = tagPhraseCounts[tagPhrase]
        if tag not in tagPhraseLists:
            tagPhraseLists[tag] = []
        tagPhraseLists[tag].append(Phrase(phraseValue, count))

    # Process each tag seperately:
    for tag in tagPhraseLists.keys():
        phraseList = tagPhraseLists[tag]

        # Sort the phrases by descending count and ascending phrase value:
        sortedPhraseList = sorted(phraseList, key=attrgetter("value"))
        sortedPhraseList = sorted(sortedPhraseList, key=attrgetter("count"), reverse=True)

        # Calculate the cumulative phrase count for each phrase in sorted order:
        totalPhrases = 0
        for phrase in sortedPhraseList:
            totalPhrases += phrase.count
            phrase.cumulativeCount = totalPhrases

        # We'll use the final total later, but we need it as a float to ensure
        # floating point division is used:
        floatTotalPhrases = float(totalPhrases)

        # Print the sorted phrases with counts, fraction of total,
        # cumulative counts, cumulative distribution function, and
        # index (enumerate the phrases per tag, starting with 1).
        print "========================================"
        phraseIndex = 0
        for phrase in sortedPhraseList:
            phraseIndex += 1
            fractionOfTotal = phrase.count / floatTotalPhrases
            cumulativeFractionOfTotal = phrase.cumulativeCount / floatTotalPhrases
            print("{0:8d} {1:50} {2:10d} {3:.5f} {4:10d} {5:.5f}".format(phraseIndex, json.dumps(tag + ": " + phrase.value),
                                                                         phrase.count, fractionOfTotal,
                                                                         phrase.cumulativeCount, cumulativeFractionOfTotal))
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

