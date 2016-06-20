"""Given a SEQ(Text, Text) input file to use as an RDD, where the
value field is supposed to be a dictionary in JSON, count the number
of occurances of each unique key in the set of dictionaries, for each
publisher.  Print the resulting map (key => count), sorted by key."""

import argparse
import json
import sys
from pyspark import SparkContext

def getKeys(value):
    global goodJsonRecords, badJsonRecords, noExtractionsCount, noTitleCount, noTitleAttribsCount, noTitleAttribsTargetCount
    try:
        d = json.loads(value)
        goodJsonRecords += 1
    except:
        badJsonRecords += 1
        return iter([])

    if "extractions" not in d:
        noExtractionsCount += 1
        return iter([])
    extractions = d["extractions"]

    if "title" not in extractions:
        targetName = "(No Title)"
        noTitleCount += 1
    elif "attribs" not in extractions["title"]:
        targetName = "(No title attribs)"
        noTitleAttribsCount += 1
    elif "target" not in extractions["title"]["attribs"]:
        targetName = "(No title attribs target)"
        noTitleAttribsTargetCount += 1
    else:
        targetName = extractions["title"]["attribs"]["target"]

    return (json.dumps(targetName + ": " + key) for key in d.keys())

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()

    global goodJsonRecords, badJsonRecords, noExtractionsCount, noTitleCount, noTitleAttribsCount, noTitleAttribsTargetCount
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    noExtractionsCount = sc.accumulator(0)
    noTitleCount = sc.accumulator(0)
    noTitleAttribsCount = sc.accumulator(0)
    noTitleAttribsTargetCount  = sc.accumulator(0)

    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    keyCounts = data.values().flatMap(getKeys).countByValue()

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "noExtractionsCount = %d" % noExtractionsCount.value
    print "noTitleCount = %d" % noTitleCount.value
    print "noTitleAttribsCount = %d" % noTitleAttribsCount.value
    print "noTitleAttribsTargetCount = %d" % noTitleAttribsTargetCount.value
    print "========================================"

    for k in sorted(keyCounts):
        print k, keyCounts[k]
    print "========================================"

    sc.stop()

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

