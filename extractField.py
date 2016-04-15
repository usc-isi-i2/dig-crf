"""Given a SEQ(Text, Text) input file to use as a pair RDD (key1,
valu1), where the SEQ value field (value1) is supposed to be a (key2,
value2) dictionary in JSON, extract all dictionary values (value2) for
a specific key (key2).  Return a a new pair RDD of (key1, value2).
Print the count of records extracted."""

import argparse
import json
import sys
from pyspark import SparkContext

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--count', help="Optionally report a count of records extracted.", required=False, action='store_true')
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    parser.add_argument('-k','--key', help="Required extraction key.", required=True)
    parser.add_argument('-s','--sample', type=int, default=0, help="Optionally print a sample of results.", required=False)
    args = parser.parse_args()

    extractionKey = args.key

    def extractValues(value):
        try:
            d = json.loads(value)
            if extractionKey in d:
                return iter([d[extractionKey]])
            else:
                return iter([])
        except:
            return iter([])

    sc = SparkContext()
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    extractedValuePairs = data.flatMapValues(extractValues)

    if args.count:
        recordCount = extractedValuePairs.count()
        print "========================================"
        print recordCount
        print "========================================"

    if args.sample > 0:
        sampleSet = extractedValuePairs.take(args.sample)
        print "========================================"
        for record in sampleSet:
            print record
        print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

