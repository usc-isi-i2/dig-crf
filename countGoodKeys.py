"""Given a SEQ(Text, Text) input file to use as an RDD, where the
value field is supposed to be a dictionary in JSON, count the number
of occurances of each unique key in the set of dictionaries.  Print
the resulting map (key => count), sorted by key."""

import argparse
import json
import sys
from pyspark import SparkContext

def getKeys(value):
    try:
        d = json.loads(value)
        return iter(d.keys())
    except:
        return iter([])

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    keyCounts = data.values().flatMap(getKeys).countByValue()

    print "========================================"
    for k in sorted(keyCounts):
        print k, keyCounts[k]
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

