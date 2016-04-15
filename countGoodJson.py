"""Given a SEQ(Text, Text) input file to use as an RDD, where the
value field is supposed to be JSON, count the number of records with
good JSON."""

import argparse
import json
import sys
from pyspark import SparkContext

def goodJsonFilter(keyValue):
    try:
        x = json.loads(keyValue[1])
        return True
    except:
        return False

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    dataWithGoodJson = data.filter(goodJsonFilter)
    recordCount = dataWithGoodJson.count()

    print "========================================"
    print recordCount
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

