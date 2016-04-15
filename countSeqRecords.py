"""Given a SEQ(Text, Text) input file to use as an RDD, count the number of records."""

import argparse
import sys
from pyspark import SparkContext

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()
    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    recordCount = data.count()

    print "========================================"
    print recordCount
    print "========================================"

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

