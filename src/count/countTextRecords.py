"""Given a text input file to use as an RDD, count the number of records."""

import argparse
import sys
from pyspark import SparkContext

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Required Seq input file on cluster.", required=True)
    args = parser.parse_args()

    sc = SparkContext()
    data = sc.textFile(args.input)
    recordCount = data.count()

    print "========================================"
    print recordCount
    print "========================================"

    sc.stop()

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

