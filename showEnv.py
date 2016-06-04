"""Given a SEQ(Text, Text) input file to use as an RDD, count the number of records."""

import os
import sys

def main(argv=None):
    for key in sorted(os.environ.keys()):
        print "%30s %s" % (key, os.environ[key])

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())

