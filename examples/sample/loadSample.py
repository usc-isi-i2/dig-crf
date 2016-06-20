#!/usr/bin/env python

"""This program will read a sample JSON file and print it.  The input file is opened as UTF-8."""

import argparse
import codecs
import json
import sys

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    args=parser.parse_args()


    with codecs.open(args.input, 'rb', 'utf-8') as json_file:
        json_data = json.load(json_file)
        print(json_data)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
