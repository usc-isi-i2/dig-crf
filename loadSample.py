#!/usr/bin/env python

"""This program will read a sample JSON file and print it."""

import argparse
import json
import sys

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    args=parser.parse_args()


    with open(args.input) as json_file:
        json_data = json.load(json_file)
        print(json_data)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
