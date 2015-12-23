#!/usr/bin/env python

try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import sys
import argparse
from prep import Prep
from digTokenizer.tokenizer import Tokenizer
from digSparkUtil import FileUtil, as_dict, dict_minus

def testPrep(sc, input_dir, output_dir, config,
                  limit=None, 
                  sampleSeed=1234,
                  debug=0, 
                  location='hdfs',
                  input_file_format="sequence",
                  input_data_type="json",
                  output_file_format="sequence",
                  output_data_type="json",
                  verbose=False,
                  **kwargs):

    futil = FileUtil(sc)

    # LOAD DATA
    print(input_dir, input_file_format, input_data_type)
    rdd_ingest = futil.load_file(input_dir, file_format=input_file_format, data_type=input_data_type)
    rdd_ingest.setName('rdd_ingest_input')

    # LIMIT/SAMPLE (OPTIONAL)
    if limit==0:
        limit = None
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_ingest.count()
        rdd_ingest = rdd_ingest.sample(False, ratio, seed=sampleSeed)
        
    ## TOKENIZE
    tok_options = {"verbose": verbose,
                   "file_format": input_file_format,
                   "data_type": input_data_type}
    tokenizer = Tokenizer(config, **tok_options)
    rdd_tokenized = tokenizer.perform(rdd_ingest)

    ## PREP
    prep_options = {}
    prep = Prep(**prep_options)
    rdd_prep = prep.perform(rdd_tokenized)

    # SAVE DATA
    out_options = {}
    futil.save_file(rdd_prep, output_dir, file_format=output_file_format, data_type=output_data_type, **out_options)

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_dir', required=True)
    parser.add_argument('--input_file_format', default='sequence', choices=('text', 'sequence'))
    parser.add_argument('--input_data_type', default='json', choices=('json', 'csv'))

    parser.add_argument('-o','--output_dir', required=True)
    parser.add_argument('--output_file_format', default='sequence', choices=('text', 'sequence'))
    parser.add_argument('--output_data_type', default='json', choices=('json', 'csv'))

    parser.add_argument('--config', default=None)

    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')

    args=parser.parse_args()
    # Default configuration to empty config
    # (avoid mutable container as default)
    args.config = args.config or {}

    sparkName = "testPrep"
    sc = SparkContext(appName=sparkName)

    # remove positional args, everything else passed verbatim
    kwargs = dict_minus(as_dict(args), "input_dir", "output_dir", "config")
    testPrep(sc, args.input_dir, args.output_dir, args.config, **kwargs)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
