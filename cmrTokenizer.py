"""Given a SEQ(Text, Text) input file to use as a pair RDD (key1,
valu1), where the SEQ value field (value1) is supposed to be a (key2,
value2) dictionary in JSON, extract all dictionary values (value2) for
a specific key (key2).  Return a a new pair RDD of (key1, value2).
Print the count of records extracted."""

import argparse
import json
import sys

def cmrTokenize(value):
    NORMAL_STATE = 0
    EOS_STATE = 1
    SKIP_ENTITY_STATE = 2
    SKIP_CHAR_STATE = 3
    state = [NORMAL_STATE]
    tokens = []
    token = [""]
    def finishToken():
        if len(token[0]) > 0:
            tokens.append(token[0])
            token[0] = ""
        state[0] = NORMAL_STATE
    for c in value:
        if state[0] == SKIP_ENTITY_STATE:
            token[0] += c
            if c in ['>']:
                finishToken()
        elif state[0] == SKIP_CHAR_STATE:
            token[0] += c
            if c in [';']:
                finishToken()
        elif c in [' ', '\t', '\n']:
            finishToken()
        elif c in ['.', '?', '!']:
            if state[0] != EOS_STATE:
                finishToken()
                state[0] = EOS_STATE
            token[0] = token[0] + c
        elif c in [',', ';', ':']:
            finishToken()
            token[0] = c
            finishToken()
        elif c in ['<']:
            finishToken()
            token[0] = c
            state[0] = SKIP_ENTITY_STATE
        elif c in ['&']:
            finishToken()
            token[0] = c
            state[0] = SKIP_CHAR_STATE
        else:
            if state[0] != NORMAL_STATE:
                finishToken()
            token[0] = token[0] + c
    finishToken()
    return tokens            

def main(argv=None):
    '''this is called if run from command line'''

    print cmrTokenize("This is a sentence.")
    print cmrTokenize("Buy???This...Now!!!")
    print cmrTokenize("The<bold>only</bold>source.")
    print cmrTokenize("Big&gt;little.")

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())
