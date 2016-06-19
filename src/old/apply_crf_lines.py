#!/usr/bin/python

# command is called with one argument MODEL
# command reads from stdin
# read each line from stdin
# execute crf_test_filter.sh MODEL using the line
# write the resulting output to STDOUT

import sys
from subprocess import Popen, PIPE, STDOUT

MODEL = sys.argv[1]

try:
    INPUT = open(sys.argv[2], 'r')
except:
    INPUT = sys.stdin

for line in INPUT:
    # p = Popen(['./crf_test_filter.sh', MODEL], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    p = Popen(['./crf_test_filter.sh', MODEL], stdin=PIPE, stdout=PIPE)
    # fails to put newline between each output
    # p.communicate(input=line.strip()+'\n')
    f_stdout = p.communicate(input=line.strip()+'\n')[0]
    print f_stdout
                             
try:
    close(INPUT)
except:
    pass

# adapted from example http://stackoverflow.com/a/165662/2077242
# from subprocess import Popen, PIPE, STDOUT
# p = Popen(['grep', 'f'], stdout=PIPE, stdin=PIPE, stderr=STDOUT)    
# grep_stdout = p.communicate(input=b'one\ntwo\nthree\nfour\nfive\nsix\n')[0]
# print(grep_stdout.decode())
