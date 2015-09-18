#!/usr/bin/python

import sys
from subprocess import Popen, PIPE, STDOUT

MODEL = sys.argv[1]

# # try:
# #     INPUT = sys.argv[2].open('r')
# # except:
# #     INPUT = sys.stdin

INPUT=sys.stdin

for line in INPUT:
    p = Popen(['./crf_test_filter.sh', MODEL], stdout=PIPE, stdin=PIPE, stderr=STDOUT)    
    f_stdout = p.communicate(input=line.strip()+'\n')[0]
    print f_stdout
                             
#     print line

# try:
#     close(INPUT)
# except:
#     pass


# command is called with one argument MODEL
# command reads from stdin
# read each line from stdin
# execute crf_test_filter.sh MODEL using the line
# write the resulting output to STDOUT


# from subprocess import Popen, PIPE, STDOUT

# p = Popen(['grep', 'f'], stdout=PIPE, stdin=PIPE, stderr=STDOUT)    
# grep_stdout = p.communicate(input=b'one\ntwo\nthree\nfour\nfive\nsix\n')[0]
# print(grep_stdout.decode())
