#!/bin/sh

# exit on any error
set -e

# Find appropriate Gnu grep which fully supports -P

ARCH=`uname -s`
case $ARCH in
Linux)
	GGREP=/usr/local/bin/grep
	;;
Darwin)
	GGREP=/opt/local/bin/grep
	;;
*)
	echo "Unrecognized system"
	exit 1
	;;
esac

MODEL=$1
INPUT=$2

# might be ./crf_test when running on cluster
# CRFTEST=/usr/local/bin/crf_test
# CRFTEST=./crf_test
CRFTEST=crf_test

# input will be in base64
# after running the CRF test, each non-null line will have one final tabsep field
# either the label or 'O' if no label applies
# first grep retains only those lines with URI of the format '/DDDDD/DDDDD' (D digit) followed by tab followed by a single field
# second grep drops any lines with 'O', i.e., unlabeled
# finally, retain only columns 1 (input word), 25 (uri), 26 (label)
# probably this depends on the number of features, and thus should be columns 1, -2, -1 instead

# alternatives for encoding/decoding base64
# (1) use unix base64/base64 -D executables
# -- seems to stall in pipeline about 0.5% of the time: so we reimplemented
# -- attempted to write to intermediate files, didn't seem to work reliably
# (2) python -m base64
# -- mostly works
# -- by default, prints output in 76-column lines
# -- I don't know the syntax for decoding
# (3) python -c
# (4) perl (untested)
# (5) openssl (untested)
# (6) python using base64 as an encoding: payload.encode('base64') (untested)

cat ${INPUT} | python -c 'import sys,base64; sys.stdout.write(base64.standard_b64decode(sys.stdin.read()))' | $CRFTEST -m ${MODEL} | $GGREP -Pa '/\d{5}/\d{5}\t[^\t]+$' | $GGREP -Pva '\tO$' | cut -f 1,25,26 | python -c 'import sys,base64; sys.stdout.write(base64.standard_b64encode(sys.stdin.read()))'
