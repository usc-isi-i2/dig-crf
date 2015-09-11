#!/bin/sh

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

base64 -D ${INPUT} | $CRFTEST -m ${MODEL} | $GGREP -Pa '/\d{5}/\d{5}\t[^\t]+$' | $GGREP -Pva '\tO$' | cut -f 1,25,26 | base64

