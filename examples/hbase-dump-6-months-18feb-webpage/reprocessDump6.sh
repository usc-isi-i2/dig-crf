#! /bin/bash

time ${DIG_CRF_HOME}/src/extract/extractDump6DescriptionsAndSaveKjsonl.sh
time ${DIG_CRF_HOME}/src/extract/extractDump6DescriptionsAndSaveSeq.sh
time ./tokenize1kDump6DescriptionsAndSaveKjsonl.sh
time ./tokenizeDump6DescriptionsAndSaveKjsonl.sh
time ./tokenizeDump6DescriptionsAndSaveSeq.sh
