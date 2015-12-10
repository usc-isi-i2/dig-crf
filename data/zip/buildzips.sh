#!/bin/sh

cp ../../../dig-mturk/mturk/src/main/python/htmltoken.py .
zip crfprocess.zip htmltoken.py
rm htmltoken.py

cp ../../../dig-mturk/mturk/src/main/python/util.py .
zip crfprocess.zip util.py
rm util.py

cp ../../crf_features.py .
zip crfprocess.zip crf_features.py
rm crf_features.py

cp ../../../hybrid-jaccard/jaro.py .
zip crfprocess.zip jaro.py
rm jaro.py

cp ../../../hybrid-jaccard/munkres.py .
zip crfprocess.zip munkres.py
rm munkres.py

cp ../../../hybrid-jaccard/typo_tables.py .
zip crfprocess.zip typo_tables.py
rm typo_tables.py

cp ../../../hybrid-jaccard/Stringmatcher.py .
zip crfprocess.zip Stringmatcher.py
rm Stringmatcher.py

cp ../../../hybrid-jaccard/hybridJaccard.py .
zip crfprocess.zip hybridJaccard.py
rm hybridJaccard.py

cp ../../bin/crf_test.linux ./crf_test
chmod a+x crf_test
zip crfprocess.zip crf_test
rm crf_test

cp ../../bin/crf_test_filter.sh .
chmod a+x crf_test_filter.sh
zip crfprocess.zip crf_test_filter.sh
rm crf_test_filter.sh

cp ../../bin/apply_crf_lines.py .
chmod a+x apply_crf_lines.py
zip crfprocess.zip apply_crf_lines.py
rm apply_crf_lines.py

zip crfprocess.zip __init__.py

# eye color

cp ../config/eyeColor_config.txt .
zip crfprocessfiles.zip eyeColor_config.txt
rm eyeColor_config.txt

cp ../config/eyeColor_reference_wiki.txt .
zip crfprocessfiles.zip eyeColor_reference_wiki.txt
rm eyeColor_reference_wiki.txt

# hair color

cp ../config/hairColor_config.txt .
zip crfprocessfiles.zip hairColor_config.txt
rm hairColor_config.txt

cp ../config/hairColor_reference_wiki.txt .
zip crfprocessfiles.zip hairColor_reference_wiki.txt
rm hairColor_reference_wiki.txt

# hair texture

cp ../config/hairTexture_config.txt .
zip crfprocessfiles.zip hairTexture_config.txt
rm hairTexture_config.txt

cp ../config/hairTexture_reference_wiki.txt .
zip crfprocessfiles.zip hairTexture_reference_wiki.txt
rm hairTexture_reference_wiki.txt

# hair length

cp ../config/hairLength_config.txt .
zip crfprocessfiles.zip hairLength_config.txt
rm hairLength_config.txt

cp ../config/hairLength_reference_wiki.txt .
zip crfprocessfiles.zip hairLength_reference_wiki.txt
rm hairLength_reference_wiki.txt

# models/features for the above

cp ../config/features.hair-eye .
zip crfprocessfiles.zip features.hair-eye
rm features.hair-eye

cp ../config/dig-hair-eye-train.model .
zip crfprocessfiles.zip dig-hair-eye-train.model
rm dig-hair-eye-train.model

# working name

cp ../config/workingname_config.txt .
zip crfprocessfiles.zip workingname_config.txt
rm workingname_config.txt

cp ../config/workingname_reference_wiki.txt .
zip crfprocessfiles.zip workingname_reference_wiki.txt
rm workingname_reference_wiki.txt

# ethnicity

cp ../config/ethnicity_config.txt .
zip crfprocessfiles.zip ethnicity_config.txt
rm ethnicity_config.txt

cp ../config/ethnicity_reference_wiki.txt .
zip crfprocessfiles.zip ethnicity_reference_wiki.txt
rm ethnicity_reference_wiki.txt

# models/features for the above

cp ../config/features.name-ethnic .
zip crfprocessfiles.zip features.name-ethnic
rm features.name-ethnic

cp ../config/dig-name-ethnic-train.model .
zip crfprocessfiles.zip dig-name-ethnic-train.model
rm dig-name-ethnic-train.model
