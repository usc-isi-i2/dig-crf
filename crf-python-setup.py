#!/usr/bin/env python

from setuptools import setup,Extension
import string

setup(name = "mecab-python",
      py_modules=["CRFPP"],
      ext_modules = [Extension("_CRFPP",
                               ["CRFPP_wrap.cxx"],
                               extra_objects=[
                                "../.libs/encoder.o",
                                "../.libs/feature_cache.o",
                                "../.libs/feature_index.o",
                                "../.libs/feature.o",
                                "../.libs/lbfgs.o",
                                "../.libs/libcrfpp.o",
                                "../.libs/node.o",
                                "../.libs/param.o",
                                "../.libs/path.o",
                                "../.libs/tagger.o"],
                               libraries=["pthread"],
                               include_dirs=["../../crf/include"])
                     ])
