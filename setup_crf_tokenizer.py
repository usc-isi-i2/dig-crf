
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'name': 'digCrfTokenizer',
    'description': 'digCrfTokenizer',
    'author': 'Craig Milo Rogers',
    'url': 'https://github.com/usc-isi-i2/dig-crf',
    'download_url': 'https://github.com/usc-isi-i2/dig-crf',
    'author_email': 'rogers@isi.edu',
    'license' : 'Apache License 2.0',
    'version': '0.1.1',
    'package_dir': {'digCrfTokenizer' : 'src/applyCrf'},
    'py_modules': ['digCrfTokenizer.crf_tokenizer']
}

setup(**config)
