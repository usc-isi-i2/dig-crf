#!/usr/bin/python

import sys
import re
import pprint
from HTMLParser import HTMLParser, HTMLParseError

from util import identity

# RE for identifying a token
# Summary
# Either a word [a-zA-Z0-9_], extended to Unicode letters where appropriate
# or a single punctuation character
# or a single emoji (probably most non-emoji graphic unicodes should be included as singletons as well)
# Adapted from http://stackoverflow.com/a/26568779/2077242
import string
try:
    # Wide UCS-4 build
    expr1 = (u'[{}]|'.format(re.escape(string.punctuation)) +
        u'[\U0001F300-\U0001F64F'
        u'\U0001F680-\U0001F6FF'
        u'\u2600-\u26FF\u2700-\u27BF]')
    expr2 = '\w+'
    expr = '(?:' + expr1 + '|' + expr2 + ')'
    RE_TOKEN = re.compile(expr, re.UNICODE)
    # print "htmltoken token recognition uses wide UCS-4 build"
except re.error:
    # Narrow UCS-2 build
    expr1 = (u'(?:' +
        u'[{}]|'.format(re.escape(string.punctuation)) +
        u'\ud83c[\udf00-\udfff]|'
        u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
        u'[\u2600-\u26FF\u2700-\u27BF])')
    expr2 = '\w+'
    expr = '(?:' + expr1 + '|' + expr2 + ')'
    RE_TOKEN = re.compile(expr, re.UNICODE)
    # print "htmltoken token recognition uses narrow UCS-2 build"

class HTMLTokenizer(HTMLParser):

    def __init__(self, keepTags=True):
        self.buffer = []
        self.keepTags = keepTags
        # this works only for new-style class
        # super(HTMLParser,self).__init__()
        HTMLParser.__init__(self)
    def reset(self):
        self.buffer = []
        HTMLParser.reset(self)
    def handle_data(self, data):
        if data:
            ### This is where the text tokenization resides
            ### Word-consitutent based tokenization is naive, but probably good enough for simple texts
            ### Other strategies (e.g., nltk.sent_tokenize) are possible
            self.buffer.extend(re.findall(RE_TOKEN, data))
    def handle_starttag(self, tag, attrs):
        # self.buffer.append("<%s" % tag + "".join([''' %s="%s"''' % attr for attr in attrs]) + ">")
        if self.keepTags:
            self.buffer.append("<%s" % tag)
            self.buffer.extend(['''%s="%s"''' % attr for attr in attrs])
            self.buffer.append(">")
    def handle_endtag(self, tag):
        if self.keepTags:
            self.buffer.append("</%s>" % tag)
    def handle_startendtag(self, tag, attrs):
        # self.buffer.append("<%s" % tag + "".join([''' %s="%s"''' % attr for attr in attrs]) + "/>")
        if self.keepTags:
            self.buffer.append("<%s" % tag)
            self.buffer.extend(['''%s="%s"''' % attr for attr in attrs])
            self.buffer.append("/>")
    def handle_entityref(self, name):
        self.buffer.append("&%s;" % name)
    def handle_charref(self, name):
        if name[0].upper() in ['A', 'B', 'C', 'D', 'E', 'F']:
            self.buffer.append("&#x%s;" % name)
        else:
            self.buffer.append("&#%s;" % name)
    def handle_comment(self, data):
        pass
    def handle_decl(self, data):
        pass

DATA = ["this is regular text",
        "<br/>text with<br/>breaks",
        """<a href="bobo"><b>bold<i>bold, italic<tt>bold italic tt</tt>more bi</i>more bold</b>just text</a>and more text""",
        "&#123; &#xA4; &#xa4; &gt;",
        u"""My name is casey\U0001f483\U0001f451<br>\nI have a TEMPTING petite frame with bedroom brown \U0001f48beyes and a sparkling Attitude \u2728\U0001f31f<br>\nWith an \U0001f493ENCHANTING \U0001f493demeanor and a seductively sweet personality to Match!\U0001f48e<br>\nLet Me Be The One To Cater To Your Needs...YOU DESERVE IT! *5'3 *120 *Indian *Cuban call Casey(707)641-3779"""
        ]

def bucketize(x, bucketName='__HTMLMARKUP__'):
    try:
        if x[0] in '<':
            return bucketName
    except:
        pass
    return x

def tokenize(document, interpret=identity, **args):
    tokenizer = HTMLTokenizer(**args)
    tokenizer.feed(document)
    tokenized = tokenizer.buffer
    try:
        tokenizer.close()
    except HTMLParseError:
        print >> sys.stderr, "Failed to parse HTML data %r" % document
    return [interpret(t) for t in tokenized]

def sample():
    """Reuses tokenizer"""
    output = []
    tokenizer = HTMLTokenizer()
    for doc in DATA:
        tokenizer.reset()
        tokenizer.feed(doc)
        tokenized = tokenizer.buffer
        tokenizer.close()
        output.append( (doc, tokenized) )
    return output

def main(argv=None):
    '''this is called if run from command line'''
    s = sample()
    pprint.pprint(s)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
