import codecs
import json
import crf_tokenizer as crft

"""Tools for reading a Web scrapings file.

At the outer level, a Web scrapings file is a sequence of sentences (each of
which may contain multiple human language sentences.

Each sentence can be thought of as a dictionary, with properties such as:

    text: a single string of text
    allTokens: a sequence of strings
    annotationSet: a set of properties resulting from manual annotation of the sentence
    uri

There are several classes provided in this file to read sentences in a variety
of external formats.  Some of these formats might not provide a full set of
properties for each sentence. Of the properties listed above, only "allTokens"
and "uri" are are required by the applyCrf code.

This code is not terribly pythonic.

"""

class CrfSentence:
    TEXT_MARKER = "text"
    ALL_TOKENS_MARKER = "allTokens"
    ANNOTATION_SET_MARKER = "annotationSet" # Not used yet.
    URI_MARKER = "uri"

    def __init__ (self, sentence, key=None):
        self.sentence = sentence
        self.key = key
        self.tokens = None

    def getText (self):
        """Get the text for a sentence."""
        return self.sentence[self.TEXT_MARKER]

    def getAllTokens (self):
        """Get all the tokens for a sentence."""
        return self.sentence[self.ALL_TOKENS_MARKER]

    def getUri (self):
        """Get the internal URI value."""
        return self.sentence[self.URI_MARKER]

    def getField (self, fieldName):
        return self.sentence.get(fieldName)

    def getKey (self):
        """Get the key if provided, falling back to the internal URI."""
        if self.key != None:
            return self.key
        else:
            return self.getUri()

    def setTokens (self, tokens):
        self.tokens = tokens
                
    def getTokens (self):
        if self.tokens != None:
            return self.tokens
        else:
            return self.getAllTokens()

class CrfSentencesFromJsonFile:
    """Load a Web scrapings file in JSON format, assuming UTF-8 encoding. The
sentences are stored as a single array, so entire file is read during
initialization.

    """
    def __init__ (self, jsonFilename):
        with codecs.open(jsonFilename, 'rb', 'utf-8') as jsonFile:
            self.sentences = json.load(jsonFile)

    def __iter__ (self):
        self.index = 0
        return self

    def next (self):
        """Return the next sentence."""
        if self.index >= len(self.sentences):
            raise StopIteration
        sentence = self.sentences[self.index]
        self.index += 1
        return CrfSentence(sentence)


class CrfSentencesFromJsonLinesSource:
    """Load a Web scrapings source of keyed JSON Lines.  The input source may be
one of three things:

1)      a sequence of (key, jsonLine) pairs (e.g., a Spark paired RDD )

2)      text lines with two tab-seperated fields, key and jsonLine

3)      unkeyed text lines containing just JSON Line data

        jsonLine encodes either a sentence structure, as defined above, or
just an array of tokens.  However, an unkeyed JSON line of just tokens may be
insufficient, as it will not have a key.

        The source is accessed as needed.

    """
    def __init__ (self, source, pairs=False, keyed=False, justTokens=False, extractFrom=None):
        self.source = source
        self.pairs = pairs
        self.keyed = keyed
        self.justTokens = justTokens
        self.extractFrom = extractFrom
        self.tok = crft.CrfTokenizer()
        self.tok.setGroupPunctuation(True)
        self.tok.setRecognizeHtmlTags(True)
        self.tok.setRecognizeHtmlEntities(True)

    def __iter__ (self):
        """Begin iterating over the contents of the source.  There's no option to
re-iterate fromt he beginning."""
        return self;

    def finish(self):
        # End of source.
        self.source = None
        raise StopIteration

    def next (self):
        """Return the next sentence from the source."""
        if self.source == None:
            # We'll end up here if next() is called past the point that we
            # returned StopIteration.  We might end up here if someone attempts to
            # iterate more than once, which is not allowed since we read the input
            # file as we iterate.
            raise StopIteration

        if self.pairs:
            # Optimize the [None, None] into a shared object?
            key, jsonData = next(self.source, [None, None])
            if key == None:
                self.finish() # cleans up and raises StopIteration
        else:
            line = next(self.source, None)
            if line == None:
                self.finish() # cleans up and raises StopIteration

            if self.keyed:
                # Split the line into the key and JSON data.
                key, jsonData = line.split('\t', 1)
            else:
                # There's no key prefacing the JSON Line data.
                key = None
                jsonData = line

        # Parse the JSON Line data and return a CrfSentence.
        if self.justTokens:
            try:
                tokens = json.loads(jsonData)
            except ValueError:
                # TODO: Remove this trap if possible!
                print "jsonData: " + jsonData
                tokens = []
                pass

            if isinstance(tokens, basestring):
                # If we loaded a string instead of a sequence of tokens, auto-tokenize.
                tokens = self.tok.tokenize(tokens)
            sentence = { CrfSentence.ALL_TOKENS_MARKER: tokens }
        else:
            sentence = json.loads(jsonData)

        crfSentence= CrfSentence(sentence, key)

        # Extract from some other field? Tokenize?
        if self.extractFrom != None:
            extractedData = crfSentence.getField(self.extractFrom)
            if extractedData == None:
                extractedData = [] # prevent fallback to getAllTokens()
            elif isinstance(extractedData, basestring):
                # If we loaded a string instead of a sequence of tokens, auto-tokenize.
                extractedData = self.tok.tokenize(extractedData)
            crfSentence.setTokens(extractedData)

        return crfSentence


# Is there a standard library way to do this?
def CrfSentencesJsonLinesReader(lineFilename):
    """This generator reads a file and yields the lines. It closes the file when done."""
    with codecs.open(lineFilename, 'rb', 'utf-8') as lineFile:
        for line in lineFile:
            yield line

def CrfSentencesPairedJsonLinesReader(keyedJsonFilename):
    """This generator reads a keyed JSON Lines file and yields the lines split into (key, jsonLine) pairs. It closes the file when done."""
    with codecs.open(keyedJsonFilename, 'rb', 'utf-8') as keyedJsonFile:
        for line in keyedJsonFile:
            key, jsonData = line.split('\t', 1)
            yield key, jsonData


class CrfSentencesFromJsonLinesFile (CrfSentencesFromJsonLinesSource):
    """Load a Web scrapings file in keyed JSON Lines format with UTF-8 encoding. The file is read as needed for iteration."""
    def __init__ (self, jsonFilename, pairs=False, keyed=False, justTokens=False, extractFrom=None):
        if pairs:
            source = CrfSentencesPairedJsonLinesReader(jsonFilename)
        else:
            source = CrfSentencesJsonLinesReader(jsonFilename)

        super(CrfSentencesFromJsonLinesFile, self).__init__(source, pairs=pairs, keyed=keyed, justTokens=justTokens, extractFrom=extractFrom)
