import json

"""Tools for reading a Web scrapings file.

At the outer level, a Web scrapings file is a sequence of sentences.
Each sentence has a set of properties:

    text: a single string
    allTokens: a sequence of strings
    annotationset: a set of properties
    uri

This code is not terribly pythonic.  It would be better, especially when the
input data is in keyed JSON Lines format, to return an iterator over
sentences.  A sentence would be a different class, with property access
methods.

"""
class Scrapings:

    TEXT_MARKER = "text"
    ALL_TOKENS_MARKER = "allTokens"
    ANNOTATION_SET_MARKER = "annotationSet"
    URI_MARKER = "uri"

    def __init__ (self, jsonFilename, kj=False):
        self.sentences = []
        self.keys = None
        self.load(jsonFilename, kj)

    def load (self, jsonFilename, kj=False):
        """Load a Web scrapings file in either JSON or keyed JSON Lines format. The entire file is read."""
        if kj:
            self.loadKeyedJsonLines(jsonFilename)
        else:
            self.loadJson(jsonFilename)
                
    def loadJson (self, jsonFilename):
        """Load a Web scrapings file in JSON format. The entire file is read."""
        with open(jsonFilename) as jsonFile:
            self.sentences = json.load(jsonFile)
            jsonFile.close()
                
    def loadKeyedJsonLines (self, jsonFilename):
        """Load a Web scrapings file formatted as keyed JSON Lines. The entire file is read."""
        self.sentences = []
        self.keys = []
        with open(jsonFilename) as jsonFile:
            for line in jsonFile:
                key, jsonData =  line.split('\t', 1)
                # Note: we might want to strip quotes from the key, depending on whether we decide to quote the keys in keyed JSON Lines files.
                self.keys.append(key)
                self.sentences.append(json.loads(jsonData))
            jsonFile.close()
                
    def sentenceCount (self):
        """At the top level, a Web scrapings file contains sentences on a sequences of sentences.  Return the count of sentences."""
        return len(self.sentences)

    def getText (self, sentenceIndex):
        """Get the text for a specific sentence."""
        return self.sentences[sentenceIndex][self.TEXT_MARKER]

    def getAllTokens (self, sentenceIndex):
        """Get all the tokens for a specific sentence."""
        return self.sentences[sentenceIndex][self.ALL_TOKENS_MARKER]

    def getUri (self, sentenceIndex):
        """Get the internal URI value."""
        return self.sentences[sentenceIndex][self.URI_MARKER]

    def getKey (self, sentenceIndex):
        """Get the key if provided, falling back to the internal URI."""
        if self.keys != None:
            return self.keys[sentenceIndex]
        else:
            return self.getUri(sentenceIndex)
