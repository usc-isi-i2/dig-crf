import json

"""Tools for reading a Web scrapings file.

At the outer level, a Web scrapings file is a sequence of sentences.
Each sentence has a set of properties:
    text: a single string
    allTokens: a sequence of strings
    annotationset: a set of properties
    uri

This code is not terribly pythonic.
It might be better to return an iterator over sentences.  A sentence would be a different class, with property access methods."""
class Scrapings:

    TEXT_MARKER = "text"
    ALL_TOKENS_MARKER = "allTokens"
    ANNOTATION_SET_MARKER = "annotationSet"
    URI_MARKER = "uri"

    def __init__ (self, jsonFilename):
        self.jsonData = []
        self.load(jsonFilename)

    def load (self, jsonFilename):
        """Load a Web scrapings file."""
        with open(jsonFilename) as jsonFile:
            self.jsonData = json.load(jsonFile)
            jsonFile.close()
                
    def sentenceCount (self):
        """At the top level, a Web scrapings file contains data on a sequences of sentences.  Return the count of sentences."""
        return len(self.jsonData)

    def getText (self, sentenceIndex):
        """Get the text for a specific sentence."""
        return self.jsonData[sentenceIndex][self.TEXT_MARKER]

    def getAllTokens (self, sentenceIndex):
        """Get all the tokens for a specific sentence."""
        return self.jsonData[sentenceIndex][self.ALL_TOKENS_MARKER]
