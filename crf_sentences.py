import json

"""Tools for reading a Web scrapings file.

At the outer level, a Web scrapings file is a sequence of sentences.
Each sentence has a set of properties:

    text: a single string
    allTokens: a sequence of strings
    annotationset: a set of properties
    uri

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

    def getText (self):
        """Get the text for a sentence."""
        return self.sentence[self.TEXT_MARKER]

    def getAllTokens (self):
        """Get all the tokens for a sentence."""
        return self.sentence[self.ALL_TOKENS_MARKER]

    def getUri (self):
        """Get the internal URI value."""
        return self.sentence[self.URI_MARKER]

    def getKey (self):
        """Get the key if provided, falling back to the internal URI."""
        if self.key != None:
            return self.key
        else:
            return self.getUri()
                
class CrfSentencesIterator:
    def __init__ (self, sentences):
        self.sentences = sentences
        self.index = 0

    def next (self):
        """Return the next sentence."""
        if self.index >= len(self.sentences):
            raise StopIteration
        result = CrfSentence(self.sentences[self.index])
        self.index += 1
        return result        

class CrfSentencesFromJsonFile:
    """Load a Web scrapings file in JSON format. The entire file is read during initialization."""
    def __init__ (self, jsonFilename):
        with open(jsonFilename) as jsonFile:
            self.sentences = json.load(jsonFile)

    def __iter__(self):
        """Return an iterator over the sentences."""
        return CrfSentencesIterator(self.sentences)


# This could perhaps be improved by making it a generator.
class CrfSentencesFromKeyedJsonLinesFile:
    """Load a Web scrapings file in keyed JSON Lines format. The file is read as needed for iteration."""
    def __init__ (self, jsonFilename):
        self.jsonFile = open(jsonFilename)

    def __iter__ (self):
        """Begin iterating over the contents of the file."""
        return self;

    def next (self):
        """Return the next sentence from the file."""
        if self.jsonFile == None:
            # We'll end up here if next() is called past the point that we
            # returned StopIteration.  We might end up here if someone attempts to
            # iterate more than once, which is not allowed since we read the input
            # file as we iterate.
            raise StopIteration

        line = next(self.jsonFile, None)
        if line == None:
            # End of file.
            self.jsonFile.close()
            self.jsonFile = None
            raise StopIteration
            
        # Split the line into the key and JSON data.  Parse the JSON data and return a CrfSentence.
        key, jsonData = line.split('\t', 1)
        return CrfSentence(json.loads(jsonData), key)
