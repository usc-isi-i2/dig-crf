import string
import sys

class cmrTokenizer:
    """The tokenization rules take into account embedded HTML tags and
entities. HTML tags begin with "<" and end with ">". The contents of a
tag are treated as a single token, although internal spaces, tabs, and
newlines are stripped out so as not to confuse CRF++. HTML entities
begin with "&" and end with ";", with certain characters allowed
inbetween. They are treated as single tokens.

There are risks to the HTML processing rules when the text being
tokenized is not proper HTML.  Left angle brackets can cause the
following text to become a single token.  Ampersands can merge into
the following textual word.

A possible solution to the bare ampersand problem is to recognize only
the defined set of HTML entities. It is harder to think of a solution
to the bare left angle bracket problem; perhaps check if they are
followed by the beginning of a valid HTML tag name?

There is also special provision to group contiguous punctuation characters."""

    whitespaceSet = set(string.whitespace)
    punctuationSet = set(string.punctuation)
    htmlEntityNameCharacterSet = set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                                      'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                                      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '#'])
    START_HTML_TAG_CHAR = "<"
    END_HTML_TAG_CHAR = ">"
    START_HTML_ENTITY_CHAR = "&"
    END_HTML_ENTITY_CHAR = ";"

    def __init__ (self):
        self.groupPunctuation = False
        self.recognizeHtmlTags = False
        self.recognizeHtmlEntities = False
        self.tokenPrefix = None

    def setGroupPunctuation (self, groupPunctuation):
        """When True, group adjacent punctuation characters into a token."""
        self.groupPunctuation = groupPunctuation

    def setRecognizeHtmlTags (self, recognizeHtmlTags):
        """When True, assume that the text being parsed is HTML.  Recognize HTML tags,
        such as "<bold>", and parse them into single tokens (e.g., "<bold>"
        instead of ["<", "bold", ">"]).

        """
        self.recognizeHtmlTags = recognizeHtmlTags

    def setRecognizeHtmlEntities (self, recognizeHtmlEntities):
        """When True, assume that the text being parsed is HTML.  Recognize HTML
        entities, such as "&gt;", and parse them into single tokens (e.g.,
        "&gt;" instead of ["&", "gt", ";"]).

        """
        self.recognizeHtmlEntities = recognizeHtmlEntities

    def setTokenPrefix (self, tokenPrefix):
        """When non None, a string that should be prepended to each token. This may be
        useful when tokens are being generated from different sources, and it
        is desired to be able to distinguish the source of a token.

        """
        self.tokenPrefix = tokenPrefix

    def tokenize (self, value):
        """Take a string and break it into tokens. Return the tokens as a list of
        strings.

        """

        # This code uses a state machine:
        class STATE:
            NORMAL = 0
            GROUP_PUNCTUATION = 1
            PROCESS_HTML_TAG = 2
            PROCESS_HTML_ENTITY = 3

        # "state" and "token" have array values to allow their
        # contents to be modified within finishToken().
        state = [STATE.NORMAL]
        token = [""] # The current token being assembled.
        tokens = [] # The tokens extracted from the input.

        def finishToken():
            """Emit the current token, if any, and return to normal state."""
            if len(token[0]) > 0:
                tokens.append(token[0])
                token[0] = ""
            state[0] = STATE.NORMAL

        def fixBrokenHtmlEntity():
            # This is not a valid HTML entity. Emit the
            # ampersand the began the prospective entity and
            # use the rest as a new current token.  Continue
            # processing with the current character.
            #
            # TODO: embedded "#" characters should be treated better
            # here.
            saveToken = token[0]
            token[0] = saveToken[0:1]
            finishToken()
            if len(saveToken) > 1:
                token[0] = saveToken[1:]

        # Process each character in the input string:
        for c in value:
            if state[0] == STATE.PROCESS_HTML_TAG:
                if c in cmrTokenizer.whitespaceSet:
                    continue # Suppress for safety. CRF++ doesn't like spaces in tokens, for example.
                token[0] += c
                if c == cmrTokenizer.END_HTML_TAG_CHAR:
                    finishToken()
                continue

            if state[0] == STATE.PROCESS_HTML_ENTITY:
                # Parse an HTML entity name. TODO: embedded "#"
                # characters imply more extensive parsing rules should
                # be performed here.
                if c == cmrTokenizer.END_HTML_ENTITY_CHAR:
                    if len(token[0]) == 1:
                        # This is the special case of "&;", which is not a valid HTML entity.
                        if not self.groupPunctuation:
                            finishToken() # Emit the "&" as a seperate token.
                    token[0] = token[0] + c
                    finishToken()
                    continue
                elif c in cmrTokenizer.htmlEntityNameCharacterSet:
                    token[0] = token[0] + c
                    continue
                else:
                    # This is not a valid HTML entity.
                    fixBrokenHtmlEntity()
                    # intentional fall-through

            if c in cmrTokenizer.whitespaceSet:
                # White space terminates the current token, then is dropped.
                finishToken()

            elif c == cmrTokenizer.START_HTML_TAG_CHAR and self.recognizeHtmlTags:
                finishToken()
                state[0] = STATE.PROCESS_HTML_TAG
                token[0] = c

            elif c == cmrTokenizer.START_HTML_ENTITY_CHAR and self.recognizeHtmlEntities:
                finishToken()
                state[0] = STATE.PROCESS_HTML_ENTITY
                token[0] = c

            elif c in cmrTokenizer.punctuationSet:
                if self.groupPunctuation:
                    # Finish any current token.  Concatenate
                    # contiguous punctuation into a single token:
                    if state[0] != STATE.GROUP_PUNCTUATION:
                        finishToken()
                        state[0] = STATE.GROUP_PUNCTUATION
                    token[0] = token[0] + c
                else:
                    # Finish any current token and form a token from
                    # the punctuation character:
                    finishToken()
                    token[0] = c
                    finishToken()

            else:
                # Everything else goes here. Presumably, that includes
                # Unicode characters that aren't ASCII
                # strings. Further work is needed.
                if state[0] != STATE.NORMAL:
                    finishToken()
                token[0] = token[0] + c

        # Finish any final token and return the array of tokens:
        if state[0] == STATE.PROCESS_HTML_ENTITY:
            fixBrokenHtmlEntity()
        finishToken()

        # Was a token prefix requested? If so, we'll apply it now.  If the
        # normal case is not to apply a token prefix, this might be a little
        # more efficient than applying the prefix in finishToken().
        if self.tokenPrefix is not None and len(self.tokenPrefix) > 0:
            tokens = map(lambda x: self.tokenPrefix + x, tokens)

        return tokens            

def main(argv=None):
    '''this is called if run from command line'''

    t = cmrTokenizer()
    print t.tokenize("This is a sentence.")
    print t.tokenize("Buy???This...Now!!!")
    print t.tokenize("The<bold>only</bold>source.")
    print t.tokenize("Big&gt;little.")
    print t.tokenize("Big & little.")
    print t.tokenize("blond&curly.")
    print t.tokenize("&brokenHtml")
    t.setGroupPunctuation(True)
    t.setRecognizeHtmlTags(True)
    t.setRecognizeHtmlEntities(True)
    print t.tokenize("Buy???This...Now!!!")
    print t.tokenize("The<bold>only</bold>source.")
    print t.tokenize("Big&gt;little.")
    print t.tokenize("Big & little.")
    print t.tokenize("blond&curly.")
    print t.tokenize("&brokenHtml")
    t.setTokenPrefix("X:")
    print t.tokenize("Tokenize with prefixes.")
    t.setTokenPrefix(None)
    print t.tokenize("No more  prefixes.")

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())
