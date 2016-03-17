import re
import string
from sys import stdin,stdout,stderr
import inspect
from argparse import ArgumentParser
import codecs

# Constant that is used to denote null aka missing aka empty feature value

EMPTY = "_NULL_"

# Other stuff

VOWELS           = set("a e i o u A E I O U".split())

# Features inspired by:
#  - Zhang and Johnson (2003) [http://stat.rutgers.edu/home/tzhang/papers/conll03-rrm.pdf]
#  - Ratinov and Roth (2009) [http://l2r.cs.uiuc.edu/~danr/Papers/RatinovRo09.pdf]

########################################################################################################################################

class FeatListEntry(object):
    """Comprises a U (unigram) or B (bigram) type indicator, a window, and a list of FeatRefs."""
    def __init__ (self):
        self.type      = "U"
        self.featRefs  = []
        self.positions = None
        self.bow       = False
   
class FeatRef(object):
    """Represents a reference to a feature in the feature list file.  Is just the feature name and its relative position."""    
    def __init__ (self,feat,pos=0):
        self.feat = feat
        self.pos  = pos

class FeatDefinition(object):
    """Represents the information needed to extract the feature"""
    def __init__ (self,name):
        self.name         = name  # The string name.
        self.tokenFunc    = None  # All definitions will have one, unless they are sequence-oriented.
        self.sequenceFunc = None  # Every definition will have one, constructed from tokenFunc if need be
        self.isSequence   = False # A sequential feature will have only a sequenceFunc

def getPrefixSuffixTokenFunc (prefixSuffix,tokenFunc,n):
    "Takes a prefix or suffix indicator, an existing token func, and an integer."
    if (prefixSuffix == "prefix"):
        return lambda token :  prefix(getFeatVal(tokenFunc,token),n)
    elif (prefixSuffix == "suffix"):
        return lambda token :  suffix(getFeatVal(tokenFunc,token),n)
    else:
        raise RuntimeError(format("Value given to prefixSuffix arg is neither 'prefix' nor 'suffix': %s" % prefixSuffix))

def makeCVDTranslation ():
    """Makes a character translation for the CVD feature"""
    source = ""
    target = ""
    source += "aeiou"
    target += "v" * 5
    source += "AEIOU"
    target += "V" * 5
    source += "bcdfghjklmnpqrstvwxyz"
    target += "c" * 21
    source += "BCDFGHJKLMNPQRSTVWXYZ"
    target += "C" * 21
    source += "0123456789"
    target += "D" * 10
    # return string.maketrans(source,target)
    charMap = dict()
    for i in range(0,len(source)):
        s = ord(source[i])
        t = ord(target[i])
        charMap[s] = t
    return charMap
cvdTranslation=makeCVDTranslation()
    
def makeShapeTranslation ():
    """Makes a character translation for the 'shape' feature, which maps upper/lowercase letters to X/x and digits to 'd'."""
    source = ""
    target = ""
    source += "aeiou"
    source += "bcdfghjklmnpqrstvwxyz"
    target += "x" * 26
    source += "AEIOU"
    source += "BCDFGHJKLMNPQRSTVWXYZ"
    target += "X" * 26
    source += "0123456789"
    target += "d" * 10
    # return string.maketrans(source,target)    
    charMap = dict()
    for i in range(0,len(source)):
        s = ord(source[i])
        t = ord(target[i])
        charMap[s] = t
    return charMap
shapeTranslation=makeShapeTranslation()
    
def isFunction (tok):
    """Takes a string; returns true if the string is the name of a function."""
    bindings = globals().get(tok)
    return (bindings != None and inspect.isroutine(bindings))

def upcase (tok):
    """Takes a token; returns its uppercase version.""" 
    return tok.upper()

def downcase (tok):
    """Takes a token; returns its lowercase version.""" 
    return tok.lower()

def prefix (s, n):
    "Returns the n-element prefix of s, or EMPTY if s is not long enough."
    length = len(s)
    if (length >= n):
        return s[0:n]
    else:
        return EMPTY

def suffix (s, n):
    "Returns the n-element suffix of s, or EMPTY if s is not long enough"
    length = len(s)
    if (length >= n):
        return s[length-n:length]
    else:
        return EMPTY    


def translateViaDict(s, d):
    r = []
    for c in s:
        t = d.get(ord(c), None)
        if t:
            r.append(unichr(t))
        else:
            r.append(unichr(ord(c)))
    return u"".join(r)

def cvd (tok):
    """Maps upper/lowercase vowels to V/v, upper/lowercase consonants to C/c, and digits to D"""
    # result = tok.translate(cvdTranslation)
    result = translateViaDict(tok, cvdTranslation)
    return result

def shape (tok):
    # result = tok.translate(shapeTranslation)
    result = translateViaDict(tok, shapeTranslation)
    return result


def hasNonInitialPeriod (tok):
    """Examples: St., I.B.M. """         
    # Clojure original: 
    #   #(some? (re-matches #"(\p{L}+\.)+(\p{L}+)?" %)
    if (re.search('^([A-Za-z]+\.)+([A-Za-z]+)?$',tok)):
        return True
    else:
        return False

def hasNoVowels (tok):
    """Returns true if there are no vowels in the argument token."""
    for c in tok:
        if (c in VOWELS):
            return False
    return True

def isAllCapitalized (tok):
    "Returns true if token consists solely of capital letters"
    for sub in tok:
        if (not sub.isalpha() or sub.islower()):
            return False
    return True

def hasCapLettersOnly (tok):
    """Returns true if token has at least one capital letter, and no lower case letters.  
       Can also contain digits, hypens, etc."""
    hasCap = False
    for sub in tok:
        if (sub.isalpha()):
            if (sub.islower()):
                return False
            else:
                hasCap = True
    return hasCap

def hasMixedChars (tok):
    hasLetters = False
    hasDigits  = False
    hasOther  = False
    for c in tok:
        if (c.isalpha()):
            hasLetters = True
        elif (c.isdigit()):
            hasDigits = True
        else:
            hasOther = True
    count = 0
    if (hasLetters):
        count += 1
    if (hasDigits):
        count += 1
    if (hasOther):
        count += 1
    return (count > 1)


def hasInternalPunctuation (tok):
    "A word with an internal apostrophe or ampersand.  Example: O'Connor"
    if (re.search('^[A-Za-z]+[\'&][A-Za-z]+$',tok)):
        return True
    else:
        return False
    
def hasInternalHyphen (tok):
    if (re.search('^[A-Za-z]+(-[A-Za-z]+)+$',tok)):
        return True
    else:
        return False

def isAllNonLetters (tok):
    "Returns true if the token consists only of non-alphabetic characters" 
    for sub in tok:
        if (sub.isalpha()):
            return False
    return True

def isMixedCase (tok):
    """Returns true if the token contains at least one upper-case letter and another character type,
       which may be either a lower-case letter or a non-letter.  Examples: ProSys, eBay """
    # Clojure original:
    #     (fn [token]    ;; ProSys, eBay
    #        (if-not (empty? token)
    #              (->> (map #(Character/isUpperCase %) token)
    #                   distinct
    #                   count
    #                   (= 2))
    #              false))
    hasUpper = False
    hasLower = False
    for c in tok:
        if (c.isupper()):
            hasUpper = True
        else:
            hasLower = True
    return hasUpper and hasLower

def nonAlphaChars (tok):
    """Returns a version of the token where alphabetic characters have been removed, 
       and digits have been replaced by 'D'. This may be the empty string of course. """
    result = ""
    for sub in tok:
        if (sub.isdigit()):
            result += "D"
        elif (not sub.isalpha()):
            result += sub
    return result

def compressedCVD (tok):
    compressed = ""
    cvdVal = cvd(tok)
    prev = ""
    for i in range(0,(len(cvdVal))):
        symbol = cvdVal[i].upper()
        if (symbol != prev):
            compressed += symbol
        prev = symbol
    return compressed


def isWordWithDigit (tok):
    "Examples: W3C, 3M"
    if (re.search('^[A-Za-z\d]*([A-Za-z]\d|\d[A-Za-z])[A-Za-z\d]*$',tok)):
        return True
    else:
        return False


def getFeatVal (featFun, token):
    """Applies a feature function to the token, converting None or empty string values to EMPTY, and 
       True or False values to strings 'true' or 'false', resp."""
    value = apply(featFun,[token])
    if (value == None or value == ""):
        return EMPTY
    elif (value == True):
        return "true"
    elif (value == False):
        return "false"
    else:
        return value    
    
def printf (msg, *msgArgs):
    print(format(msg % msgArgs))
    
def printstream (stream,msg, *msgArgs):
    stream.write(format(msg % msgArgs))
    
def printerr (msg, *msgArgs):
    printstream(stderr,msg,msgArgs)

def nonOverlapping (files1, files2):
    """Takes two lists of files; raises an exception if they overlap."""
    for file1 in files1:
        for file2 in files2:
            if (file1 != None and file2 != None and file1 == file2):
                raise RuntimeError(format("Can't overwrite %s" % file1)) 

def hasXorZ (tok):
    return ("X" in tok or "Z" in tok)
            
def join (lst,sep=" "):
    """Takes a list of objects of arbitrary type, plus an optional separator string.  Returns a string in which the 'str' 
     representations of the objects are joined by the separator, whose default value is just a single space."""
    strg = ""
    for i,x in enumerate(lst):
        if (i > 0):
            strg += sep
        strg += str(x)
    return strg

 
def split (strg,sep=" "):
    tokens = []
    for tok in strg.split(sep):
        if (tok != ""):
            tokens.append(tok)
    return tokens
      

def containsSlash (tok):
    "Returns true if the token contains a '/'."
    for c in tok:
        if (c == '/'):
            return True
    return False

def uniqueChars (tok):
    charSet = set()
    charList = []
    for c in tok:
        if (c not in charSet):
            charSet.add(c)
            charList.append(c)
    return join(charList,"")

def sortChars (tok):
    chars = []
    for c in tok:
        chars.append(c)
    chars.sort()
    return join(chars,"")

def stripVowels (tok):
    "Returns a version of the token from which all vowels have been removed. "
    nonVowels = []
    for c in tok:
        if (c not in VOWELS):
            nonVowels.append(c)
    result = join(nonVowels,"")
    if (result == ""):
        result = EMPTY
    return result


def composeTokenFunctions (func1,func2):
    """Takes two token functions; returns the token function defined as func2(func1(x)). """
    return lambda token : func2(getFeatVal(func1,token))

def getPhraseIndexFeatValues (tokens,phraseIndex):
    """Feature looks at the whole utterance and returns a vector of true/false feature values, one feature value per word"""
    featValues = [None] * len(tokens)
    for i in range(0,len(tokens)):
        phrase = getMatchingPhrase(tokens,i,phraseIndex)
        if (phrase):
            for j in range(0,len(phrase)):
                featValues[i+j] = "true"
    for i,val in enumerate(featValues):
        if (val == None):
            featValues[i] = "false"
    return featValues
    
def getMatchingPhrase (tokens,idx,phraseIndex):
    """Returns the matching phrase from phraseIndex at position idx in tokens. Returns None if there is no match."""
    token     = tokens[idx].lower()
    phrases   = phraseIndex.get(token)
    if (phrases):
        remaining = len(tokens) - idx
        for phrase in phrases:
            if (remaining >= len(phrase) and matches(tokens,idx,phrase)):
                return phrase
    return None

def matches (tokens,idx,phrase):
    """Returns True if phrase is present at position idx in tokens"""
    for i,word in enumerate(phrase):
        if (tokens[idx+i].lower() != word):
            return False
    return True

def wordSetToTokenFunc (wordSet):
    """Takes a set of words and returns a token-level function that returns true if the 
       token is in that set of words."""
    return lambda token : token.lower() in wordSet

def phraseIndexToSequenceFunc (phraseIndex):
    return lambda tokens : getPhraseIndexFeatValues(tokens,phraseIndex)

def tokenFuncToSequenceFunc (tokenFunc):
    """Takes a function that takes a single token as argument, and and lifts it to a function 
       that takes a sequence of tokens as argument.""" 
    return lambda tokens : [getFeatVal(tokenFunc,token) for token in tokens]

class CrfFeatures(object):
    def __init__(self, featListFile, featDefsFile=None, outputFile=None, templateFile=None, 
                 labeled=False, verbose=False, monocase=False):
        # globals()['input'] = input
        self.featListFile = featListFile
        self.featDefsFile = featDefsFile
        self.outputFile = outputFile
        self.templateFile = templateFile
        self.labeled = labeled
        self.verbose = verbose
        self.monocase = monocase

        # Mapping from feature names to FeatDefinition objects.
        self.featureNamesToDefinitions = {}

        # The list of specific features and their associated functions 
        # that will be used in the current run of the program.
        self.featureEntries         = []  # Entries in the feature list file.
        self.featureNamesUsed       = []  # Column names of the feature matrix.
        self.featureDefinitionsUsed = []  # Corresponding feature definitions of those columns.

        # Initialize whatever script variables have to be initialized
        # initializeScriptData()
        # Define the script's built-in features
        self.defineFeatures()   
        # Read any additional feature definitions that may have been specified
        if (self.featDefsFile):
            self.readExtraFeatDefsFile(self.featDefsFile)
        # Read the list of feature entries we will be working with
        self.readFeatureListFile(self.featListFile)
        # Print them out
        if (self.verbose):
            self.printFeatsUsed()

    ## Methods of CRFF in general order of original file
    ## NOT PORTED YET
    def writeFeatMatrixFile (self,input,outputFile,labeled,addLabels=None):
        """Featurizes input, writing the result to outputFile.  If 'labeled' is True, lines in the input must have a label. """
        reqFields = None
        instream  = codecs.open(input,"r",encoding='utf-8') if input != None else stdin 
        outstream = codecs.open(outputFile,"wb",encoding='utf-8') if outputFile != None else stdout
        tokens  = []
        labels = []
        addLabels = addLabels or []
        while (True):
            line = instream.readline()
            if (not line):
                break
            line = line.strip()
            if (line == ""):
                featuresPerWord = self.featurizeSentence(tokens)
                for i in range(0,len(tokens)):
                    outfields = [tokens[i]]
                    outfields.extend(featuresPerWord[i])
                    outfields.extend(labels[i])
                    outfields.extend(addLabels)
                    outstream.write("%s\n" % "\t".join(outfields))
                outstream.write("\n")
                tokens = []
                labels = []
            else:
                # Extract fields; check validity and consistency of line format
                fields  = line.split("\t")
                nfields = len(fields)
                if (reqFields == None):
                    if (labeled and nfields < 2):
                        raise RuntimeError("Label is required but missing")
                    reqFields = nfields
                elif (nfields != reqFields):
                    raise RuntimeError(format("Inconsistent number of fields: current line is %d vs. previous lines %d. In:\n%s" % (nfields,reqFields,line)))
                token = fields[0]
                if (monocase):
                    token = token.lower()
                tokens.append(token)
                labels.append(fields[1:])
        if (tokens): # I could take care of this but I won't. Discipline. ;)
            raise RuntimeError("Input file did not end with an empty line as required")
        if (input != None):
            instream.close()     
        if (outputFile != None):
            outstream.close()

    def computeFeatMatrix (self,inputVector,labeled,addLabels=None,addIndex=False):
        """Featurizes inputVector, returning the result as a vector.  If 'labeled' is True, 
lines in the inputFile must have a label. addLabels will be added to each row
addIndex means add another label which is the index of the token within the sentence"""
        ## input vector should be
        ## labeled with URI:[["w1", "uri1"], ["w2", "url2"], ..., ""]
        ## or unlabeled:    [["w1"], ["w2"], ..., "", ..., ""]
        reqFields = None
        tokens  = []
        labels = []
        outputVector = []
        addLabels = addLabels or []
        idx = 1
        while (inputVector):
            line = inputVector.pop(0)
            if (line == ""):
                featuresPerWord = self.featurizeSentence(tokens)
                for i in range(0,len(tokens)):
                    outfields = [tokens[i]]
                    outfields.extend(featuresPerWord[i])
                    outfields.extend(labels[i])
                    outfields.extend(addLabels)
                    if idx:
                        outfields.append(str(idx))
                        idx += 1
                    outputVector.append(outfields)
                tokens = []
                labels = []
            else:
                # Extract fields; check validity and consistency of line format
                # Vector input is pre-split
                fields = line
                nfields = len(fields)
                if (reqFields == None):
                    if (labeled and nfields < 2):
                        raise RuntimeError("Label is required but missing")
                    reqFields = nfields
                elif (nfields != reqFields):
                    raise RuntimeError(format("Inconsistent number of fields: current line is %d vs. previous lines %d. In:\n%s" % (nfields,reqFields,line)))
                token = fields[0]
                if (self.monocase):
                    token = token.lower()
                tokens.append(token)
                labels.append(fields[1:])
        if (tokens): # I could take care of this but I won't. Discipline. ;)
            raise RuntimeError("Input did not end with an empty line as required: %s" % tokens)
        outputVector.append("")
        return outputVector

    def featurizeSentence (self,tokens):
        """Takes a list of tokens, and returns a corresponding list of feature values"""
        columnsPerFeature = []
        for featDef in self.featureDefinitionsUsed:
            column = featDef.sequenceFunc(tokens)
            for i,val in enumerate(column):
                if (val == None):
                    column[i] = EMPTY
            columnsPerFeature.append(column)
        rowsPerToken = []
        for i in range(0,len(tokens)):
            row = []
            for column in columnsPerFeature:
                row.append(column[i])
            rowsPerToken.append(row)
        return rowsPerToken

    def readExtraFeatDefsFile (self,filename):
        stderr.write("Reading additional feature defs from %s\n" % filename)
        execfile(filename,globals(),locals())

    def executeOptionsDirective (self, string):
        string = string.lower()
        string = re.sub(r'^options:','',string)
        tokens = string.split()
        for token in tokens:
            if (token == "monocase"):
                # print "\nSetting monocase to True"
                self.monocase = True
            else:
                raise RuntimeError(format("Unknown token in params: command: %s" % token))

    def executeDefWordList (self, string):
        """Executes a feature definition that defines the feature by membership in a set of words 
           specified by a comma-delimited list of files"""
        tokens    = string.split()
        featname  = tokens[1]
        filenames = tokens[2]
        wordSet   = readWordSetFromFiles(filenames.split(","))
        tokenFunc = wordSetToTokenFunc(wordSet)
        self.defFeat(featname,tokenFunc)

    def executeDefPhraseList (self, string):
        """Executes a feature definition that defines the feature by whole-phrase match in a phrase
           list specified by a comma-delimited list of files"""
        tokens    = string.split()
        featname  = tokens[1]
        filenames = tokens[2]
        index     = readPhraseIndexFromFiles(filenames.split(","))
        seqFunc   = phraseIndexToSequenceFunc(index)
        self.defFeat(featname,seqFunc,isSeq=True)

    def defFeat (self,name,func,isSeq=False):
        """Defines a feature in terms of a name and a value-extraction function.  By default, the function is interpreted as 
           operating on tokens, and returning a single feature value for them.  If isSeq is true, the function is interpreted 
           as operating on an entire word sequence and returning a entire sequence of values of the same length. """     
        assert(name)
        assert(func)
        if (" " in name or "\t" in name):  # Probably this would never happen, but may as well prevent it!
            raise RuntimeError(format("Can't have whitespace in feature name: '%s'" % name))
        if (self.featureNamesToDefinitions.get(name)):
            stderr.write("\n\nREDEFINING FEATURE: %s\n\n" % name)
        featDef            = FeatDefinition(name)
        featDef.isSequence = isSeq
        if (featDef.isSequence):
            featDef.tokenFunc    = None
            featDef.sequenceFunc = func
        else:
            featDef.tokenFunc    = func
            featDef.sequenceFunc = tokenFuncToSequenceFunc(func)
        # Don't forget to set the featname's definition in the map
        self.featureNamesToDefinitions[name] = featDef
        return featDef

    def readFeatureListFile (self,filename):
        """Reads the features to be used, one feature entry per line.  Lines starting with '#' are treated as comments and ignored. Feature entries
           may be simple, consisting of just a single feature reference, or compound, consisting of multiple feature references separated by '/'s. """
        with open(filename,"r") as instream:
            for line in instream:
                line = line.strip()
                # Ignore blank lines or lines starting with a '#'.
                if (line == "" or line.startswith("#")):  
                    pass
                # Special forms like 'defwordlist' of 'defphraselist'
                elif (line.lower().startswith("defwordlist")):
                    self.executeDefWordList(line)
                elif (line.lower().startswith("defphraselist")):
                    self.executeDefPhraseList(line)
                elif (line.lower().startswith("options:")):
                    self.executeOptionsDirective(line)
                # Otherwise treat as regular feature entry
                else:
                    self.featureEntries.append(self.parseFeatureListEntry(line))    
        # Get the set of feature names for which we have a simple (non-compound) entry.
        singleFeats = set() 
        for entry in self.featureEntries:
            if (len(entry.featRefs) == 1):
                singleFeats.add(entry.featRefs[0].feat)
        # For compound entries, add simple feature entries for component features not explicitly provided.
        featsToAdd = []
        for entry in self.featureEntries:
            for featRef in entry.featRefs:
                featname = featRef.feat
                if (featname not in singleFeats and featname not in featsToAdd):
                    featsToAdd.append(featname)
        # Just treat feature entries to be added as strings to be parsed 
        for featname in featsToAdd:
            self.featureEntries.append(self.parseFeatureListEntry(featname))
        # Set up the lists of single-feature names and defintions that are being used in this run of the feature extractor. 
        # This gives the semantics of the columns in the feature matrix file. 
        for entry in self.featureEntries:        
            if (len(entry.featRefs) == 1):
                featName = entry.featRefs[0].feat
                self.featureNamesUsed.append(featName)
                self.featureDefinitionsUsed.append(self.getFeatDefinitionOrError(featName))

    def parseFeatureListEntry (self,entryString):
        """Parses a line from the feature list file, and returns the corresponding FeatListEntry object."""
        entry       = FeatListEntry()    
        tokens      = entryString.split()
        refString   = tokens[0]
        quantTokens = tokens[1:]
        # The entry is U: or B: followed by feature names
        prefixUorB  = re.search(r"^(U|B):(\S*)",refString)
        if (prefixUorB):
            entry.type = prefixUorB.group(1)
            refString  = prefixUorB.group(2)
        # The entry is just U or B on its own
        elif (refString == "B" or refString == "U"):
            entry.type = refString
            refString  = ""      
        # Parse each FeatRef in the entry. If there is more than one, they are separated by '/'. 
        for featRef in split(refString,"/"):
            entry.featRefs.append(self.parseFeatRef(featRef))
        # Parse the quantifier string, which specifies which positions the entry will apply to
        self.parseQuantifierString(entry,join(quantTokens))    
        return entry    

    def parseQuantifierString (self,featListEntry,quantString):
        """Takes a FeatListEntry and the 'quantifier' portion of the feat list entry string. This string specifies the set of word positions the entry 
           is to be applied to, and whether they are to be treated bag-of-words are not.  An empty quantifier string is implicitly position 0 only. 
           Multiple position specifications are allowed, and combined via set union."""
        unparsed  = quantString.strip()
        positions = set()
        while (unparsed != ""):
            # Window:  +- 2    
            plusMinus = re.match(r'\+-\s*(\d+)',unparsed)
            # Range: -2...2 or -1..2
            ellipsis  = re.match(r'([+-]?\d+)\s?\.\.\.?\s?([+-]?\d+)',unparsed)       
            # Comma-delimited list of positions: -1,1,+2
            commas = re.match(r'([+-]?\d+)((,[+-]?\d+)*)',unparsed)
            # Bag-of-words spec:  -bow
            bow       = re.match(r'\-bow',unparsed)
            if (plusMinus):
                window = int(plusMinus.group(1))
                end    = plusMinus.end()
                positions.update(range(-1*window,window+1))            
                unparsed = unparsed[end:].strip()
            elif (ellipsis):            
                first = int(ellipsis.group(1))
                last  = int(ellipsis.group(2))
                end   = ellipsis.end()
                if (first > last):
                    raise RuntimeError("Last %d is before first %d in this quantifier quantString: %s" % (last,first,quantString))
                positions.update(range(first,last+1))
                unparsed = unparsed[end:].strip()
            elif (bow):
                end = bow.end()
                featListEntry.bow = True
                unparsed = unparsed[end:].strip()
            elif (commas):
                pos1   = commas.group(1)
                others = split(commas.group(2),",")
                positions.add(int(pos1))
                for other in others:
                    positions.add(int(other))
                end = commas.end()
                unparsed = unparsed[end:].strip()
            else:
                raise RuntimeError(format("Can't parse '%s' in %s" % (unparsed,quantString)))
        if (not positions):
            positions.add(0)
        featListEntry.positions = list(positions) 
        featListEntry.positions.sort()            

    def parseFeatRef (self,featRefString):
        """Parses a single feature reference like 'cvd' or 'cvd-1' into a FeatRef object"""
        assert(featRefString)
        if (self.isDefinedFeat(featRefString)):
            return FeatRef(featRefString)
        else:
            # Check whether it has a +n or -n relative position indicator
            relPos = re.search(r"^(\S+)([+-]\d+)$",featRefString)
            if (relPos):
                featname = relPos.group(1)
                pos      = int(relPos.group(2))      
                self.getFeatDefinitionOrError(featname)
                return FeatRef(featname,pos)       
            else:
                self.getFeatDefinitionOrError(featRefString)
                return FeatRef(featRefString)

    ## NOT PORTED/TESTED YET
    def readWordSetFromFiles (self,filenames):
        """Takes a list of files; reads a word set from them. Words are lower-cased. Lines containing multiple words 
           are split on whitespace. Ignores lines starting with '#'. """
        words    = set()
        for filename in filenames:
            with open(filename,"r") as instream:
                for line in instream:
                    line = line.strip()
                    line = line.lower()
                    if (line != "" and not line.startswith("#")):
                        for word in line.split():
                            words.add(word)
        return words

    def isDefinedFeat (self,string):
        """Returns true if the string is the name of an existing defined feature."""
        return self.featureNamesToDefinitions.get(string) != None

    ## NOT PORTED YET
    def writeTemplateFile (filename):
        "Writes out the template definitions in the index-addressed format that CRF++ uses."
        # We split up unigram and bigram features, and write their template entries separately just for clarity's sake.
        unigrams = []
        bigrams  = []
        for entry in self.featureEntries:
            if (entry.type == "B"):
                bigrams.append(entry)
            else:
                unigrams.append(entry)
        outstream = open(filename,"wb")
        writeTemplatesForFeatEntries(unigrams,outstream)
        # We typically would not expect a bigram feature except for "B" itself, but they are allowed w/o prejudice.
        if (bigrams):
            outstream.write("\n")
            writeTemplatesForFeatEntries(bigrams,outstream)
        outstream.close()

    ### NOT PORTED YET
    def writeTemplatesForFeatEntries (self,entries,outstream):
        "Writes a list of FeatListEntry objects to a stream, leaving the stream open when it is done"
        idx = 0
        for entry in entries:
            # An entry containing no feature references is just a reference to a tag unigram or tag bigram, and 
            # produces 'U' or 'B' on its own line 
            if (len(entry.featRefs) == 0):
                outstream.write("%s\n" % entry.type)
            # Otherwise, we write out the entries with ids that serve to distinguish them from one another. These ids
            # are just strings whose content doesn't matter, so long as they are distinguishing. 
            else:
                # The main part of the id is just the type U or B, together with a zero-padded index
                entryId = entry.type + format("%02d" % idx)
                for pos in entry.positions:
                    outstream.write(entryId)
                    # Add a +n or -n to distinguish different values of pos, unless this entry is bag-of-words
                    if (not entry.bow):
                        outstream.write("+" if pos >= 0 else "")
                        outstream.write(str(pos))
                    outstream.write(":")
                    for f,ref in enumerate(entry.featRefs):
                        # The column index is i+1, since index 0 in feature matrix rows is by convention the token itself. We don't have
                        # to write the token out, but we do for clarity. If the token is used as a feat itself, it will simply appear twice.
                        col = self.featureNamesUsed.index(ref.feat) + 1
                        row = ref.pos + pos                   
                        if (f > 0):
                            outstream.write("/")
                        # Each indexed feature reference is of the form %x[i,j].  During CRF++ internal feature expansion, 
                        # these strings get replaced with the corresponding string value in the feature matrix.
                        outstream.write("%x" + format("[%d,%d]" % (row,col)))
                    outstream.write("\n")
                idx += 1

    def printFeatsUsed (self):
        """Prints out the feature names which define the columns of the feature matrix."""
        stderr.write("\nColumns of feature matrix:\n\n")
        for i,feat in enumerate(self.featureNamesUsed):
            stderr.write("%-2d  %s\n" % (i+1,feat))

    def getFeatFunc (self,feat):
        "Returns the definition function for the feature"
        return self.getFeatDefinitionOrError(feat).tokenFunc

    def getFeatDefinitionOrError (self,featname):
        """Returns the stored definition for feature, constructing a definition via function composition
           if the name it contains '.'.  Error if there is no feature, or none can be constructed."""
        entry = self.featureNamesToDefinitions.get(featname)
        if (entry):
            return entry
        elif ("." in featname):
            return self.defineFeatureUsingFunctionComposition(featname)
        else:
            raise RuntimeError(format("Undefined feature: '%s'" % featname))

    def defineFeatureUsingFunctionComposition (self,featstring):
        """Takes a string like containing ".", which indicates function composition.  The first '.'-separated token is an
           existing feature, to whose values the functions represented by subsequent elements are successively applied.
           Example: 'cvd.upper.prefix3', which starts with 'cvd' as base feature, and takes the upper case version, and then 
           3-character prefix."""
        tokens = featstring.split(".")
        underlyingFeat = tokens[0]
        underlyingDef  = self.getFeatDefinitionOrError(underlyingFeat)
        tokenFunc      = underlyingDef.tokenFunc
        for i in range(1,len(tokens)):
            token = tokens[i]
            # If it is a form like 'prefix2' or 'suffix4'..
            affixMatch = re.search(r'^(prefix|suffix)(\d+)$',token)
            if (affixMatch):
                prefixSuffix = affixMatch.group(1)
                n            = int(affixMatch.group(2))
                tokenFunc = getPrefixSuffixTokenFunc(prefixSuffix,tokenFunc,n)       
            elif (token == "unique"):
                tokenFunc = composeTokenFunctions(tokenFunc,uniqueChars)
            elif (token == "sort"):
                tokenFunc = composeTokenFunctions(tokenFunc,sortChars)
            elif (isFunction(token)):
                func      = globals()[token]
                tokenFunc = composeTokenFunctions(tokenFunc,func)
            else:
                raise RuntimeError(format("Can't handle this token: %s" % token))
        return self.defFeat(featstring,tokenFunc)

    def defineFeatures (self):
        """Defines the built-in features for this package"""    

        self.defFeat('token', lambda(x) : x)

        self.defFeat('shape', shape)
        self.defFeat('has-cap-letters-only', hasCapLettersOnly)
        self.defFeat('mixed-chars', hasMixedChars)
        self.defFeat('word-with-digit', isWordWithDigit)
        self.defFeat('upper-token', lambda (x) : x.upper())
        self.defFeat('mixed-case', isMixedCase)

        self.defFeat('non-initial-period', hasNonInitialPeriod)
        self.defFeat('internal-hyphen', hasInternalHyphen)
        self.defFeat('all-digits', lambda (x) : x.isdigit())

        self.defFeat('has-no-vowels', hasNoVowels)

        self.defFeat('all-capitalized', isAllCapitalized)
        self.defFeat('all-non-letters', isAllNonLetters)
        self.defFeat('initial-capitalized', lambda (x) : x[0].isupper())
        self.defFeat('internal-punctuation', hasInternalPunctuation)
        self.defFeat('non-alpha-chars', nonAlphaChars)

        self.defFeat('prefix3', lambda (x) : prefix(x,3))
        self.defFeat('prefix4', lambda (x) : prefix(x,4))

        self.defFeat('suffix4', lambda (x) : suffix(x,4))
        self.defFeat('suffix2', lambda (x) : suffix(x,2))
        self.defFeat('suffix3', lambda (x) : suffix(x,3))
        self.defFeat('suffix1', lambda (x) : suffix(x,1))

        self.defFeat('cvd', cvd)
        self.defFeat('compressed-cvd', compressedCVD)

        self.defFeat('ends-with-digit', lambda (x) : (not x[0].isdigit()) and x[-1].isdigit())
        self.defFeat('has-X-or-Z', hasXorZ)
        self.defFeat('contains-slash', containsSlash)

        self.defFeat('constant', lambda (x) : "CONST")
        self.defFeat('unique-chars', uniqueChars)
        self.defFeat('strip-vowels', stripVowels)

# TODO: Do we still need access outside of the command line?
# def OLDextractFeatures (input, outputFile, featListFile,templateFile=None,labeled=False,featDefsFile=None):
#     """The top-level function that does the work.  This is useful for being called by other scripts in a 
#        programmatic context where command-line argument parsing can be dispensed with."""
#     # Read any additional feature definitions that may have been specified
#     if (featDefsFile):
#         readExtraFeatDefsFile(featDefsFile)
#     # Get the features we will be working with
#     readFeatureListFile(featListFile)
#     # Print them out
#     printFeatsUsed()
#     # Featurize the file.            
#     writeFeatMatrixFile(input,outputFile,labeled)
#     # Write the template file if one was provided.
#     if (templateFile):
#         writeTemplateFile(templateFile)

    ## NOT PORTED YET
    def readPhraseIndexFromFiles (filenames):
        """Takes a list of files; reads phrases from them, with one phrase per line, ignoring blank lines 
           and lines starting with '#'. Returns a map from words to the list of phrases they are the first word of."""
        phraseIndex = dict()
        for filename in filenames:
            with open(filename,"r") as instream:
                for line in instream:
                    line = line.strip()
                    if (line != "" and not line.startswith("#")):
                        line   = line.lower()
                        phrase = line.split()
                        firstWord  = phrase[0]
                        phraseList = phraseIndex.get(firstWord)
                        if (phraseList == None):
                            phraseList = []
                            phraseIndex[firstWord] = phraseList
                        phraseList.append(phrase)
        # Sort each list of phrases in decreasing order of phrase length
        for phrases in phraseIndex.values():
            phrases.sort(key=lambda p: len(p),reverse=True)
        return phraseIndex

SampleVectorInput=[[u"brown", u"http://dig.isi.edu/url2"],
                   [u"eyes", u"http://dig.isi.edu/url2"],
                   [u"and", u"http://dig.isi.edu/url2"],
                   [u"blonde", u"http://dig.isi.edu/url2"],
                   [u"hair", u"http://dig.isi.edu/url2"],
                   u""
                   ]

# Call the 'main' function if we are being invoke in a script context. 
if (__name__ == "__main__"):
    scriptArgs = ArgumentParser(description="Extracts features for input to CRF++'s crf_learn and crf_test executables")

    scriptArgs.add_argument('--input',help="Optional input file, with one token per line. Additional tab-separated fields, e.g. a label, may follow. Reads from stdin if no argument provided.")
    scriptArgs.add_argument('--output',help="Optional output file with lines of the form '<token><tab><feat><tab<feat><tab>...<label>'. Writes to stdout if no argument provided.")
    scriptArgs.add_argument('--featlist',help="Required input file with features to be extracted, one feature entry per line.",required=True)
    scriptArgs.add_argument('--templates',help="Optional output file containing feature template definitions needed by crf_learn")
    scriptArgs.add_argument('--labeled',action='store_true',help="Require input lines to have a label as well as a token.")
    scriptArgs.add_argument('--monocase',action='store_true',help="Convert all input tokens to lower case before feature extraction.")
    scriptArgs.add_argument('--verbose',action='store_true',help="Print out extra information about the feature extraction.")
    scriptArgs.add_argument('--extrafeatdefs',help="File of additional 'defFeat' feature definitions to use.")

    argValues = vars(scriptArgs.parse_args())

    # Command line arguments
    input        = argValues["input"]
    outputFile   = argValues["output"]
    featListFile = argValues["featlist"]
    templateFile = argValues["templates"]
    labeled      = argValues["labeled"]
    verbose      = argValues["verbose"]
    monocase     = argValues["monocase"]
    featDefsFile = argValues["extrafeatdefs"]

    ## RUNTIME STARTS HERE

    c = CrfFeatures(featListFile, templateFile=templateFile, 
                    labeled=labeled, verbose=verbose, featDefsFile=featDefsFile)
    if input or outputFile:
        raise RuntimeError("file-based input/output under construction")
    else:
        o = c.computeFeatMatrix(SampleVectorInput, False)
        print o

'''

THIS IS THE ORIGINAL MAIN DAVID S WROTE:
HERE TO PRESERVE INTENDED FULL WORKFLOW

def main ():
    """The function that is called in a command line context."""
    # Make sure we aren't unintentionally overwriting an input file        
    nonOverlapping([featListFile,input,featDefsFile],[outputFile,templateFile])
    # Initialize whatever script variables have to be initialized
    initializeScriptData()
    # Define the script's built-in features
    defineFeatures()   
    # Read any additional feature definitions that may have been specified
    if (featDefsFile):
        readExtraFeatDefsFile(featDefsFile)
    # Read the list of feature entries we will be working with
    readFeatureListFile(featListFile)
    # Print them out
    if (verbose):
        printFeatsUsed()
    # Featurize the input file or input vector.
    if isinstance(input, list):
        v = computeFeatMatrix(input, labeled)
    else:
        writeFeatMatrixFile(input,outputFile,labeled)
    # Write the template file if one was provided.
    if (templateFile):
        writeTemplateFile(templateFile)
    # If not writing a file, return value is vector
    if isinstance(input, list):
        return v

'''
