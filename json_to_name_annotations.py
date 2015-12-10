import sys
import json
import codecs
from argparse import ArgumentParser
from itertools import count, izip

scriptArgs = ArgumentParser()
scriptArgs.add_argument("--inputs",nargs='+',help="File containing a JSON list of JSON annotation objects")
scriptArgs.add_argument("--output",required=True,help="Output file which will have lines <token><tab><label>, one token/label pair per line")
scriptArgs.add_argument("--iob",action='store_true',help="Add 'B_' and 'I_' prefixes to name labels for IOB annotation, vs. the default IO.")
scriptArgs.add_argument("--nametypes",required=True,help="Entity types to look for, comma-separated")
argValues = vars(scriptArgs.parse_args())

inputFiles  = argValues["inputs"]
outputFile = argValues["output"]
useIOB     = argValues["iob"]

# entityTypes = set(["eyeColor","hairType"])
entityTypes = set(argValues["nametypes"].split(","))

# Not using these right now
excludeTokens = set("&lt;br&gt; &lt;br/&gt; &amp; &amp;#039; &lt;/a&gt;".split())

##############################################################

def main ():
    nonOverlapping(inputFiles,[outputFile])
    outstream = codecs.open(outputFile,"wb","utf-8")
    for inputFile in inputFiles:
        processJSONForms(inputFile,outputFile,outstream)
    outstream.close()

def processJSONForms (inputFile,outputFile,outstream):
    instream  = codecs.open(inputFile,"rb","utf-8")
    forms = json.load(instream)
    assert(type(forms) == list)
    print("%d forms" % len(forms))
    for form in forms:
        assert(type(form) == dict)
        sentTokens = form["allTokens"]
        assert(type(sentTokens) == list)
        for i,token in enumerate(sentTokens):
            if (" " in token or "\t" in token):
                sentTokens[i] = "_BAD_"            
        sentLen    = len(sentTokens)
        assert(sentLen > 0)
        annotSet   = form["annotationSet"]        
        assert(type(annotSet) == dict)
        # Collect up all the entities that were identified for this sentence
        entities = []
        for labelType,annots in annotSet.iteritems():
            if (labelType == "noAnnotations"):
                continue
            if (labelType not in entityTypes):
                raise RuntimeError(format("Not in entity types: %s" % labelType))
            assert(type(annots) == list)
            for annot in annots:
                assert(type(annot) == dict)
                annotTokens = annot["annotatedTokens"]
                assert(type(annotTokens) == list)
                assert(len(annotTokens) > 0)
                start         = int(annot["start"])
                assert(start >= 0 and start < sentLen)
                end           = start + len(annotTokens)-1
                entity        = Entity()
                entity.type   = labelType
                entity.start  = start
                entity.end    = end
                entity.tokens = sentTokens[start:end+1]
                entity.string = " ".join(entity.tokens)
                # assert(entity.tokens == annotTokens)
                entities.append(entity)
        # Generate labels for them and print them out one per line
        labels = generateLabelsForSentence(sentTokens,entities)
        # Don't filter right now 
        # (sentTokens,labels) = filterTokens(sentTokens,labels)
        for i in range(0,len(labels)):
            # outstream.write("%s\t%s\n" % (sentTokens[i].encode("utf-8"),labels[i]))
            # outstream.write("%s\t%s\n" % (sentTokens[i],labels[i]))
            token = fixToken(sentTokens[i])
            if token != sentTokens[i]:
                print >> sys.stderr, ("Making edit in form {}".format(form))
            tmp = token
            tmp += unicode("\t")
            tmp += unicode(labels[i])
            tmp += unicode("\n")
            if ("\t" not in tmp):
                raise "Huh?"
            outstream.write(tmp)
            # outstream.write(sentTokens[i])
            # outstream.write(unicode("\t"))
            # outstream.write(labels[i])
            # outstream.write("\n")
        # Last line must be empty with newline.     
        outstream.write("\n")
    instream.close()

def fixToken (token):
    hasWhite = False
    for char in token:
        if (shouldRemoveChar(char)):
            hasWhite = True
            break
    if (hasWhite):
        newToken = ""
        for char in token:
            if (not shouldRemoveChar(char)):
                newToken += char
        if (newToken == ""):
            newToken = "__BAD__"
        # print >> sys.stderr, ("Substitute [{}] for [{}]".format(newToken, token)).encode('utf-8')
        print >> sys.stderr, "Substitute old {} new {}".format(json.dumps(token), json.dumps(newToken))
        print >> sys.stderr, "Removed {}".format(["#"+str(i) for i,c in izip(count(len(token)), token)])
        return newToken
    else:
        return token

def shouldRemoveChar (char):
    # return (char.isspace() or char == u"\u009c")
    if char.isspace():
        print >> sys.stderr, "Must remove char [%r]" % char
    return char.isspace()

def filterTokens (tokens,labels):
    validIndices = []
    for i in range(0,len(tokens)):
        token = tokens[i]
        label = labels[i]
        if (token not in excludeTokens or label != "O"):
            validIndices.append(i)
    newTokens = []
    newLabels = []
    for idx in validIndices:
        newTokens.append(tokens[idx])
        newLabels.append(labels[idx])
    return (newTokens,newLabels)

def generateLabelsForSentence (words,entities):
    labels = ["O"] * len(words)
    for e in entities:
        for i in range(e.start,e.end+1):
            label = e.type
            if (useIOB):
                prefix = "B_" if i == e.start else "I_"
                label = prefix + label
            if (e.end >= len(labels)):
                raise RuntimeError(format("'%s' %s %d:%d %s" % (" ".join(words),e.type,e.start,e.end,e.string)))
            labels[i] = label
    return labels

def split (strg,sep=" "):
    tokens = []
    for tok in strg.split(sep):
        if (tok != ""):
            tokens.append(tok)
    return tokens


def nonOverlapping (files1, files2):
    """Takes two lists of files; raises an exception if they overlap."""
    for file1 in files1:
        for file2 in files2:
            if (file1 != None and file2 != None and file1 == file2):
                raise RuntimeError(format("Can't overwrite %s" % file1)) 
        
class Entity:
    def __init__ (self):
        self.type   = None
        self.start  = None
        self.end    = None
        self.worker = None  
        self.tokens = None
        self.string = None

######################################

if (__name__ == "__main__"):
    main()
