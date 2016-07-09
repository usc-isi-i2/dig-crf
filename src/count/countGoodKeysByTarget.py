"""Given a SEQ(Text, Text) input file to use as an RDD, where the
value field is supposed to be a dictionary in JSON, count the number
of occurances of each unique key in the set of dictionaries, for each
publisher.  Print the resulting map (key => count), sorted by key."""

import argparse
import json
import sys
from pyspark import SparkContext

def getKeysByTarget(value):
    global goodJsonRecords, badJsonRecords, noExtractionsCount, noTitleCount, noTitleAttribsCount, noTitleAttribsTargetCount, noUrlCount
    try:
        d = json.loads(value)
        goodJsonRecords += 1
    except:
        badJsonRecords += 1
        return iter([])

    goodTargetName = False
    if "extractions" not in d:
        targetName = "(No extractions)"
        noExtractionsCount += 1
        extractions = None
    else:
        extractions = d["extractions"]
        if "title" not in extractions:
            targetName = "(No title)"
            noTitleCount += 1
        elif "attribs" not in extractions["title"]:
            targetName = "(No title attribs)"
            noTitleAttribsCount += 1
        elif "target" not in extractions["title"]["attribs"]:
            targetName = "(No title attribs target)"
            noTitleAttribsTargetCount += 1
        else:
            targetName = extractions["title"]["attribs"]["target"]
            goodTargetName = True

    if not goodTargetName:
        if "url" not in d:
            noUrlCount =+ 1
        else:
            # Go for URL:
            url = d["url"]
            httpPart, emptyPart, domainName, remainder = url.split("/", 3)
            if domainName:
                targetName = domainName + " " + targetName                

    results = [ json.dumps(targetName + ": " + key) for key in d.keys() ]
    if extractions:
        results.extend([ json.dumps(targetName + ": extractions: " + key) for key in extractions.keys() ])

    return iter(results)

def getKeysByDomainName(value):
    global goodJsonRecords, badJsonRecords, noExtractionsCount, noUrlCount, noDomainNameCount
    try:
        d = json.loads(value)
        goodJsonRecords += 1
    except:
        badJsonRecords += 1
        return iter([])

    if "url" not in d:
        domainName = "(no url)"
        noUrlCount =+ 1
    else:
        url = d["url"]
        splitUrl = url.split("/")
        if len(splitUrl) < 3:
            domainName="(bad url)"
        else:
            httpPart, emptyPart, domainName = splitUrl[:3]
            if not domainName:
                domainName="(no domain name)"

            # Reduce the domain name in an ad-hoc way:
            components = domainName.split(".")
            if len(components) >= 2:
                if components[-2] in ["com", "co"] and len(components) >= 3:
                    domainName = ".".join(components[-3:])
                else:
                    domainName = ".".join(components[-2:])

    if "extractions" not in d:
        noExtractionsCount += 1
        extractions = None
    else:
        extractions = d["extractions"]

    results = [ json.dumps(domainName + ": " + key) for key in d.keys() ]
    if extractions:
        results.extend([ json.dumps(domainName + ": extractions: " + key) for key in extractions.keys() ])

    return iter(results)

def main(argv=None):
    '''this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help="Seq input file on cluster.", required=True)
    parser.add_argument('-u','--byUrl', help="Group by URL domain name.", required=False, action='store_true')
    args = parser.parse_args()

    sc = SparkContext()

    global goodJsonRecords, badJsonRecords, noExtractionsCount, noTitleCount, noTitleAttribsCount, noTitleAttribsTargetCount, noUrlCount, noDomainNameCount
    goodJsonRecords = sc.accumulator(0)
    badJsonRecords = sc.accumulator(0)
    noUrlCount = sc.accumulator(0)
    noDomainNameCount = sc.accumulator(0)
    noExtractionsCount = sc.accumulator(0)
    noTitleCount = sc.accumulator(0)
    noTitleAttribsCount = sc.accumulator(0)
    noTitleAttribsTargetCount  = sc.accumulator(0)

    data = sc.sequenceFile(args.input, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    if args.byUrl:
        keyCounts = data.values().flatMap(getKeysByDomainName).countByValue()
    else:
        keyCounts = data.values().flatMap(getKeysByTarget).countByValue()

    print "========================================"
    print "goodJsonRecords = %d" % goodJsonRecords.value
    print "badJsonRecords = %d" % badJsonRecords.value
    print "noUrlCount = %d" % noUrlCount.value
    print "noDomainNameCount = %d" % noDomainNameCount.value
    print "noExtractionsCount = %d" % noExtractionsCount.value
    if not args.byUrl:
        print "noTitleCount = %d" % noTitleCount.value
        print "noTitleAttribsCount = %d" % noTitleAttribsCount.value
        print "noTitleAttribsTargetCount = %d" % noTitleAttribsTargetCount.value
    print "========================================"

    priorDomain = None
    for k in sorted(keyCounts):
        newDomain, key = k.split(":")
        if priorDomain is not None and priorDomain != newDomain:
            print
        priorDomain = newDomain
        print k, keyCounts[k]
    print "========================================"

    sc.stop()

# call main() if this is run as standalone                                                             
if __name__ == "__main__":
    sys.exit(main())
