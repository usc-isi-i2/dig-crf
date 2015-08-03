#!/usr/bin/python

import csv
import codecs
import cStringIO

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

idx = {}
rows = []

for i in range(0,8):
    p = 'part-0000%s' % i
    with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/bodies/' + p, 'r') as f:
        for l in f:
            uri, body = eval(l)
            rows.append({"uri": uri, "body": body})
            idx[uri] = len(rows)-1

for i in range(0,8):
    p = 'part-0000%s' % i
    with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/baseEyecolor/' + p, 'r') as f:
        for l in f:
            uri, eyeColor = eval(l)
            if eyeColor is not None:
                rows[idx[uri]].update({"baseEyecolor": eyeColor})

for i in range(0,8):
    p = 'part-0000%s' % i
    with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/baseHaircolor/' + p, 'r') as f:
        for l in f:
            uri, hairColor = eval(l)
            if hairColor is not None:
                rows[idx[uri]].update({"baseHaircolor": hairColor})

for i in range(0,8):
    p = 'part-0000%s' % i
    with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/crfOutput/' + p, 'r') as f:
        for l in f:
            uri, dict = eval(l)
            crfHair = None
            if dict and dict.get('category', None)=='hairType':
                crfHair = dict.get('words', "")
                rows[idx[uri]].update({"crfHairWords": crfHair})
            if dict and dict.get('category', None)=='eyeColor':
                crfEyes = dict.get('words', "")
                rows[idx[uri]].update({"crfEyesWords": crfEyes})
        

# for i in range(0,8):
#     p = 'part-0000%s' % i
#     with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/extractedHairtype/' + p, 'r') as f:
#         for l in f:
#             uri, hairColor = eval(l)
#             if hairColor is not None:
#                 rows[idx[uri]].update({"extractedHairtype": hairColor})

# for i in range(0,8):
#     p = 'part-0000%s' % i
#     with open('/Users/philpot/Documents/project/dig-mturk/spark/data/output-compare/extractedEyecolor/' + p, 'r') as f:
#         for l in f:
#             uri, eyeColor = eval(l)
#             if eyeColor is not None:
#                 rows[idx[uri]].update({"extractedEyecolor": eyeColor})


with codecs.open('/tmp/excel.tsv', 'w', encoding='utf-8') as f:
    # csvWriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # csvWriter = UnicodeWriter(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')

    outrow = ["describesEyes","describesHair","baseEyecolor", "baseHaircolor", "crfEyesWords", "crfHairWords", "body", "uri"]
    outline = '\t'.join(outrow)
    f.write(outline + '\n')

    for row in rows:
        uri = row['uri']
        body = row['body']
        baseHaircolor = row.get('baseHaircolor', "")
        baseEyecolor = row.get('baseEyecolor', "")
        #extractedHairtype = row.get('extractedHairtype', "")
        #extractedEyecolor = row.get('extractedEyecolor', "")
        crfHairWords = row.get('crfHairWords', "")
        crfEyesWords = row.get('crfEyesWords', "")

        body = body.replace('\n', ' ')
        body = body.replace('\r', ' ')
        body = body.replace('\t', ' ')

        outrow = ["","",baseEyecolor, baseHaircolor, crfEyesWords, crfHairWords, body, uri]
        outline = '\t'.join(outrow)
        f.write(outline + '\n')

