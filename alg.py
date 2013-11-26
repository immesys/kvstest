import time
import random
import uuid

def run_test(provider, scale):
    doc = {
        "key":provider.key(),
        "prop1":450,
        "prop2":500.5,
        "prop3":"aval",
        "children":[provider.key() for k in 128]
        }

    #Test a single object insert
    rng = int(1000*scale)
    inserted = [None] * rng
    then = time.time()
    for i in xrange(rng):
        k = provider.key()
        doc["key"] = k
        provider.insert(k, doc)
        inserted[i] = k
    now = time.time()
    print "Single insert %d records in %.3f seconds" % (rng, now-then)
    random.shuffle(inserted)

    #Locate objects
    then = time.time()
    for k in inserted:
        doc = provider.get(k)
    now = time.time()
    print "Located records in %.3f seconds" % (now - then,)


class MongoProvider(object):
    def __init__(self):
        global pymongo
        pymongo = __import__("pymongo")
        self.db = pymongo.MongoClient()
        self.col = self.db.col
        self.col.ensure_index("key")

    def key(self):
        return int(uuid.uuid1())

    def insert(self, k, v):
        self.col.save(v)

    def get(self, k):
        return self.col.find_one({"key":k}) 

if __name__ == "__main__":
    provider = None
    scale = 1.0
    if sys.argv[0] == "mongo":
            provider = MongoProvider()
    if len (sys.argv) == 2:
            scale = float(sys.argv[1])
            
    run_test(provider, scale)