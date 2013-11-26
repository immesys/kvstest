import sys
import time
import random
import uuid
import json
import marshal

def run_test(provider, scale):
    doc = {
        "key":provider.key(),
        "prop1":450,
        "prop2":500.5,
        "prop3":"aval",
        "children":[provider.key() for k in xrange(128)]
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


class RedisSockJSONProvider(object):
    def __init__(self):
        global redis
        global ujson
        ujson = __import__("ujson")
        redis = __import__("redis")
        self.con = redis.StrictRedis(unix_socket_path='/tmp/redis.sock')
        self.con.flushall()

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.con.set(k, ujson.dumps(v))

    def get(self, k):
        return ujson.loads(self.con.get(k))

class RedisTCPJSONProvider(object):
    def __init__(self):
        global redis
        global ujson
        ujson = __import__("ujson")
        redis = __import__("redis")
        self.con = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.con.flushall()

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.con.set(k, ujson.dumps(v))

    def get(self, k):
        return ujson.loads(self.con.get(k))

class RedisTCPMarshallProvider(object):
    def __init__(self):
        global redis
        redis = __import__("redis")
        self.con = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.con.flushall()

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.con.set(k, marshal.dumps(v))

    def get(self, k):
        return marshal.loads(self.con.get(k))

class RedisSockPMarshallProvider(object):
    def __init__(self):
        global redis
        redis = __import__("redis")
        self.con = redis.StrictRedis(unix_socket_path='/tmp/redis.sock')
        self.con.flushall()

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.con.set(k, marshal.dumps(v))

    def get(self, k):
        return marshal.loads(self.con.get(k))

class MongoProvider(object):
    def __init__(self):
        global pymongo
        pymongo = __import__("pymongo")
        self.con = pymongo.MongoClient()
        self.db = self.con.db
        self.db.drop_collection("col")
        self.col = self.db.col
        self.col.ensure_index("key")

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.col.save(v)

    def get(self, k):
        return self.col.find_one({"key":k}) 

class KyotoFileProvider(object):
    def __init__(self):
        global kyoto
        kyoto = __include__("kyotocabinet")
        self.db = kyoto.open("test.kch",kyoto.DB.OTRUNCATE)
    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.db.set(k, marshal.dumps(v))

    def get(self, k):
        return marshal.loads(self.db.get(k))

class LevelProvider(object):
    def __init__(self):
        global level
        level = __import__("leveldb")
        self.db = level.LevelDb("./level.db")

    def key(self):
        return str(uuid.uuid1())

    def insert(self, k, v):
        self.db.Put(k, marshal.dumps(v))

    def get(self, k):
        return marshal.loads(self.db.Get(k))

if __name__ == "__main__":
    provider = None
    scale = 1.0
    if sys.argv[1] == "mongo":
            provider = MongoProvider()
    if sys.argv[1] == "level":
            provider = LevelProvider()
    if sys.argv[1] == "redis_tcp_json":
            provider = RedisTCPJSONProvider()
    if sys.argv[1] == "redis_tcp_marshal":
            provider = RedisTCPMarshallProvider()
    if sys.argv[1] == "redis_sock":
            provider = RedisSockPMarshallProvider()
    if sys.argv[1] == "redis_sock_json":
            provider = RedisSockJSONProvider()
    if len (sys.argv) == 3:
            scale = float(sys.argv[2])
    if provider == None:
	print "You need a provider"
	sys.exit(1)        
    run_test(provider, scale)
