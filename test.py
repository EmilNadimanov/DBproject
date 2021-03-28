import json
import time
from aux import MongoLastInserted, MongoException
from mymongocoll import MyMongoCollection
from mymongodoc import MyMongoDoc, MongoId
from mymongoDB import MyMongoDB
import datetime

# CREATE DATABASE
mmdb = MyMongoDB("my_db")



# CREATE COLLECTION

# нет коллекции "тест" - создадим её
mmc = mmdb.test

assert mmc.parent_database == mmdb
assert mmc.name == "test"

# коллекция "тест" уже есть, так что достанем её
mmc_getitem = mmdb["test"]

assert mmc_getitem is mmc

# LIST DATABASE COLLECTIONS

assert mmdb.list_collections() == ["test"]

# INSERT ONE
post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.now()}
last_one: MongoId = mmc.insert_one(post).inserted_id()

found: MyMongoDoc = mmc.find_one({"author": "Mike"})

assert last_one == found.objectId

# INSERT MANY
post_1 = {"author": "Dwight",
          "text": "Beets, Bears, Battlestar Galactica",
          "tags": ["co-manager", "paper", "Dunder-Mifflin"],
          "date": datetime.datetime.now()}

post_2 = {"author": "Kevin",
          "text": "Why say lot word but few word do trick",
          "tags": ["chili", "accounting", "women"],
          "date": datetime.datetime.now()}

last_ones = mmc.insert_many(post_1, post_2)

assert len(mmc) == 3

# CANNOT INSERT THE SAME OBJECT

try:
    mmc.insert_one(post)
except MongoException as e:
    print("avoided")

# DELETE ONE

mmc.delete_one(last_one)

assert len(mmc) == 2

# DELETE MANY

ids = [last.objectId for last in last_ones]
mmc.delete_many(ids)

assert len(mmc) == 0

##################
# DRAMATIC PAUSE #
##################

# loading data

with open('data.json5') as f:
    j = json.loads(f.read())

events = j['result']['events']


# FIND ONE


t = time.time()
mmc.clear()
mmc.insert_many(events)
print(time.time() - t)

#FIND

t1 = time.time()
mmc.find({"date": 2012, "category_1": "June"})
without_index = time.time() - t1

# CREATE INDEX + FIND
mmc.create_index("date")

t2 = time.time()
mmc.find({"date": 2012, "category_1": "June"})
with_index = time.time() - t2

# I AM BORED, LET'S SOBIRAT MANATKI AND GET OUT

mmdb.save("MyMongoShenanigans.mongodb", overwrite=True)

mmdb = None
# ...

# OK, BACK TO WORK
assert mmdb is None

mmdb = MyMongoDB.load("MyMongoShenanigans.mongodb")

all_the_good_stuff = mmdb.list_collections()

all_the_data = mmdb[all_the_good_stuff[0]]
a = 1