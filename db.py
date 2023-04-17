import pymongo
from pymongo.collection import Collection
from pymongo.database import Database


mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db: Database = mongo_client["mydatabase"]
jobs_collection: Collection = db["jobs"]
frames_collection: Collection = db["frames"]


def print_db():
    print("collections: ", db.list_collection_names())

    print("\njobs collection:")
    for x in jobs_collection.find():
        print(x)

    print("\nframes collection:")
    for x in frames_collection.find():
        print(x)


def clear_db():
    jobs_collection.drop()
    frames_collection.drop()


# clear_db()
print_db()
