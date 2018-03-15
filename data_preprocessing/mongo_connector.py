from pymongo import MongoClient
import pprint

class MongoConnector:

    def __init__(self, ipAddress, port):
        self.client = MongoClient(ipAddress, port)
        self.db = None

    def createDatabase(self, name):
        self.db = self.client[name]
        return self.db

    def createCollection(self, name):
        return self.db[name]

    def insert_one(self, data, collection):
       return collection.insert_one(data).inserted_id

    def insert_many(self, data, collection):
        return collection.insert_many(data).inserted_ids

    def read(self, collection, _id):
        return collection.find_one({"_id": _id})
       
    def update(self):
        pass
    
    def delete(self):
        pass
    
    def close(self):
        self.client.close()

if __name__ == '__main__':

    ipAddress = 'localhost'
    port = 27017

    mc = MongoConnector(ipAddress, port)
    
    db = mc.createDatabase("test_db")
    collection = mc.createCollection("test_collection")
    
    book = {}
    book ["_id"] = "123"
    book ["title"] = "MongoDB Guide"
    book ["year"] = 2017
    book ["author"] = "me"

    inserted = mc.create(book, collection)
    print inserted

    print "Reading from the database with id " + inserted

    pprint.pprint(mc.read(collection, inserted))









   