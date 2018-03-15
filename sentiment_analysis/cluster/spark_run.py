from pyspark import SparkContext
from pymongo import *
import json
import information_extractor as extractor

sc = SparkContext(pyFiles = ["information_extractor.py", "originalnaivebayes5k.pickle","MNB_classifier5k.pickle","BernoulliNB_classifier5k.pickle","LogisticRegression_classifier5k.pickle","LinearSVC_classifier5k.pickle","word_features5k.pickle", "config.json"])

config = json.load(open("config.json" , "r"))


dbclient = MongoClient()
db = dbclient[config['db_name']]
collections = db.collection_names()

collections.remove("system.indexes")

#collections = "reviews_db" 
#print collections

collections_rdd = sc.parallelize(collections)

#extractor_instance = extractor.OpinionExtractor()
#mapped = collections_rdd.map(lambda x: extractor_instance.process(x))
#mapped = collections_rdd.map(lambda x: x)

mapped = collections_rdd.map(lambda x : extractor.OpinionExtractor().process(x))

print mapped.collect()
