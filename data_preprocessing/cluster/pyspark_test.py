from pyspark import SparkContext
import json
import data_extractor_spark as extractor

sc = SparkContext(pyFiles = ["data_extractor_spark.py" , "facebook_moda.py", "mongo_connector.py" ,"log.py", "conf.json", "pages.txt"])
print "files injected"

terms = json.load(open("terms.json","r"))
print "terms loaded"

dd = [ {model:terms[model]} for model in terms.keys()]
print "dd prepeared"

rdd = sc.parallelize(dd)
print "rdd prepared"

mapped = rdd.map(lambda x: extractor.extract_data(x))
#mapped = rdd.map(lambda x: x)
print mapped.collect()

#prnt mapped.take(10)

print "done"
