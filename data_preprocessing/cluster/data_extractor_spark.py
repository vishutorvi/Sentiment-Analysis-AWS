from mongo_connector import *
import json
import facebook_moda

mc = None
db = None

def extract_data(model_dict):
    """
        main method to extract data
    """
    conf = json.load(open("conf.json" , "r"))
    mongo_ip = conf["db_host_ip"]
    db_name = conf["db_name"]

    pages = [page.strip() for page in (open('pages.txt','r')).readlines()]
   
    # extract data from fb
    fb_extractor = facebook_moda.PageFeedReader('conf.json')

    global mc
    global db

    mc = MongoConnector(mongo_ip , 27017)
    db = mc.createDatabase(db_name)

    model = (model_dict.keys())[0]
    terms = model_dict[model]

    print "about to start extracting"
    for term in terms:
        for page in pages:
            data = fb_extractor.fetch(model, term, page)
            write_data(model, data)

    #Close the database connection
    #mc.close()

    return "success from " + model

def write_data(model, data):
    """
        creates a new file based on the 'model' name and dumps 'data' to it
    """
    collection_name = "facebook_" + model
    collection = mc.createCollection(collection_name)

    print "Inserting " + str(len(data)) + " reviews under the collection : " +  collection_name

    if(len(data) > 0):
        mc.insert_many(data, collection)


if __name__ == '__main__':
    #extract_data()
    pass
