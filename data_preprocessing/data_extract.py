from mongo_connector import *
import json
import facebook_moda

mc = None
db = None

def extract_data():
    """
        main method to extract data
    """

    terms_file = '../templates/terms.json'
    pages = [page.strip() for page in (open('pages.txt','r')).readlines()]
    terms_file_f = open(terms_file, 'r')
    terms = json.load(terms_file_f)
    phone_models = terms.keys()
    print phone_models
    # extract data from fb
    fb_extractor = facebook_moda.PageFeedReader('conf.json')
    global mc
    global db
    mc = MongoConnector('localhost' , 27017)
    db = mc.createDatabase("cloud_db")

    for model in phone_models:
        model_data = []
        model_terms = terms[model]
        for model_term in model_terms:
            for page in pages:
                data = fb_extractor.fetch(model, model_term, page)
                write_data(model, data)
                #Close the mongodb connection after data is inserted
                mc.close()
            """model_data.extend(fb_extractor.fetch_everything(model_term))
            write_data(model, model_data)
            model_data = []"""

def write_data(model, data):
    """
        creates a new file based on the 'model' name and dumps 'data' to it
    """
    collection_name = "facebook_" + model
    collection = mc.createCollection(collection_name)

    print "Inserting " + str(len(data)) + " reviews under the collection : " +  collection_name

    if(len(data) > 0):
        mc.insert_many(data, collection)


    """json.dump(data, open(model+'_facebook.json', 'w'))"""

if __name__ == '__main__':
    extract_data()
