from mongo_connector import *
import requests
import json
import sys
import os
import log
import string
import datetime
# import pickle

# main class to read data
# input:- path of config file, path to destination csv file, log directory path
def filter_printable(s):
    printable = set(string.printable)
    return filter(lambda x: x in printable, s)

class PageFeedReader:
    def __init__(self, conf_path):
        all_conf = json.load(open(conf_path))
        self.conf = all_conf['facebook']
        self.data_path = str((datetime.datetime.now()).strftime('%H_%M_%S.%f')) + "_" + str(self.conf['data_path'])
        # self.terms = terms
        self.logger = log.Logger(all_conf['log_path'])
        self.total_comment_count = 0
        self.pages = self.get_page_list(self.conf['page_list_file'])
        # self.mc = MongoConnector('localhost' , 27017)
        # self.db = self.mc.createDatabase("cloud_db")

    def get_page_list(self, path):
        print "Considering pages listed in " + path
        text_file = open(path, "r")
        return text_file.readlines()

    # constructs request url either to fetch a page or a comment(s) based on what is given in kwargs
    def construct_url(self, kwargs):
        node_id, params, edge = kwargs['node_id'], self.conf['data'][kwargs['node_type']]['params'], self.conf['data'][kwargs['node_type']]['edge']
        url = self.conf['url_root'] + '/' + node_id + '/' + edge + '/'
        query_string = '&'.join([ key + '=' + (','.join(value) if type(value) == list else value) for key, value in params.iteritems() ])
        query_string += '&access_token=' + self.conf['access_token']
        return url + '?' + query_string

    # method to fetch all the posts in a page
    def fetch_all_posts(self):
        # posts = self.fetch_all(self.base_url,'posts')
        posts = self.fetch_all(self.construct_url({'node_type':'page','node_id':self.page}),'posts')
        print "Scanned " + str(len(posts)) + " posts"
        self.logger.info("Scanned " + str(len(posts)) + " posts")
        return posts

    # method to fetch all comments in a post
    # input:- id of the post
    def fetch_all_comments(self, post_id):
        return self.fetch_all(self.construct_url({'node_type':'post','node_id':post_id}),'comments')

    # method to handle pagination and fetch all data(can be either posts or comments)
    def fetch_all(self,initial_url,typ):
        try:
            data = json.loads((requests.get(initial_url)).text)
            data_collection = data['data']
            # print initial_url
        except Exception as e:
            print initial_url
            print "There was an error while trying to fetch " + typ + ". Please check the input parameters, the key might be wrong" + str(e)
            self.logger.error("There was an error while trying to fetch " + typ + ". Please check the input parameters, the key might be wrong" +str(e))
            return []
            # exit(1)
        count = 0
        while data.has_key('paging') and data['paging'].has_key('next'):
            next_url = data['paging']['next']
            data = json.loads((requests.get(next_url)).text)
            if data.has_key('data'):
                data_collection += data['data']
                count += len(data_collection)
            if typ == "posts":
                sys.stdout.write("Scanned %d posts\r" % len(data_collection))
            else:
                sys.stdout.write("Fetched %d comments\r" % len(data_collection))
        return data_collection

    # method to look for posts containing only those terms that are specified in the "terms" field in the config file
    # it sees if "all" the words present in the "terms" field are present either in the post's message or in the
    # heading of the article link in the body of the post
    def filter_data(self,data):
        filtered_data = []
        filename = "postmsgs.txt"
        f = open(filename,'w')
        try:
            for item in data:
                in_name, in_message = False, False
                # if the terms are present in the post's message
                if item.has_key('message'):
                    if item.has_key("created_time"):
                        f.write(filter_printable(item["message"].lower() + "\n" + item["created_time"]))
                    if all((word in item['message'].lower()) for word in self.terms.split()):
                        in_message = True
                # if the terms are present in the heading of an article link in the post's body
                if item.has_key('name'):
                    if item.has_key("created_time"):
                        f.write(filter_printable(item["name"].lower() + "\n" + item["created_time"]))
                    if all((word in item['name'].lower()) for word in self.terms.split()):
                        in_name = True
                if in_name or in_message:
                        filtered_data.append(item)
        except Exception as e:
            print "There was an error while trying to filter data. Error: " + str(e)
            self.logger.error("There was an error while trying to filter data. Error: " + str(e))
            exit(1)

        return filtered_data

    # wrapper to fetch and filter the posts and then invoke method to fetch all comments from those posts
    def fetch(self, model, model_terms, page):
        """
        if os.path.exists(self.conf['comment_ids_persisted']):
            self.comment_ids = pickle.load(open(self.conf['comment_ids_persisted']))
        else:
            self.comment_ids = set()
        """
        self.comment_ids = []
        self.page = page.strip()
        self.terms = model_terms
        print "\nScanning page:" + self.page
        print "Term:" + self.terms
        filtered_posts = self.filter_data(self.fetch_all_posts())
        
        print "Relevant posts: " + str(len(filtered_posts))
        self.logger.info("Relevant posts: " + str(len(filtered_posts)))
        
        comment_count = 0
        all_comments = []
        for post in filtered_posts:
            if post.has_key('id'):
                cms = self.fetch_all_comments(post['id'])
                comment_count += len(cms)
                for item in cms:
                    comment = {}
                    comment['id'] = item['id']
                    comment['comment'] = item['message']
                    all_comments.append(comment)

                    #Check where the comment is present in the set
                    #if no, then add it to the set and also all_comments
                    #if yes, then skip it altogether
                    #if comment['id'] not in self.comment_ids:
                        #self.comment_ids.append(comment['id'])
                        #all_comments.append(comment)
                # self.write_out(cms,post['id'])
        
        print "Fetched " + str(comment_count) + " comments from all the relevant posts of this page"
        self.logger.info("Fetched " + str(comment_count) + " comments from all the relevant posts of this page")
        self.total_comment_count += comment_count

        # pickle.dump(self.comment_ids, open(self.conf['comment_ids_persisted'],'w'))
        return all_comments
    # extracts fields from the posts and write to csv file
    # if it is decided to fetch more fields from facebook than what it is now, then code in this method
    # needs to be modified
    def write_out(self,data,post_id):
        # filename = datetime.datetime.now().strftime("%H%M%S")
        f = open(self.data_path,'a')
        try:
            for item in data:
                # print item
                if (len(self.comment_ids) == 0) or ((len(self.comment_ids) > 0) and (item['id'] not in self.comment_ids)):
                    write_string = ''
                    write_string += ((item['message'].replace(',','')).replace("\n",' ') if item.has_key('message') else '')
                    write_string += ',' + (item['created_time'] if item.has_key('created_time') else '')
                    write_string += ',' + (item['from']['name'] if item.has_key('from') and item['from'].has_key('name') else '')
                    write_string += '/' + (item['from']['id'] if item.has_key('from') and item['from'].has_key('id') else '')
                    write_string += ',' + item['id'] + "," + post_id + "," + self.page + ",facebook\n"
                    f.write(filter_printable(write_string))
            f.close()
        except Exception as e:
            print "Error while trying to write data: " + str(e)
            self.logger.error("Error while trying to write data: " + str(e))
            exit(1)


    def fetch_everything(self, model, terms):
        if os.path.exists(self.data_path):
            self.comment_ids = [line.split(',')[4].strip() for line in open(self.data_path,'rb').readlines()]
        else:
            self.comment_ids = []

        self.terms = terms
        print "Beginning FB scan"
        self.logger.info("Beginning FB scan")
        # print self.pages
        all_comments = []
        for page in self.pages:
            self.page = page.strip()
            print "Scanning FB page: " + page
            self.logger.info("Scanning FB page: " + page)
            # self.base_url = self.construct_url({'node_type':'page'})
            # all_comments.extend(self.fetch())
            comments = self.fetch()
            
            self.write_data(model, comments)


        # print "Written " + str(self.total_comment_count) + "comments to " + self.data_path
        # self.logger.info("Written " + str(self.total_comment_count) + "comments to " + self.data_path)
        return all_comments

    def write_data(self, model, data):

        collection_name = "facebook_" + model
        collection = self.mc.createCollection(collection_name)

        print "Inserting " + str(len(data)) + " reviews under the collection : " +  collection_name

        self.mc.insert_many(data, collection)

if __name__ == '__main__':
    pf = PageFeedReader('conf.json')
    pf.fetch_everything()