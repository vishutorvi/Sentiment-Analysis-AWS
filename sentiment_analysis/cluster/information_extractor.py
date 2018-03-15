import re
import pprint
from nltk import word_tokenize, sent_tokenize, pos_tag, RegexpParser
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
from nltk.stem import WordNetLemmatizer
from nltk.classify import ClassifierI
import pickle
import os
from statistics import mode
from pymongo import *
import string

class OpinionExtractor:
    def __init__(self):
        #self.data_source = json.load(open('all_data.json','r'))
        classifier = pickle.load(open("originalnaivebayes5k.pickle", "rb")
        
        self.sentiment_classifier = classifier

        #self.sentiment_classifier = VoteClassifier(classifier)

        self.word_features = pickle.load(open("word_features5k.pickle", "rb"))
        self.lemmatizer = WordNetLemmatizer()

        self.config = json.load(open("config.json" , "r"))
        db_name = self.config['db_name']
        db_ip = self.config['db_ip']
        db_port = self.config['db_port']

        self.dbclient = MongoClient(db_ip, db_port)
        self.db = self.dbclient[db_name]
        
        self.sid = SentimentIntensityAnalyzer()
        
        if os.path.exists('aggregated.json'):
            self.aggregation_data = json.load(open('aggregated.json','rb'))
        else:
            self.aggregation_data ={}
        
        #self.stop_words = open('stop_words.txt', 'r')

    def __getstate__(self):
          state = {}
          state['test'] = 'test'
          return state
        

    def find_features(self, document):
        words = word_tokenize(document)
        features = {}
        for w in self.word_features:
            features[w] = (w in words)
        
        #print "Printing features"
        #print features

        return features

    def pos_tag_my(self, text):
        """
        splits a text stream into sentences, tokenizes and returns a list of pos tagged sentences
        """
        word_tokenized = word_tokenize(text)
        lemmatized_sentence = [self.lemmatizer.lemmatize(word) for word in word_tokenized]
        
        return pos_tag(lemmatized_sentence)

    def np_chunk(self, pos_tagged):
        """
        returns the noun phrases in the sentences
        """
        grammar = r"""
        NP: {<DT>?<JJ.*>*<NN.*>+}
        """
        chunk_parser = RegexpParser(grammar)
        # for subtree in tree.subtrees():
            # if subtree.label() == 'CHUNK': print(subtree)
        return [ chunk_parser.parse(sent) for sent in pos_tagged]

    def analyze_sentiment(self, text):
        #print text
        return self.sentiment_classifier.classify(self.find_features(text))

    def process(self,model):
        data = self.read_data(model)
        
        self.model = model
        
        dict_noun = {}

        if not(self.aggregation_data.has_key(model)):
            self.aggregation_data[model] = {}
        
        #data = ["This is  very good phone", "The battery sucks"]
        for review in data:
            # sentences = sent_tokenize(review)
            if review.has_key('comment'):                
                sentences = sent_tokenize(review['comment'])
                polarity = 0
                
                for sentence in sentences:
                    #print sentence
                    #break
                    """
                    sentence = current_sentence
                    for line in self.stop_words:
                        line1 = line.strip()
                        sentence = sentence.replace(str(line1), '')
                       
                    print sentence
                    """
                    
                    sentence = self.stem_emotion_removal(sentence)
                    polarity = self.sid.polarity_scores(sentence)
                    sentiment = self.analyze_sentiment(sentence)
                    pos_tags = self.pos_tag_my(sentence)
                    nouns =  self.find_pos_tagged_words(pos_tags, "NN")
                    
                    adjectives = self.find_pos_tagged_words(pos_tags, "JJ")
                    self.associate_adjectives_nouns(adjectives, nouns)    
                    for noun in nouns:
                        self.aggregation_data[model][noun]['sentiment'][sentiment] += 1
                        if(noun in dict_noun):
                            dict_noun[noun]['polarity'] += polarity['compound']
                            dict_noun[noun]['count'] += 1
                        else:
                            dict_noun[noun] = {'polarity' : polarity['compound'], 'count' : 1 }
                    
                    
                    for noun in nouns:
                        score = dict_noun[noun]['polarity'] / dict_noun[noun]['count']
                        sentiment = self.get_score_bracket(score)
                        self.aggregation_data[model][noun]['sentiment']['score'] = score
                        self.aggregation_data[model][noun]['sentiment']['overall_sentiment'] = sentiment
                    
        print "Writing data to op JSON file"
        #json.dump(self.aggregation_data, open('/home/ec2-user/aggregated.json','w'))
        self.write_data()
        return "success"
    
    def get_score_bracket(self, score):
        if score >= 0.75:
            return "Very Good"
        elif score >= 0.50 and score < 0.75:
            return "Good"
        elif score > 0.25 and score < 0.50:
            return "Above Average"
        elif score > 0 and score <= 0.25:
            return "Average"
        elif score > -0.25 and score <= 0:
            return "Below Average"
        elif score > -0.50 and score <= -0.25:
            return "Bad"
        elif score < -0.50:
            return "Very Bad"
        

    def read_data(self, model):    
        return self.db[model].find()
        #return self.data_source[model]

    def write_data(self):
        collection = self.db['results']
        #print type(self.aggregation_data)
        #print self.aggregation_data
        """
        for key in self.aggregation_data.keys():
            db_data = {}
            db_data[key] = self.aggregation_data[key]
            collection.insert_one(json.dumps(db_data))
        """
        collection.insert_many([self.aggregation_data])

    def associate_adjectives_nouns(self, adjectives, nouns):
        model = self.model
        for noun in nouns:
            if not(self.aggregation_data[model].has_key(noun)):
                self.aggregation_data[model][noun] = {}
                self.aggregation_data[model][noun]['sentiment'] = {}
                self.aggregation_data[model][noun]['sentiment']['pos'] = 0
                self.aggregation_data[model][noun]['sentiment']['neg'] = 0
                self.aggregation_data[model][noun]['sentiment']['score'] = 0
                self.aggregation_data[model][noun]['sentiment']['overall_sentiment'] = ""
                self.aggregation_data[model][noun]['adjectives'] = []
            for adjective in adjectives:
                self.aggregation_data[model][noun]['adjectives'].append(adjective)

    def find_pos_tagged_words(self, tagged_items, tag):
        retval = []
        for tagged_item in tagged_items:
            if tagged_item[1].startswith(tag):
                retval.append(tagged_item[0])
        return retval
    
    def stem_emotion_removal(self, sentence):
        emoji_pattern = re.compile(
            u"(\ud83d[\ude00-\ude4f])|"  # emoticons
            u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
            u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
            u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
            u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
            "+", flags=re.UNICODE)
        
        for p in string.punctuation:
            sentence = sentence.replace(p,'')
     
        cleaned = emoji_pattern.sub(r'', sentence)
        
        return cleaned
    
    def extract_entities(text):
        for sent in nltk.sent_tokenize(text):
            for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
                if hasattr(chunk, 'label'):
                    print chunk.label(), ' '.join(c[0] for c in chunk.leaves())       

if __name__ == '__main__':
    extractor = OpinionExtractor()

    config = json.load(open("config.json" , "r"))
    db_name = config['db_name']
    db_ip = config['db_ip']
    db_port = config['db_port']

    dbclient = MongoClient(db_ip, db_port)
    db = dbclient[db_name]

    collections = db.collection_names()

    for coll in collections:
        extractor.process(coll)
    #extractor.process('facebook_iPhoneX')
