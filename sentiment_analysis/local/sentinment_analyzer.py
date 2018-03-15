import os
import pickle
from nltk import sent_tokenize, word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import json


class TopicClassifier:

    def __init__(self):
        # self.read_data()
        self.read_model()
        # print self.items
        self.sid = SentimentIntensityAnalyzer()

    def read_model(self):
        if os.path.exists('model1.pickle'):
            self.model = pickle.load(open('model1.pickle', 'rb'))
        else:
            self.train_model()
            print('No model found, training')
            # exit(1)
        # self.train_model()

    def read_data(self):
        # self.data = pickle.load(open(cls'data.pickle','rb'))
        # self.items = open('positive.txt','rb').readlines()
        # self.items = [ item for item in data]
        # print self.items
        pass

    def extract_features(self, sentence, ngram=False):
        features = {}
        for word in word_tokenize(sentence):
            if word.lower() in features.keys():
                features[word.lower()] += 1
            else:
                features[word.lower()] = 1
        if ngram:
            features[sentence] = True

        return features

    def analyze_sentence(self, sentence):
        polarity = self.sid.polarity_scores(sentence)
        return {'category': self.model.classify(self.extract_features(sentence)), 'polarity': ' '.join(
            [str(key + ':' + str(polarity[key])) for key in polarity.keys()])}

    def train_model(self):
        self.feature_set = []

        # for category in self.data.keys():
        # 	if (type(self.data[category]) == dict):
        # 		category_items = self.data[category].keys()
        # 	else:
        # 		category_items = self.data[category]

        # feature_set += [(self.extract_features(item,ngram=True),"positive") for item in category_items]
        # i = 0
        # for item in self.items:
        #     self.feature_set += (self.extract_features(item),"positive")
        #     i+=1
        #     print str(i) + "\r"

        if os.path.exists('model.pickle'):
            self.model = pickle.load(open('model.pickle', 'rb'))
        else:
            # self.train_model()
            print('No model found, training')

        items_p = open('p.txt', 'rb').readlines()
        items_n = open('n.txt', 'rb').readlines()
        self.feature_set += [(self.extract_features(item,
                                                    ngram=True), "positive") for item in items_p]
        self.feature_set += [(self.extract_features(item,
                                                    ngram=True), "negative") for item in items_n]
        self.model = nltk.classify.NaiveBayesClassifier.train(self.feature_set)
        self.write_model()

    def write_model(self):
        pickle.dump(self.model, open('model.pickle', 'wb'))


if __name__ == '__main__':

    c = TopicClassifier()
    # c.train_model()
    # c.write_model()
    # print c.feature_set
    yorn = 'c'

    while yorn != 'n':
        sent = raw_input("Sentence:")
        print c.analyze_sentence(sent)
        yorn = raw_input("Continue?(y/n)")
