##############################################################################
# Used to build a classifier                                                 #
# Input: 2 separate documents containing positive and negative sentiments    #
# Output: Classifier object serialized - classifier.pickle                   #
##############################################################################

import nltk
import string
from nltk import word_tokenize,sent_tokenize,pos_tag
import pickle
from nltk.stem import WordNetLemmatizer

def filter_printable(text):
    return ''.join([ character for character in text if character in string.printable ])

#sentiment lexicon - frequently used words that express sentiment
sentiment_lexicon = []

# documents to extract features from
pos_data = open("").read()
neg_data = open("").read()

# constructs a sentiment lexicon by considering the most frequently used adjectives in a document
def build_sentiment_lexicon(filename):
    all_words = []
    content = open(filename,'rb').read()
    for sent in sent_tokenize(content):
        pos_tagged_words = pos_tag(word_tokenize(filter_printable(sent)) )
        for word in pos_tagged_words:
            if word[1].startswith("JJ"):
                all_words.append(word[0])
    return nltk.FreqDist(all_words)

def extract_features(sentence):
    features = {}
    words = word_tokenize(sentence)
    for word in sentiment_lexicon:
        features[word] = (word in words)

    return features

featuresets = [(extract_features(rev), category) for (rev, category) in documents]

random.shuffle(featuresets)


#testing_set = featuresets[:]
#training_set = featuresets[:]

classifier = nltk.NaiveBayesClassifier.train(training_set)

pickle.dump(classifier, open("NaiveBayesClassifier","wb"))
