from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark import SparkContext
import json

sid = SentimentIntensityAnalyzer()
sc = SparkContext()

pos = sc.textFile("hdfs://ip-172-31-22-218.ec2.internal:8020/user/hadoop/test/positive.txt")
total_pos = pos.count()
neg = sc.textFile("hdfs://ip-172-31-22-218.ec2.internal:8020/user/hadoop/test/negative.txt")
total_neg = neg.count()
print "files read"

data_pos = pos.map(lambda line: 1 if (sid.polarity_scores(line)['pos'] > sid.polarity_scores(line)['neg']) else 0)
print "pos mapped"
p_temp = data_pos.filter(lambda x: x==1)

print "pos filtered"
pos_corr = p_temp.count()
print "pos counted"
#pos_corr = data_pos.count()

data_neg = neg.map(lambda line: 1 if (sid.polarity_scores(line)['pos'] < sid.polarity_scores(line)['neg']) else 0)
print "neg mapped"
n_temp = data_neg.filter(lambda x: x==1)
print "neg filtered"
neg_corr = n_temp.count()
print "neg counted"

accuracy_pos = float(pos_corr)/float(total_pos)
accuracy_neg = float(neg_corr)/float(total_neg)

print "pos_count:" + str(pos_corr)
print "neg_count:" + str(neg_corr)
print "pos_accuracy:" + str(accuracy_pos*100)
print "neg_accuracy:" + str(accuracy_neg*100)
