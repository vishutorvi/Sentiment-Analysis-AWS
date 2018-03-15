import json
import pickle
import os

glossary = pickle.load(open('glossary.pickle','rb'))
data = json.load(open('sentiments.json','rb'))

nd = {}

imgdir = './images/'

for item in data['models']:
	model = ([key for key in item.keys() if key != '_id'])[0]
	d = item[model]
	model = model.replace('facebook_','')
	nd[model] = {}
	nd[model]['features'] = {}
	nd[model]['location'] = ''
	# print "model:" + model
	# print "aspects:" + str(d.keys())
	aspects = d.keys()
	for aspect in aspects:
		if aspect.lower() in glossary:
			nd[model]['features'][aspect] = d[aspect]

	im = [ i for i in os.listdir(imgdir) if i.startswith(model)]

	if len(im) > 0:
		nd[model]['location'] = imgdir + im[0]


json.dump(nd,open('mastered.json','wb'))
