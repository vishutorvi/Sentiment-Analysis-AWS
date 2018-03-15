# from dependencies import *
import requests
import json
import string
import sys
import urllib
import log
import os

# main class to read data
# input:- path of config file, path to destination csv file, log directory path
def filter_printable(s):
	printable = set(string.printable)
	return filter(lambda x: x in printable, s)

class YoutubeDownloader:
	def __init__(self,conf_path):
		all_conf = json.load(open(conf_path,'rb'))
		self.conf = all_conf['youtube']
		self.data_path = self.conf['data_path']
		self.writer = open(self.data_path,'a')
		self.logger = Logger(all_conf['log_path'])
		self.comment_count = 0

	# handles pagination and fetches all the top level comments and writes to csv file
	def fetch_top_level_comments(self,video_id):
		self.comment_count = 0
		try:
			req_url = self.conf['top_level_comment']['base_url'] + '&key=' + self.conf['key'] + '&videoId=' + video_id + '&' + '&'.join([item+'='+str(self.conf['top_level_comment']['params'][item]) for item in self.conf['top_level_comment']['params']])
			response = json.loads((requests.get(req_url)).text)
			comments = response['items']
		except Exception as e:
			print "Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e)
			self.logger.error("Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters"+str(e))
			return# exit(1)

		while True:
			for comment in comments:
				if (len(self.comment_ids) == 0) or ((len(self.comment_ids) > 0) and (comment['snippet']['topLevelComment']['id'] not in self.comment_ids)):
					write_data = []
					write_data.append(comment['snippet']['topLevelComment']['snippet']['textDisplay'])
					write_data.append(video_id)
					write_data.append(comment['snippet']['topLevelComment']['snippet']['authorDisplayName'])
					write_data.append(comment['snippet']['topLevelComment']['snippet']['publishedAt'])
					write_data.append(comment['snippet']['topLevelComment']['id'])
					self.writer.write(','.join([filter_printable(item.replace(","," ")) for item in write_data])+"\n")
				self.comment_count +=1
				sys.stdout.write("Fetched %d comments\r" % self.comment_count)
				if comment['snippet']['totalReplyCount'] > 0:
				    self.fetch_comment_replies(comment['snippet']['topLevelComment']['id'],video_id)
			
			if response.has_key('nextPageToken'):
			    try:
			        req_url = self.conf['top_level_comment']['base_url'] + '&key=' + self.conf['key'] + '&videoId=' + video_id + '&pageToken=' + response['nextPageToken']+ '&' + '&'.join([item+'='+str(self.conf['top_level_comment']['params'][item]) for item in self.conf['top_level_comment']['params']])
			        response = json.loads((requests.get(req_url)).text)
			        comments = response['items']
			    except Exception as e:
					print "Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e)
					self.logger.error("Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e))
					break# exit(1)
			else:
				break
				
	# handles pagination and fetches all the comment replies and writes to csv file
	def fetch_comment_replies(self,parent_comment_id,vid):
		try:
			req_url = self.conf['comment_replies']['base_url'] + '&parentId=' + parent_comment_id + '&key=' + self.conf['key'] + '&' +'&'.join([item+'='+str(self.conf['comment_replies']['params'][item]) for item in self.conf['comment_replies']['params']])
			response = json.loads((requests.get(req_url)).text)
			replies = response['items']
		except Exception as e:
			print "There was an error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e)
			self.logger.error("Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e))
			return# exit(1)

		while True:
			for reply in replies:
				if (len(self.comment_ids) == 0) or ((len(self.comment_ids) > 0) and (reply['id'] not in self.comment_ids)):
					write_data = []
					write_data.append(reply['snippet']['textDisplay'])
					write_data.append(vid)
					write_data.append(reply['snippet']['authorDisplayName'])
					write_data.append(reply['snippet']['updatedAt'])
					write_data.append(reply['id'])
					write_data.append(reply['snippet']['parentId'])
					self.writer.write(','.join([filter_printable(item.replace(","," ")) for item in write_data])+"\n")
				self.comment_count +=1
				sys.stdout.write("Fetched %d comments\r" % self.comment_count)

			if response.has_key('nextPageToken'):
			    try:
			        req_url = self.conf['comment_replies']['base_url'] + '&key=' + self.conf['key'] + '&parentId=' + parent_comment_id + '&pageToken=' + response['nextPageToken']+ '&'+'&'.join([item+'='+str(self.conf['comment_replies']['params'][item]) for item in self.conf['comment_replies']['params']])
			        response = json.loads((requests.get(req_url)).text)
			        replies = response['items']
			    except Exception as e:
					print "There was an error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e)
					self.logger.error("Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e))
					break# exit(1)
			else:
			    break

	def youtube_everything(self):
		print "Beginning Youtube scan"
		self.logger.info("Beginning Youtube scan")
		total_comments_from_all_videos = 0
		cf = self.conf
		iterations = cf['search']['maxVideos']/cf['search']['params']['maxResults']
		video_list = []
		req_url = cf['search_url'] + '&key=' + cf['key'] + '&' + urllib.urlencode({'q':cf['search']['terms']}) + '&order=date&' + '&'.join([item+'='+str(cf['search']['params'][item]) for item in cf['search']['params']])
		for i in range(0,iterations):
			try:
				videos = json.loads((requests.get(req_url)).text)

			except Exception as e:
				print "There was an error trying to fetch video list. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e)
				self.logger.error("Error trying to fetch data. This could be a transient error. Please try again. If the error persists please check the input parameters" + str(e))
				return# exit(1)

			for video in videos['items']:
				video_list.append(video['id']['videoId'])

			if videos.has_key('nextPageToken'):
				req_url = cf['search_url'] + '&token='+videos['nextPageToken']+'&key=' + cf['key'] + '&' + urllib.urlencode({'q':cf['search']['terms']}) + '&' + '&'.join([item+'='+str(cf['search']['params'][item]) for item in cf['search']['params']])
			else:
				break

		if os.path.exists(self.data_path):
			self.comment_ids = [line.split(',')[3].strip() for line in open(self.data_path,'rb').readlines()]
		else:
			self.comment_ids = []

		# print self.comment_ids[0]
		for video_id in video_list:
			print "Scanning video:" + video_id
			title = filter_printable(json.loads((requests.get("https://www.googleapis.com/youtube/v3/videos?part=snippet&key=AIzaSyCC5ieM4Sm1wH2by58N0ojfr91E4umcTPI&id="+video_id)).text)['items'][0]['snippet']['title'])
			print "Title: " + title
			self.logger.info("Scanning video:" + video_id)
			if all(True if t in title.lower() else False for t in cf['search']['terms'].split(" ")):
				self.fetch_top_level_comments(video_id)
				print "Fetched " + str(self.comment_count) + " comments from " + video_id + "\n"
				self.logger.info("Fetched " + str(self.comment_count) + " comments from " + video_id + "\n")
				total_comments_from_all_videos +=self.comment_count

		print "Total comments fetched from all videos: " + str(total_comments_from_all_videos)
		self.logger.info("Written " + str(total_comments_from_all_videos) + self.data_path)

if __name__ == '__main__':
	ytcd = YoutubeDownloader('conf/conf.json')
	ytcd.youtube_everything()
