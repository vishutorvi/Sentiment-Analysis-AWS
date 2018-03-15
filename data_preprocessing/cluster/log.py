import time
class Logger:
	def __init__(self,file_path):
		self.file = open(file_path,'a')
	def now(self):
		return time.strftime("%d/%m/%Y %H:%M:%S")
	def info(self, msg):
		self.file.write(self.now() + " [INFO] " + str(msg) + "\n")
	def error(self, msg):
		self.file.write(self.now() + " [ERROR] " + str(msg) + "\n")