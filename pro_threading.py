#!/usr/bin/env python
#coding:utf-8
__author__ = 'chenhuican'

'''
 多线程将指定目录下的文件导入MySQL

'''
import MySQLdb
import os
import sys
import glob
import time
import datetime
from time import ctime
from threading import Thread
from Queue import Queue


def listDir(path):
	file_list = []
	for filename in os.listdir(path):
		file_list.append(os.path.join(path, filename))
	return file_list

def readFile(file):
	alilines = []
	for line in open(file):
		alilines.append(line)
	return alilines


def writeLinestoDB(q, queue):
	sql = "insert into log(logline) values (%s)"
	while True:
		content = queue.get()
		conn = MySQLdb.connect(host="localhost",user="root",passwd="spark01234",db="test")
		cur = conn.cursor()
		cur.executemany(sql, content)
		#cur.close()
		conn.commit()
		conn.close()
		queue.task_done()

def main(dirs):
	print "Readfile start at: ",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	for files in glob.glob(os.path.join(dirs,"*.txt")):
		print('file is %s'%files)
		queue.put(readFile(files))
	
	print "Readfile Done at: ",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')	
	
	for q in range(num_thread):
		worker = Thread(target=writeLinestoDB, args=(q, queue))
		worker.setDaemon(True)
		worker.start()
	
	print 'insert into mysql Mian Thread at', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	queue.join()	
	print 'insert into mysql at', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
	num_thread = 2
	queue = Queue()
	dirs = '/root/www/tests02/data'
	print('------ starting at:', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'------\n')
	main(dirs)
	print('------ ending at:',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'------\n')
