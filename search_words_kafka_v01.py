#!/usr/bin/env python
#coding:utf-8
'''
	版本v1.0 
	搜索关键字排名
	日志格式：{"message": "type=searchwords,city_id=755,keyword=竹子林门店地址16xx,time=2018/10/22"}
	mail: huican.chen@360gst.com
'''
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pymysql
import MySQLdb
import time
import logging
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import sys
from pprint import pprint
reload(sys)
sys.setdefaultencoding('utf8')

#consumer = KafkaConsumer('log-topic',group_id='log_groups',bootstrap_servers="hadoop.node1:9092")

LOGS_DIR = 'logs'
logging.basicConfig(level=logging.INFO,
		format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
		datefmt= "%Y-%m-%d %H:%M:%S",
		filename=os.path.join(LOGS_DIR,'notice_item_logs.log'),
		filemode= 'a'
		)


def conn_mysql(hostip,user,passwd,database,port=3306):
	try:
		conn = MySQLdb.connect(hostip,user,passwd,database,port=port,charset="utf8")
		#conn=pymysql.connect(host=hostip,user=user,password=passwd,db=database,charset='utf8mb4')
	except Exception,e:
		print(str(e))
		send_mail("Conn_mysql error:<b>%s</b>"%str(e))

	return conn
	
def send_mail(contexts):
	msgroot = MIMEMultipart('related')
	msgtext = MIMEText(contexts, 'html','utf-8')
	msgroot.attach(msgtext)

	sender = 'techdata@360gst.com'
	receiver_list = ['huican.chen@360gst.com',
			'lihua.xiao@360gst.com',
			'haiwen.liang@360gst.com']

	msgroot['subject'] = u'[Error关键字搜索]-预约号关键字搜索异常'
	msgroot['From'] = sender
	msgroot['To'] = ";".join(receiver_list[:1])
	#msgroot['Cc'] = ";".join(receiver_list[1:])

	try:
		smtp = smtplib.SMTP_SSL('webmail.360gst.com', 465)
		smtp.login('techdata@360gst.com','xx8758ddf090HY21')
		smtp.sendmail(sender, receiver_list, msgroot.as_string())
		smtp.quit()
	except Exception,e:
		#print("Error:"+str(e))
		logging.error(str(e), exc_info=True)
	

def get_conn():
	hostip = ['10.26.0.133','11.23.12.23']
	#hostip = ['10.26.0.133','120.76.47.18'] #test
	user = 'xletl'
	passwd = ['gstdata@2016','Gstdata@2016']
	database = ['DbCenter','DbReport']
	try:
		db1 = conn_mysql(hostip[1], user, passwd[1], database[1])
		#db0 = conn_mysql(hostip[0], user, passwd[0], database[0])
	except Exception,e:
		print("[get_conn]获取连接失败：%s"%str(e))
	
	return db1

class kafka_consumer():
	def __init__(self, host, topic, groupid):
		self.host  = host
		self.topic = topic
		self.groupid = groupid

		self.consumer = KafkaConsumer(self.topic, 
					group_id=self.groupid,
					bootstrap_servers=self.host,
					client_id ='kafka-python-v1',
					#enable_auto_commit=False,
					auto_offset_reset='latest'
					)
	
	def consumer_msg(self):
		try:
			for msg in self.consumer:
				yield msg
		except KafkaError, e:
			logging.error(str(e))
		
if __name__ == "__main__":
	db = get_conn()
	cursor = db.cursor()	
	servers = ['hadoop.node1:9092','hadoop.node2:9092','hadoop.node3:9092']
	consumer = kafka_consumer(servers,'log-topic','log_groups')
	messages = consumer.consumer_msg()

	for msg in messages:
		ranking = 0
		msg_strs = str((msg.value).decode('utf8'))
		m = re.search(r'type=searchwords', msg_strs)
		#print(msg_strs)
		if m is not None:
			print("【Match:】"+msg_strs)
			logging.info("【Match:】"+msg_strs)
			try:
				key_dicts = json.loads(msg_strs)
				infos = key_dicts["message"]
				#pprint("【Searchinfo】"+str(info))
				info  = infos.split(",")
				ranking  += 1
				types = info[0].split('=')[-1]
				if types == 'searchwords':
					city_id  = info[1].split('=')[-1]
					key_word = str(info[2].split('=')[-1].encode('utf8'))
					row_key  = str(city_id)+'_'+str(key_word)
			except Exception, e:
				logging.error(str(e))

		if ranking != 0:
			sqls = "insert into F_SearchWords(Row_Key,City_Id,Key_Word,RanKing) values ('%s','%s','%s','%s') \
					ON DUPLICATE KEY UPDATE RanKing=RanKing+1" % \
					(row_key,city_id,key_word,ranking)

			print("\n"+str(sqls))

			try:
				db.ping(True)
				cursor.execute(sqls)
				db.commit()
			except Exception, e:
				db.rollback()
				send_mail("insert mysql error:%s \n, sqls=%s"%(str(e),str(sqls)))
			finally:
				if cursor.rowcount<0:
					if db:
						cursor.close()
						db.close()
					db = get_conn()
					cursor  = db.cursor()


				
	db.close()
