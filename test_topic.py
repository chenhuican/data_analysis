#!/usr/bin/env python
#coding:utf8
'''
  kafka消息处理
  author: huican.chen
'''
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')


#consumer = KafkaConsumer('test-canal',group_id="test_group",bootstrap_servers="hadoop.node1:9092")

class kafka_consumer():
	
	def __init__(self, host, port, topic, group_id):
		self.topic     = topic
		self.groupid   = group_id
		self.host      = host
		self.port      = port
		self.consumer  = KafkaConsumer(self.topic,group_id=self.groupid,bootstrap_servers="{kf_host}:{kf_port}".format(kf_host = self.host,kf_port=self.port))

		self.producer  = KafkaProducer(bootstrap_servers='{host}:{port}'.format(host=self.host,port=self.port))
	
	def consume_data(self):
		try:
			for message in self.consumer:
				yield message
		except KeyboardInterrupt, e:
			print(e)

	
	def send_message(self,datas):
		try:
			self.parmas_msg = json.dumps(datas)
			producer	= self.producer
			producer.send(self.topic, self.parmas_msg.encode('utf-8'))
			producer.flush()
			print('send messages successful !')
		except KafkaError as e:
			print(str(e))

 

if __name__ == "__main__":
	consumer = kafka_consumer('hadoop.node1',9092,'log-topic','log_groups')
	#consumer = kafka_consumer("hadoop.node1", 9092,"test-canal","test_group")
	messages = consumer.consume_data()

	
	try:
		for msg in messages:
			recv = "[topic]:%s [partition]:%d :[offset]%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value.decode('utf8'))
			msg_strs = str(recv)
			#infos = json.loads(msg.value.decode('utf8'))
			print(msg_strs+'\n')
			#print(infos["searchinfo"]+'\n')
	except KafkaError,e:
		print(str(e))

