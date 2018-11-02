#coding:utf-8
'''
  nginx 日志模拟
  author: huican.chen

'''
import random
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

url_paths = [
	"class/112.html",
	"class/128.html",
	"class/145.html",
	"class/146.html",
	"class/131.html",
	"class/130.html",
	"learn/821",
	"course/list"
]

ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

http_referers = [
	"http://www.baidu.com/s?wd={query}",
	"http://www.sogou.com/web?query={query}",
	"http://cn.bing.com/search?q={query}",
	"https://search.yahoo.com/search?p={query}",
]

search_keyword = [
	"东山门店",
	"竹子林门店地址",
	"福州西湖门店预约",
	"固生堂中医养生",
	"大数据实战",
	"固生堂中医医院专家",
	"GST中医理疗"
]

status_codes = ["200","404","500"]

def sample_url():
	return random.sample(url_paths, 1)[0]

def sample_ip():
	slice = random.sample(ip_slices, 4)
	return ".".join(str(i) for i in slice)


def sample_referer():
	if random.uniform(0, 1) > 0.2:
		return "-"
	refer_str = random.sample(http_referers, 1)
	query_str = random.sample(search_keyword, 1)
	return refer_str[0].format(query=query_str[0])

def sample_status_code():
	return random.sample(status_codes, 1)[0]

def generate_log(count):
	time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	#f = open("/root/logs/access_log.log", "a+")
	with open("/root/logs/access_log.log", 'a') as f:
		while count >= 1:
			time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
			query_log = "{ip}\t{times}\t\'GET /{url} HTTP/1.1'\t{status_codes}\t{referer}".format(ip=sample_ip(),times=time_str, \
				url=sample_url(), \
				status_codes=sample_status_code(), \
				referer=sample_referer())
			count = count - 1
			f.write(query_log+"\n")
			f.flush()
			print(query_log)
			time.sleep(1)


def generate_msg():
	time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	query_log = "{ip},{times},\'GET /{url} HTTP/1.1',{status_codes},{referer}\n".format(ip=sample_ip(),times=time_str, \
			url=sample_url(), \
			status_codes=sample_status_code(), \
			referer=sample_referer())
	
	time.sleep(2)
	#return json.dumps(query_log)
	return query_log

class kafka_producer():
	
	def __init__(self, host, port, topic):
		self.host  = host
		self.port  = port
		self.topic = topic

		self.producer = KafkaProducer(bootstrap_servers='{kf_host}:{kf_port}'.format(kf_host=self.host, kf_port=self.port),
					value_serializer=lambda v: json.dumps(v).encode('utf-8'))

	def send_message(self, msg):
		#print(json.loads(msg))
		#self.parmas_msg = json.dumps(msg)
		try:
			producer = self.producer
			producer.send(self.topic, msg)
			producer.flush()
		
		except KafkaError, e:
			print(str(e))
		

if __name__ == "__main__":
	count = 100000
	#test-canal
	producer = kafka_producer("hadoop.node1", 9092, "test-topic")

	'''
	try:
		generate_log(count)
	except KeyboardInterrupt, e:
		print(str(e))
	'''
	
	for _ in range(count):
		msg = generate_msg()
		print msg
		producer.send_message(str(msg))

