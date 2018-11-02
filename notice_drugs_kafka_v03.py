#!/usr/bin/env python
#coding:utf-8
'''
	版本v3.0 分仓版本
	Date: 2018/10/15 线上
	mail:huican.chen@360gst.com
'''
from kafka import KafkaConsumer
import pymysql
import MySQLdb
import time
import logging
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import sys
from pprint import pprint
reload(sys)
sys.setdefaultencoding('utf8')

consumer = KafkaConsumer('drugs-item',
			group_id='drugs_groups',
			bootstrap_servers="10.46.132.37:9092")

LOGS_DIR = 'logs'
logging.basicConfig(level=logging.INFO,
		format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
		datefmt= "%Y-%m-%d %H:%M:%S",
		filename=os.path.join(LOGS_DIR,'notice_item_logs.log'),
		filemode= 'a'
		)

def search_item(item_id,cur,shop_id=None):
	if item_id is not None and cur is not None:
		print("\n [function] [search_item] args item_id is: %d \n"%item_id)
		try:
			cur.execute("SELECT Fclinic_id,Fpresciption_units FROM Dim_Cloud_Item_Info WHERE Fitem_id=%d"%int(item_id))
			units = cur.fetchone()
			shop_names = None
			if shop_id is not None:
				cur.execute("select DISTINCT Shop_Short_Name from Dim_Shop where Id=%d"%int(shop_id))
				shop_names = cur.fetchone()
			return units,shop_names[0] if shop_names is not None else ''
		except Exception,e:
			print(str(e))
			logging.error('Failed to search item_id unit.', exc_info=True)
			cur.close()
	
	else:
		return ['','']
	

def conn_mysql(hostip,user,passwd,database,port=3306):
	try:
		conn = MySQLdb.connect(hostip,user,passwd,database,port=port,charset="utf8")
		#conn=pymysql.connect(host=hostip,user=user,password=passwd,db=database,charset='utf8mb4')
	except Exception,e:
		print(str(e))
		send_mail("Conn_mysql error:<b>%s</b>"%str(e))

	return conn
	

def del_data(db, cur):
	dt = time.strftime("%H%M%S", time.localtime())
	if str(dt) == '060000':
		try:
			cur.execute("truncate table F_LackDrugsNotice")
			db.commit()
		except Exception,e:
			db.rollback()
			logging.error(str(e), exc_info=True)

def send_mail(contexts):
	msgroot = MIMEMultipart('related')
	msgtext = MIMEText(contexts, 'html','utf-8')
	msgroot.attach(msgtext)

	sender = 'techdata@360gst.com'
	receiver_list = ['huican.chen@360gst.com',
			'lihua.xiao@360gst.com',
			'haiwen.liang@360gst.com']

	msgroot['subject'] = u'[Error云药房]-区仓版诊中缺药运算异常'
	msgroot['From'] = sender
	msgroot['To'] = ";".join(receiver_list[:1])
	#msgroot['Cc'] = ";".join(receiver_list[1:])

	try:
		smtp = smtplib.SMTP_SSL('webmail.360gst.com', 465)
		smtp.login('techdata@360gst.com','ddtrataS&*1')
		smtp.sendmail(sender, receiver_list, msgroot.as_string())
		smtp.quit()
	except Exception,e:
		#print("Error:"+str(e))
		logging.error(str(e), exc_info=True)
	

def get_conn():
	hostip = ['10.26.0.133','10.29.206.42']
	user = 'xletl'
	passwd = ['gstdata@2016','Gstdadfta@2016']
	database = ['DbCenter','DbReport']
	try:
		db1 = conn_mysql(hostip[1], user, passwd[1], database[1])
		db0 = conn_mysql(hostip[0], user, passwd[0], database[0])
	except Exception,e:
		print("[get_conn]获取连接失败：%s"%str(e))
	
	return db1,db0


if __name__ == "__main__":

	db, db0 = get_conn()

	cursor = db.cursor()	
	cursor0 = db0.cursor()

	for msg in consumer:
		notice_num = 0
		msg_strs = str((msg.value).decode('utf8'))
		m = re.search('deal', msg_strs)

		#del_data(db, cursor)

		#print(msg_strs)
		if m is not None:
			print("【Match:】"+(msg.value).decode('utf8'))
			if re.search(r'deal.*Response.*原门店id.+门店id.+药房:',msg_strs):
				print("【收费站Match:】"+(msg.value).decode('utf8'))
				try:
					info  = msg_strs.split("Response:")[-1].split(',')
					ori_shop_id = int(info[0].split(':')[-1].strip('[').strip(']'))
					source_id   = 1  #"收费站"
					shop_id     = int(info[1].split(':')[-1].strip('[').strip(']'))
					pharmacy_id = str(info[2].split(':')[-1].strip('[').strip(']'))
					shop_name   = str(info[3].split(':')[-1].strip('[').strip(']'))
					pharmacy_name = str(info[4].split(':')[-1].strip('[').strip(']').encode("utf-8"))
					item_name   = str(info[5].split(':')[-1].strip('[').strip(']').encode("utf-8"))
					item_id     = int(info[6].split(':')[-1].strip('[').strip(']'))
					item_code   = str(info[7].split(':')[-1].strip('[').strip(']'))
					current_stock = str(info[8].split(':')[-1].strip('[').strip(']'))
					need_stock  = str(info[9].split(':')[-1].strip('[').strip(']"')) 
					if re.findall('\d+\.?\d+',need_stock):
						need_stock  = str(re.findall('\d+\.?\d+',need_stock)[0])
					notice_num  += 1
					#row_key     = str(shop_id)+str(source_id)+str(pharmacy_id)+item_code

					f_results,ori_shop_name  = search_item(item_id,cursor0,ori_shop_id)
					
					if ori_shop_id == shop_id:
						sys_flag = 0
					else:
						sys_flag = 1

					if f_results is not None:
						small_unit = str(f_results[1])
					else:
						small_unit = ''
					
					row_key = str(shop_id)+str(source_id)+str(sys_flag)+str(pharmacy_id)+item_code

				except Exception, e:
					logging.error(str(e))

		else:
			#print("\n"+(msg.value).decode('utf8'))
			if re.search(r'门店ID', msg_strs):
				#print("\n 【医生站Match:】"+(msg.value).decode('utf8'))
				try:
					info = msg_strs.split("Response:")[-1].split()
					shop_name = str(info[0].split(":")[-1].encode("utf-8"))
					shop_id   = int(info[1].split(":")[-1])
					ori_shop_id = shop_id
					ori_shop_name = ''
					pharmacy_id = ''
					pharmacy_name = ''
					sys_flag  = 0
					source_id = 2  #"医生站"
					item_name   = str(info[2].split(",")[0].split(":")[1].replace('[','').replace(']','').encode("utf-8"))
					item_code   = str(info[2].split(",")[1].split(":")[1].replace('[','').replace(']','').encode("utf-8"))
					item_id     = int(info[3].split(':')[1].strip('[').strip(']'))
					current_stock  = str(info[5].split(u"：")[1].strip('[').strip(']'))
					need_stock  = str(info[6].split(u"：")[1].strip('[').strip(']"}'))
					notice_num  += 1
					row_key     = str(shop_id)+str(source_id)+item_code 	
					
					f_results  = search_item(item_id,cursor0)
					if f_results is not None:
						small_unit = str(f_results[1])
					else:
						small_unit = ''

				except Exception, e:
					logging.error(str(e))
		
		if notice_num != 0:
			sqls = "insert into F_LackDrugsNotice(rowkey,ori_shop_id,ori_shop_name,shop_id,shop_name,\
					source_id,item_code,item_name,small_unit,item_id, \
					pharmacy_id,pharmacy_name,current_stock,need_stock,notice_num,sys_flag) \
					values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') \
					ON DUPLICATE KEY UPDATE notice_num=notice_num+1,current_stock=%s" % \
				(row_key,ori_shop_id,ori_shop_name,shop_id,shop_name,source_id,item_code,item_name,small_unit,item_id,
						pharmacy_id,pharmacy_name,current_stock,need_stock,notice_num,sys_flag,current_stock)

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
						db.close()
					if db0:
						db0.close()
					db, db0 = get_conn()
					cursor  = db.cursor()
					cursor0 = db0.cursor()


				
	db.close()
	db0.close()

