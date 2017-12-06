#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
库存信息的日志统计
'''
import subprocess
import time
from datetime import date, timedelta
import subprocess
import __main__
import smtplib
import sys  
import xlwt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.Header import Header
import MySQLdb
import _mysql
from inspect import currentframe
from warnings import filterwarnings
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os, sys

g_cur_date  = (date.today() + timedelta(0))


(recipe0_db_host,recipe0_db_port,recipe0_db_user,recipe0_db_passwd,recipe0_db_name)=("110.41.245.182", 3306, "user", "password", "clinic_management")

def init_db():
    try:
        recipe_conn_list = []
        recipe_conn_list.append(MySQLdb.connect(
            host    = recipe0_db_host,
            user    = recipe0_db_user,
            passwd  = recipe0_db_passwd,
            port    = recipe0_db_port,
            db      = recipe0_db_name,
            charset = 'utf8'))
 
        return recipe_conn_list
    except MySQLdb.Error,e:
        raise RuntimeError(" Mysql Error. [%d: %s]" % ( e.args[0], e.args[1]))
    except RuntimeError,e:
        if recipe_conn_list[0]:
             recipe_conn_list[0].close()
        raise RuntimeError(e.args[0])
    except:
        if recipe_conn_list[0]:
             recipe_conn_list[0].close()
        raise RuntimeError("Ln:[%d] Uncaught exception. line:[%s], desc:[%s]" % (lineno(), sys.exc_info()[2].tb_lineno, (",".join(str(x) for x in (sys.exc_info())))))

def close_connection(recipe_conn_list):
    for conn in recipe_conn_list:
        if conn:
            conn.close()
   
def get_shop_name(recipe_conn_list,shop_no):
		try:
				recipe_conn  = recipe_conn_list[0]

				recipe_sql   = ("SELECT Fshop_nick_name FROM t_shop where Fshop_no= %d" % (shop_no))

				print recipe_sql

				shop_name = ""

				recipe_conn.query(recipe_sql)
				recipe_rows = recipe_conn.store_result().fetch_row(maxrows=0)

				for row in recipe_rows:
						if not row:
								continue

						shop_name   = row[0]

				return shop_name    

		except MySQLdb.Error,e:
				raise RuntimeError("Mysql Error. [%d: %s]" % ( e.args[0], e.args[1]))
		except RuntimeError,e:
				close_connection(recipe_conn_list)
				raise RuntimeError(e.args[0])
		except:
				close_connection(recipe_conn_list)
				raise RuntimeError("Uncaught exception. line:[%s], desc:[%s]" % (sys.exc_info()[2].tb_lineno, (",".join(str(x) for x in (sys.exc_info())))))  
	

def grep():
   
   findstr = os.popen('grep 项目不能使用 /home/item_not_can_use/recipecache_debug0*.log |grep -w UpdateItem').read()
   
   return findstr
       
    
def send_mail(statistic_result,file_name):
		sender        = 'niu.niu@qq.com'
		reciever_list = ['xx.niu@qq.com','bb.niu@qq.com','cc.niu@qq.com','dd.niu@qq.com','ee.niu@qq.com','ff.niu@qq.com','gg.niu@qq.com']
		to            = str(reciever_list)

		message = MIMEMultipart('alternative')
		message['Subject'] = Header("[库存信息的日志统计] %s" % g_cur_date.strftime('%Y_%m_%d'), 'utf-8')
		message['From'] = sender
		message['To'] = to

		#构造附件1
		att1 = MIMEText(open(file_name, 'rb').read(), 'base64', 'utf-8')
		att1["Content-Type"] = 'application/octet-stream'
		att1["Content-Disposition"] = 'attachment; filename=%s' %(file_name)
		message.attach(att1)

		content_html = MIMEText(render_result.encode('UTF-8'), 'html', 'UTF-8')
		content_html["Accept-Language"] = "zh-CN"
		content_html["Accept-Charset"]="ISO-8859-1,UTF-8"
		message.attach(content_html)
		client = smtplib.SMTP_SSL('webmail.qq.com:465')
		client.login('niu.niu@qq.com','37651677')
		client.sendmail(sender, reciever_list, message.as_string())
		client.quit()
                    																																			
if __name__ == "__main__":
	
		recipe_conn_list = init_db()
	
		os.popen('cp /data/c2c_logs/recipecache/recipecache_debug0.log /home/item_not_can_use/')	
		
		strFind =  grep()
		
		if strFind == "":
				exit()
					
		str_line = strFind.split("\n")
		
		line = 1
		
		item_list = {}
		
		workbook = xlwt.Workbook(encoding = 'utf-8') 
			
		sheet = workbook.add_sheet("book")
		
		sheet.write(0, 0, '处方id')
		sheet.write(0, 1, '门店id')
		sheet.write(0, 2, '门店名称')
		sheet.write(0, 3, '项目id')
		sheet.write(0, 4, '项目名称')
		sheet.write(0, 5, '备注')

		
		sheet.col(0).width=156*20
		sheet.col(1).width=200*20
		sheet.col(2).width=350*20
		sheet.col(3).width=256*20
		sheet.col(4).width=450*20
		sheet.col(5).width=350*20
		
		for str_line in str_line:		
				str_registerId = str_line.split("ddwRegisterId:")
				
				if len(str_registerId) < 2:
						continue
				
				str_registerId = str_registerId[1].split("]")
				
				str_shop = str_line.split("ddwClinicId:")
				
				str_shop = str_shop[1].split("]")
				
				str_item_name = str_line.split("Error:[")
				
				str_item_name = str_item_name[1].split("项目")
				
				str_item_id = str_line.split("ddwItemId:")
				
				str_item_id = str_item_id[1].split("]")
				
				strShopName = get_shop_name(recipe_conn_list,int(str_shop[0]))
				
				item_key = ("%s_%s" %(str_shop[0],str_item_id[0]))
				
				print item_key
				
				if item_key in item_list.keys():
						use_num = int(item_list[item_key]['use_num']) + 1
						item_list[item_key] = {
						'str_item_id'       : str_item_id[0],
						'str_shop_no'       : str_shop[0],
						'str_item_name'     : str_item_name[0],
						'str_registerId'     : str_registerId[0],
						'str_shop_name'       : strShopName,
						'str_remarks'			    :'禁用、缺少库存或者价格为零',
						'use_num'			    :use_num,	
						}
				else:
						item_list[item_key] = {
						'str_item_id'       : str_item_id[0],
						'str_shop_no'       : str_shop[0],
						'str_item_name'     : str_item_name[0],
						'str_registerId'     : str_registerId[0],
						'str_shop_name'       : strShopName,
						'str_remarks'			    :'禁用、缺少库存或者价格为零',
						'use_num'			    :1,	
						}	
						
				sheet.write(line, 0, str_registerId[0])
				sheet.write(line, 1, str_shop[0])				
				sheet.write(line, 2, strShopName)
				sheet.write(line, 3, str_item_id[0])
				sheet.write(line, 4, str_item_name[0])
				sheet.write(line, 5, '禁用、缺少库存或者价格为零')				
	
				line = line + 1
		
		
		value_list = list(item_list.values())
		
		value_list.sort(key=lambda x: x['use_num'], reverse=True)  
		
		#print value_list
		#print reversed(sorted(item_list.items()))
		#list_sort =  sorted(item_list)
		#print list_sort
		#print item_list
		#a = {'a': {'b':  'China'}, 'c': {'d': 'USA'}, 'b': {'c': 'Russia'}, 'd': {'a': 'Canada'}}  
		#b = sorted(item_list.items(), key=lambda x: x[1][6], reverse=True) 
			
		#print b	
			
		#print(dict)
    	
		file_fd = open('/home/item_not_can_use/report_contrast.html', 'r')
		template_file = file_fd.read()
		template = Template(template_file.decode('UTF8'))
		render_result = template.render(contrast_list=value_list)
		file_fd.close()
		
		file_name = ("库存不足的日志统计_%s.xls" % g_cur_date.strftime('%Y_%m_%d'))						
		workbook.save(file_name)

		send_mail(render_result,file_name)
		
		close_connection(recipe_conn_list)
			
		
		
	
