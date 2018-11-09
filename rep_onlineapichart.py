#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'chenhuican'
'''后台接口性能监控数据日报
   Mail <huican.chen@360gst.com>
'''
import os
import mimetypes,sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import datetime
import MySQLdb
import plotly
import plotly.graph_objs
import smtplib
import xlwt
import random
from jinja2 import Template,Environment,FileSystemLoader,PackageLoader
from pprint import pprint
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.MIMEBase import MIMEBase
from email import Utils,Encoders
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import pylab
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
from matplotlib.dates import AutoDateLocator, DateFormatter,DayLocator,HourLocator
from matplotlib.pyplot import plot,savefig
from matplotlib.ticker import MultipleLocator,FuncFormatter,FormatStrFormatter
from matplotlib.font_manager import FontProperties
import config
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
# 上面是防止绘制的图片中文乱码

def  conMysql(DB_HOST,DB_USER,DB_PASS,DB_NAME):
	try:
		db = MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASS,db=DB_NAME,port=3306,charset="utf8") 
		#加cursorclass = MySQLdb.cursors.DictCursor 返回字典
	except MySQLdb.Error, e:
		try:
			sqlError = "Error %d:%s" % (e.args[0],e.args[1])
		except IndexError:
			print "MySQL Error:%s" % str(e)
	return db

def closeconn(dbs):
	if dbs:
		dbs.close()

#jinjia2 自定义测试器
def splitstr(str1):
	str2 = str1.split("(")[1].split("%")[0]
	if str2 > '99.00':
		return True
	else:
		return False

#创建excel文件
def createsheet(db,filesname,startime,endtime,dirs):
	results1   = []
	try:
		cursor = db.cursor()
		sqls1 = '''
			SELECT  @rowno:=@rowno+1                         AS  '序号',
				DATE_FORMAT(A.created_date,'%%Y-%%m-%%d') AS '统计日期',
				A.server_url                             AS '服务名(DAO)',
				A.api_url                                AS 'api名称(命令字)',
				A.call_nums                              AS '调用次数',
				A.succ_nums                              AS '成功返回次数',
				A.fail_nums                              AS '失败返回次数',
				A.ms100_nums                             AS '0-100ms返回次数',
				CONCAT(A.ms100_per,'%%')                  AS '0-100ms返回次数/占比',
				A.ms200_nums                             AS '100-200ms返回次数',
				CONCAT(A.ms200_per,'%%')                  AS '100-200ms返回次数/占比',
				A.ms500_nums                             AS '300-500ms返回次数',
				CONCAT(A.ms500_per,'%%')                  AS '300-500ms返回次数/占比',
				A.m2s_nums                               AS '500ms-2s返回次数',
				CONCAT(A.m2s_per,'%%')                   AS '500ms-2s返回次数/占比',
				A.mup2_nums                              AS '2s以上返回次数',
				CONCAT(A.mup2_per,'%%')                  AS '2s以上返回次数/占比',
				A.avg_times                              AS '接口平均访问时长(s)'
			FROM Rep_Online_Api_Day A, (SELECT @rowno:=0) B
			WHERE A.created_date>='%s' AND A.created_date<='%s'
		''' % (startime,endtime)
		cursor.execute(sqls1)
		results1 = cursor.fetchall()
	except MySQLdb.Error, e:
		try:
			sqlError = "Error %d:%s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s 获取Excel数据失败！" %  (str(e))
	#os.chdir('/root/report_mail/online_api/excel') #切换目录，crontab 下执行，会生成文件到,root目录
	columnName = [u'序号',
		      u'统计日期',
		      u'服务名(DAO)',
		      u'api名称(命令字)',
		      u'调用次数',
		      u'成功返回次数',
		      u'失败返回次数',
		      u'0-100ms返回次数',
		      u'0-100ms返回次数占比',
		      u'100-200ms返回次数',
		      u'100-200ms返回次数占比',
		      u'300-500ms返回次数',
		      u'300-500ms返回次数占比',
		      u'500ms-2s返回次数',
		      u'500ms-2s返回次数占比',
		      u'2s以上返回次数',
		      u'2s以上返回次数占比',
		      u'接口平均访问时长']
	wb = xlwt.Workbook(encoding='utf-8')
	sheetlist = wb.add_sheet(u'后台接口服务监控数据',cell_overwrite_ok=True)
	columns = len(columnName)
	rownums = len(results1)
	#设置表格样式
	style = xlwt.XFStyle()
	font = xlwt.Font()
	font.name = 'SimHei'
	style.font = font
	alignment = xlwt.Alignment()
	alignment.horz = xlwt.Alignment.HORZ_CENTER
	style.alignment = alignment
	style1 = xlwt.easyxf('pattern: pattern solid, fore_colour sky_blue; font: bold on;')
	style2 = xlwt.easyxf('pattern: pattern solid, fore_colour light_green ; font: bold on;')
	for i in range(columns):
		sheetlist.col(i).width = 3000  #设置sheet
	for j in range(columns):
		sheetlist.write(0,j,columnName[j],style1)     #将表头数据插入表格
		# 写sheet单元格
	for i in range(1,rownums):
		for j in range(columns):
			if results1[i][8] > '99.00%':
				sheetlist.write(i,j,results1[i][j],style2)
			else:
				sheetlist.write(i,j,results1[i][j],style)
	file_name= os.path.join(dirs,endtime+filesname)
	wb.save(file_name)
	return file_name
	
#绘制并列柱状图
def drawbarcharts(db,endtime):
	xdatas   = [u'调用总次数',
		    u'成功返回次数',
		    u'失败返回次数',
		    u'0-100ms返回次数',
		    u'100-200ms返回次数',
		    u'300-500ms返回次数',
		    u'500ms-2s返回次数',
		    u'2s以上返回次数'
		   ]
	apidatas = '' 
	try:
		#cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor) 查询返回字典
		cursor = db.cursor()
		sqls = '''
			SELECT 
				CONCAT_WS('.',A.server_url,A.api_url) as server_url,
				A.call_nums   AS  '调用总次数',
				A.succ_nums   AS  '成功返回次数',
				A.fail_nums   AS  '失败返回次数',
				A.ms100_nums  AS  '0-100ms返回次数',
				A.ms200_nums  AS  '100-200ms返回次数',
				A.ms500_nums  AS  '300-500ms返回次数',
				A.m2s_nums    AS  '500ms-2s返回次数',
				A.mup2_nums   AS  '2s以上返回次数'
			FROM Rep_Online_Api_Day A
			WHERE A.server_url='dao_pay_center' AND A.created_date='%s' 
			''' % endtime
		cursor.execute(sqls)
		apidatas = cursor.fetchall()
	except MySQLdb.Error, e:
		try:
			sqlError = "Error %d:%s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s" %  (str(e))
	
	#横坐标 为 call_nums，succ_nums，fail_nums，ms100_nums，ms200_nums，ms500_nums，m2s_nums，mup2_nums
	idx = np.arange(len(apidatas[0])-1)
	#设置每个坐标总宽度total_width, 计算总的柱状图格式 nums
	total_width,nums = 1.2,len(apidatas[0])-1
	width = total_width/nums
	font01 = FontProperties(fname=r"/usr/share/fonts/chinese/simhei.ttf",size=9)
	fig = plt.figure(1, figsize=(14,6))
	ax  = fig.add_subplot(111)
	colors = ['b','r','g','y','m','c']
	#绘制多条
	for i in range(len(apidatas)):
		ydatas = []
		for j in range(1,len(apidatas[0])):
			ydatas.append(apidatas[i][j])
		labels=apidatas[i][0]
		#xdatas.append(apidatas[i][0])
		if j==1:
			ax.bar(idx,ydatas,width,color=colors[i],label=labels)
		else:
			ax.bar(idx+i*width,ydatas,width,color=colors[i],label=labels)
	ax.set_xticks(idx+width)
	ax.set_xticklabels(xdatas,fontproperties=font01)
	ax.set_ylabel(u'请求次数',fontproperties=font01)
	ax.set_xlabel(u'调用请求次数分布',fontproperties=font01)
	ax.set_title(u'pay_center接口调用情况图',fontproperties=font01)
	fig.autofmt_xdate()
	#显示图例
	ax.legend(prop=font01)
	plt.savefig('/root/report_mail/online_api/barapibar.png',format='png')
	return os.path.abspath('/root/report_mail/online_api/barapibar.png')

#生成表格头部
def generateheader(results):
	headerlist = []
	#传过来的 results 类型为  <type 'tuple'>,转换成list 可以得到字段名称
	#retlis     = list(results)
	#for i in results[0].keys():
	#	headerlist.append(i)
	headerlist=['序号',
		    '统计日期',
		    '服务名(DAO)',
	            '接口名(命令字)',
		    '调用总次数',
		    '成功返回次数',
		    '失败返回次数',
	            '0-100ms返回次数/占比',
		    '100-200ms返回次数/占比',
		    '300-500ms返回次数/占比',
		    '500ms-2s返回次数/占比',
		    '2s以上返回次数/占比',
		    '接口平均访问时长(ms)']
	return headerlist

def sendMail(render_result,imgurls):
	pass

#创建附件函数
def createAttachment(filename):
	fd = file(filename,"rb")
	mimetype,mimeencoding = mimetypes.guess_type(filename)
	if mimeencoding or (mimetype is None):
		mimetype = "application/octet-stream"
	maintype,subtype = mimetype.split("/")
	if maintype == "text":
		retval = MIMEText(fd.read(),_subtype = subtype)
	else:
		retval = MIMEBase(maintype,subtype)
		retval.set_payload(fd.read())
		Encoders.encode_base64(retval)
	retval.add_header("Content-Disposition","attachment",filename = filename)
	fd.close()
	return retval

#生成邮件正文内容
def generatedata(db,startime,endtime):
	sql ='''
		SELECT  @rowno:=@rowno+1 AS  '序号',
			DATE_FORMAT(A.created_date,'%%Y-%%m-%%d') AS '统计日期',
			A.server_url                             AS '服务名(DAO)',
			A.api_url                                AS 'api名称(命令字)',
			A.call_nums                              AS '调用次数',
			A.succ_nums                              AS '成功返回次数',
			A.fail_nums                              AS '失败返回次数',
			CONCAT(A.ms100_nums,'/(',A.ms100_per,'%%)')  AS '0-100ms返回次数/占比',
			CONCAT(A.ms200_nums,'/(',A.ms200_per,'%%)')  AS '100-200ms返回次数/占比',
			CONCAT(A.ms500_nums,'/(',A.ms500_per,'%%)')  AS '300-500ms返回次数/占比',
			CONCAT(A.m2s_nums,'/(',A.m2s_per,'%%)')      AS '500ms-2s返回次数/占比',
			CONCAT(A.mup2_nums,'/(',A.mup2_per,'%%)')    AS '2s以上返回次数/占比',
			A.avg_times                                  AS '接口平均访问时长'
		FROM Rep_Online_Api_Day A, (SELECT @rowno:=0) B
		WHERE A.created_date>='%s' AND A.created_date<='%s' 
		''' % (startime,endtime)

	try:
		#cursor   = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
		cursor	 = db.cursor()
		cursor.execute(sql)
		results  = cursor.fetchall()
		ncols    = len(results[0])
		nrows    = len(results)
	#	pprint(results)
	except MySQLdb.Error, e:
		print "Error: %d: %s" % (e.args[0], e.args[1])
	#db.close()
	return results,ncols,nrows

def addimg(src,imageid):
	fp = open(src, 'rb')
	msgImage = MIMEImage(fp.read())
	fp.close()
	msgImage.add_header('Content-ID',imageid)
	return msgImage

if __name__ == "__main__":
	DB_HOST	   = config.mysql_hostip
	DB_USER    = config.mysql_user
	DB_PASS    = config.mysql_passwd
	DB_NAME    = config.database
	#env = Environment(loader=FileSystemLoader('/root/report_mail/online_api',followlinks=True))
	env = Environment(loader=PackageLoader(__name__,'templates'))	
	#env = Environment()
	#把测试器注册到模板环境里
	env.filters['splitstr'] = splitstr
	template = env.get_template('api_report.html')
	sender     = config.mail_name
	receiver_list = ['huican.chen@360gst.com',
			 'huican.chen@360gst.com']
	#receiver_list = ['1319193932@qq.com','1148028509@qq.com','huican.chen@360gst.com','1148028509@qq.com']
	rev_list = receiver_list[:2]
	cc_list  = receiver_list[2:]
	subject    = u'[报表]-后台接口性能监控数据日报[系统邮件]'
	smtpserver = config.smtpserver
	username   = config.mail_name
	password   = config.mail_password
	imgurls,imgurls2    = '',''
	stime      = datetime.date.today() - datetime.timedelta(days = 1)
	etime      = datetime.date.today() - datetime.timedelta(days = 1)
	startime   = stime.strftime('%Y-%m-%d')
	endtime    = etime.strftime('%Y-%m-%d')
	print startime,endtime
	
	#转换目录，因为在crontab 定时任务时，生成的文件会默认放到了root目录
	os.chdir(os.path.dirname(__file__))
	dbs = conMysql(DB_HOST,DB_USER,DB_PASS,DB_NAME) 
	
	#写excel文件
	dirs = '/root/report_mail/online_api/excel'
	filesname = u'后台接口性能监控数据报表.xls'
	file_name = createsheet(dbs,filesname,startime,endtime,dirs)
	
	#绘制柱状图曲线图
	imgurls2 = drawbarcharts(dbs,endtime)

	#生成邮件正文
	results_list,ncols,nrows = generatedata(dbs,startime,endtime)
	print 'results_list types is ',type(results_list)

	#统一关闭数据库连接
	closeconn(dbs)
	#生成头部与表格内容
	header_list = generateheader(results_list)
	
	#print(header_list)
	# 带- 的注释代表可用,暂注释 ，模板的加载采用了 jinjia2 get_template方法，为了 加载 自定义测试器 2017/05/23
	#-file_fd = open('./api_report.html', 'r')
	#-template_file = file_fd.read()
	#-template = Template(template_file.decode('UTF8'))
	#-template = env.get_template('api_report.html')
	# 模板文件 templates/api_report.html
	render_result = template.render(deader_list=header_list,bodylists=results_list,ncols=ncols,nrows=nrows)
	#-file_fd.close()
	msgRoot = MIMEMultipart('related')
	msgText = MIMEText(render_result,'html','utf-8')
	msgRoot.attach(msgText)
	msgRoot.attach(addimg(imgurls2,'image2'))
	
	#带附件
	path = r'/root/report_mail/online_api/excel'
	basename = file_name.decode("utf-8")
	print 'basename is: ', basename
	attach = MIMEText(open(os.path.join(path, file_name),'rb').read(),'base64','gb2312')
	attach['Content-Type'] = 'application/octet-stream'
	attach["Content-Disposition"] = 'attachment; filename=%s' % os.path.basename(file_name).encode("gb2312")
	msgRoot.attach(attach)

	msgRoot['Subject'] = subject
	msgRoot['From'] = sender
	msgRoot['To']   = ";".join(rev_list)
	msgRoot['CC']   = ";".join(cc_list)
	try:
		#smtp = smtplib.SMTP()
		smtp = smtplib.SMTP_SSL('webmail.360gst.com',465)
		smtp.login(username, password)
		smtp.sendmail(sender, receiver_list, msgRoot.as_string())
		smtp.quit()
	except Exception, e:
		print "Send Email Error:"+str(e)
