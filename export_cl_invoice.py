#!/usr/bin/env python
#coding:utf-8
import os
import openpyxl
import threading
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import MySQLdb

'''
	财务2015-2016年发票数据导出
'''

def excel_export(engine, shop_id, start_date, end_date):
	sqls = '''
		SELECT
		  A.Invo_No     AS   '发票号',
		  C.Chrg_No     AS   '结算号',
		  A.Shop_Id,
		 (SELECT C1.Shop_Short_Name FROM Dim_Shop C1 WHERE C1.Id=A.Shop_Id)       AS  '门店',
		  A.Chrg_Date    AS  '收费日期',
		  A.Invo_Date    AS  '发票日期',
		  B.Oper_Name    AS  '收银员',
		  D.Fee_CodeName AS  '费别',
		  A.Acct_Sum     AS  '记账金额',
		  A.Sb_Sum       AS  '自负金额', 
		  A.Sp_Sum       AS  '自费金额'
	      from Dim_CL_Invoice A
	      join Dict_Code_Operator B on A.Shop_Id=B.Shop_Id AND A.Oper_Code=B.Oper_Code
	      join Dim_CL_Charge_Invoice C on A.Shop_Id=C.Shop_Id AND A.Invo_No=C.Invo_No AND A.HType=C.HType
	      join Dim_CL_Charge D ON C.Shop_Id=D.Shop_Id AND C.Chrg_No=D.Chrg_No AND D.STATUS='1'
	      where A.Shop_Id=%d
		      and A.Status<>3
		      and A.Chrg_Date>='%s'
		      and A.Chrg_Date<='%s'
	'''%(shop_id, start_date, end_date)

	print(sqls)
	df1 = pd.read_sql(sqls,engine)
	#df1.rename(columns={'Shop_Name':'门店','City_Name':'区域',},inplace=True)
	paths = os.path.dirname(__file__)
	
	filename = str(shop_id)+str(start_date)+'_'+str(end_date)+'岭南四馆发票数据.xlsx'
	print(filename)
	df1.to_excel(os.path.join(paths,filename),engine='openpyxl')

if __name__ =="__main__":

	'''
	conn = pymysql.connect(host='10.26.0.133',
				port=3306,
				user='uapi',
				password='Gst#!2017.data',
				db='DbCenter',
				charset ='utf8',
				cursorclass=pymysql.cursors.DictCursor
			)
		'''
	shop_list = [264,265,266,267]
	date_list = [('2015.01.01','2015.12.31'),('2016.01.01','2016.12.31'),('2017.01.01','2017.06.30')]
	
	engine = create_engine("mysql+pymysql://uapi:Gst#!2017.data@10.26.0.133:3306/DbCenter?charset=utf8",echo=True)
	
	for stime,etime in date_list:
		for shop_id in shop_list:
			threads  = threading.Thread(target = excel_export, args = (engine, shop_id, stime, etime), name = 'thread-' + str(shop_id) + str(stime))
			threads.start()
			threads.join()
	
	print(u'数据导出完成！')

