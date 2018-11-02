#!/usr/bin/env python
#coding:utf-8

import numpy as np
import pandas as pd
from io import BytesIO
import xlsxwriter
from flask import Flask, send_file, make_response


app = Flask(__name__)

class Excel:
	def export(self):
		output = BytesIO()
		writer = pd.ExcelWriter(output, engine='xlsxwriter')
		workbook = writer.book
		worksheet = workbook.add_worksheet('sheet1')
	
		cell_format = workbook.add_format({
			'bold': 1,
			'border':1,
			'align':'center',
			'valign':'vcenter',
			'fg_color':'#f4cccc'
			})
		col = 0
		row = 1
		columns = ['A','B','C','D','E']
		for item in columns:
			worksheet.write(0, col,item,cell_format)
			col+=1

		index = 0
		while index<10:
			for co in columns:
				worksheet.write(row, columns.index(co), index)
			row += 1
			index += 1
			print('row == %s, index ===%s' %(row, index))

		worksheet.set_column('A:E', 20)

		writer.close()
		output.seek(0)
		return output


@app.route('/index')
def index():
	return 'Hello World'


@app.route('/export')
def export():
	output = Excel().export()
	return send_file(
			output,
			attachment_filename='testing.xlsx',
			as_attachment=True
			)

@app.route('/new_export')
def new_export():
	output = Excel().export()
	resp = make_response(output.getvalue())
	resp.headers['Content-Type'] = 'application/x-xlsx'
	resp.headers["Content-Disposition"] ="attachment; filename=testing.xlsx"
	return resp

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5003, debug=True)
