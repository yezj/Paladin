
# -*- coding: utf-8 -*-
#!python -m json.tool
import time
import xlwt
import xlrd
from datetime import datetime
import json
import psycopg2
DATA_HOST = '127.0.0.1'
DATA_NAME = 'ptkingdom'
DATA_USER = 'deploy'
DATA_PASSWORD = 'asdf12345678'

__author__ = 'yezijian'

conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port="5432")

wb = xlrd.open_workbook(u'VIP汇总.xlsx')
table = wb.sheet_by_name(u'钻石商店')

#魂匣
for x in xrange(3, table.nrows):
	pid = repr(str(table.cell(x, 0).value))
	code = repr(str(table.cell(x, 2).value))
	order = int(table.cell(x, 3).value)
	ptype = int(table.cell(x, 4).value)
	value = int(table.cell(x, 5).value)
	extra = int(table.cell(x, 6).value)
	times = int(table.cell(x, 8).value)
	price = int(table.cell(x, 9).value)
	#print pid, code, order, ptype, value, extra, times, price
	query = "INSERT INTO core_product(pid, code, sequence, type, value, extra, times, price, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
	print query
	params = (pid, code, order, ptype, value, extra, times, price, int(time.time()))
	sql =  query % params
	print sql
	cur = conn.cursor()
	cur.execute(sql)
	conn.commit()
conn.close()

	

