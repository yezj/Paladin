
# -*- coding: utf-8 -*-
#!python -m json.tool
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

wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'签到')

#魂匣
for x in xrange(2, table.nrows):
	day = int(table.cell(x, 0).value)
	prods = str(table.cell(x, 1).value)
	nums = str(table.cell(x, 5).value)
	gold = int(table.cell(x, 2).value)
	feat = int(table.cell(x, 3).value)
	rock = int(table.cell(x, 4).value)
	vip = int(table.cell(x, 6).value)
	if prods == '-1':
		query = "INSERT INTO core_seal(day, gold, rock, vip, feat) VALUES (%s, %s, %s, %s, %s)"
		params = (day, gold, rock, vip, feat)
	else:
		query = "INSERT INTO core_seal(day, gold, rock, prods, nums, vip, feat) VALUES (%s, %s, %s, %s, %s, %s, %s)"
		params = (day, gold, rock, prods, nums, vip, feat)
	sql =  query % params
	print sql
	cur = conn.cursor()
	cur.execute(sql)
	conn.commit()
conn.close()

	

