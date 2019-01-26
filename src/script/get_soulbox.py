
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
table = wb.sheet_by_name(u'魂匣')

#魂匣
for x in xrange(8, table.nrows):
	week = int(table.cell(x, 0).value)
	mtype = int(table.cell(x, 1).value)
	prod = str(table.cell(x, 2).value)
	num = int(table.cell(x, 3).value)
	proba = float(table.cell(x, 4).value)/100
	print 'proba', proba
	rock = int(table.cell(x, 5).value)
	query = "INSERT INTO core_soulbox(week, type, prod, num, proba, rock) VALUES (%s, %s, %s, %s, %s, %s)"
	params = (week, mtype, prod, num, proba, rock)
	sql =  query % params
	cur = conn.cursor()
	cur.execute(sql)
	conn.commit()
conn.close()

	

