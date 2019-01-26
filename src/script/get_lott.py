# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
SQL = True

wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table0 = wb.sheet_by_name(u'抽奖概率')
table1 = wb.sheet_by_name(u'抽奖奖励')
if not SQL:
	PRODPROB = {}
	PROB1 = {}
	PROB2 = {}
	for x in xrange(4, table0.nrows):
	    if int(table0.cell(x, 1).value) == 1:
	        PROB1[int(table0.cell(x, 0).value)] = [one for one in str(table0.cell(x, 2).value).split(',')]
	    elif int(table0.cell(x, 1).value) == 2:
	        PROB2[int(table0.cell(x, 0).value)] = [one for one in str(table0.cell(x, 2).value).split(',')]
	    else:pass

	PRODPROB = {1:PROB1, 2:PROB2}
	print 'PRODPROB =', json.dumps(PRODPROB, sort_keys=True, indent=2, separators=(',', ': '))

	PRODREWARD = {}
	for x in xrange(2, table1.nrows):
	  PRODREWARD[int(table1.cell(x, 0).value)] = []

	for x in xrange(2, table1.nrows):
	  value = int(table1.cell(x, 0).value)
	  if value in PRODREWARD:
	    PRODREWARD[value].append([str(table1.cell(x, 1).value), int(table1.cell(x, 2).value), int(table1.cell(x, 3).value)])

	print 'PRODREWARD =', json.dumps(PRODREWARD, sort_keys=True, indent=2, separators=(',', ': '))
else:
	import psycopg2
	DATA_HOST = '127.0.0.1'
	DATA_NAME = 'ptkingdom'
	DATA_USER = 'deploy'
	DATA_PASSWORD = 'asdf12345678'
	DATA_PORT = '5432'
	__author__ = 'yezijian'

	conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port=DATA_PORT)

	for x in xrange(4, table0.nrows):
		times = int(table0.cell(x, 0).value)
		lotttype = int(table0.cell(x, 1).value)
		proba = repr(str(table0.cell(x, 2).value))
		query = "INSERT INTO core_prodproba(times, lotttype, proba) VALUES (%s, %s, %s)"
		params = (times, lotttype, proba)
		sql =  query % params
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
		
	for y in xrange(2, table1.nrows):
		lotttype = int(table1.cell(y, 0).value)
		times = int(table1.cell(y, 6).value)
		prod = repr(str(table1.cell(y, 1).value))
		maxnum = int(table1.cell(y, 3).value)
		minnum = int(table1.cell(y, 2).value)
		query = "INSERT INTO core_prodreward(lotttype, times, prod, maxnum, minnum) VALUES (%s, %s, %s, %s, %s)"
		params = (lotttype, times, prod, maxnum, minnum)
		sql =  query % params
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()

	conn.close()