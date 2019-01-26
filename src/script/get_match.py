# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
SQL = False
wb = xlrd.open_workbook(u'关卡汇总.xlsx')
table = wb.sheet_by_name(u'武将出场')
#武将出场
FIRSTBATT = {}
for x in xrange(2, table.nrows):
	if type(table.cell(x, 3).value) != float and str(table.cell(x, 3).value) != '0':
		id = table.cell(x, 0).value

table = wb.sheet_by_name(u'战役关卡')
table1 = wb.sheet_by_name(u'英雄试炼')
#关卡汇总 战役关卡
MATCH = {}
TRIAL = {}
BATT = []
HARDBATT = []
if not SQL:
	for x in xrange(2, table.nrows):
		if table.cell(x, 0).value:
			id = table.cell(x, 0).value
			#str(one) for one in table.cell(x, 47).value.split(',')
			MATCH[str(id)] = dict(label=str(table.cell(x, 0).value), hp=int(table.cell(x, 17).value), gold=int(table.cell(x, 20).value), \
				feat=int(table.cell(x, 21).value), xp=int(table.cell(x, 18).value), hxp=int(table.cell(x, 19).value), \
				prods=[], minnum=int(table.cell(x, 54).value), maxnum=int(table.cell(x, 55).value))
			BATT.append(str(id))
			if str(id) not in FIRSTBATT:
				FIRSTBATT[str(id)] = {}
				
			FIRSTBATT[str(id)]['gold'] = int(table.cell(x, 49).value)
			FIRSTBATT[str(id)]['feat'] = int(table.cell(x, 50).value)
			FIRSTBATT[str(id)]['rock'] = int(table.cell(x, 51).value)
			if str(table.cell(x, 52).value) != '-1':
				FIRSTBATT[str(id)]['prods'] = dict(zip(table.cell(x, 52).value.split(','), [int(one) for one in table.cell(x, 53).value.split(',')]))
		if table.cell(x, 1).value:
			if str(table.cell(x, 1).value) != '-1.0' and str(table.cell(x, 1).value) != '-1':
				id = table.cell(x, 1).value
				if str(table.cell(x, 33).value) != '-1.0' and str(table.cell(x, 33).value) != '-1':
					MATCH[str(id)] = dict(label=str(table.cell(x, 1).value), hp=int(table.cell(x, 36).value), gold=int(table.cell(x, 39).value), \
					feat=int(table.cell(x, 40).value), xp=int(table.cell(x, 37).value), hxp=int(table.cell(x, 38).value), \
					prods=[], maxentry=int(table.cell(x, 42).value),\
					 minnum=int(table.cell(x, 56).value), maxnum=int(table.cell(x, 57).value))
					HARDBATT.append(str(id))
				else:
					MATCH[str(id)] = dict(label=str(table.cell(x, 0).value), hp=int(table.cell(x, 36).value), gold=int(table.cell(x, 39).value), \
					feat=int(table.cell(x, 40).value), xp=int(table.cell(x, 37).value), hxp=int(table.cell(x, 38).value), \
					prods=[], minnum=0, maxnum=0)
	for x in xrange(2, table1.nrows):
		if table1.cell(x, 0).value:
			if str(table1.cell(x, 0).value) != '-1.0' and str(table1.cell(x, 0).value) != '-1':
				id = table1.cell(x, 0).value
				#print 'id', [one for one in table1.cell(x, 15).value.split(',') if one]
				TRIAL[str(id)] = dict(label=str(table1.cell(x, 0).value)[:4], hp=int(table1.cell(x, 16).value), gold=int(table1.cell(x, 19).value), \
				feat=int(table1.cell(x, 20).value), xp=int(table1.cell(x, 17).value), hxp=int(table1.cell(x, 18).value), \
				prods=zip([str(one) for one in table1.cell(x, 14).value.split(',')], [float(one)/100.0 for one in table1.cell(x, 15).value.split(',') if one]), \
				colddown=int(table1.cell(x, 21).value), maxentry=int(table1.cell(x, 22).value), \
				playterm="lambda"
				)
	print 'FIRSTBATT =', json.dumps(FIRSTBATT, sort_keys=True, indent=2, separators=(',', ': '))
	print 'MATCH =', json.dumps(MATCH, sort_keys=True, indent=2, separators=(',', ': '))
	# print 'TRIAL =', json.dumps(TRIAL, sort_keys=True, indent=2, separators=(',', ': '))
	print 'BATT =', json.dumps(BATT, sort_keys=True, indent=2, separators=(',', ': '))
	print 'HARDBATT =', json.dumps(HARDBATT, sort_keys=True, indent=2, separators=(',', ': '))
else:
	import psycopg2
	DATA_HOST = '127.0.0.1'
	DATA_NAME = 'ptkingdom'
	DATA_USER = 'deploy'
	DATA_PASSWORD = 'asdf12345678'
	DATA_PORT = '5432'
	__author__ = 'yezijian'

	conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port=DATA_PORT)
	for x in xrange(2, table.nrows):
		if table.cell(x, 0).value:
			id = table.cell(x, 0).value
			# MATCH[str(id)] = dict(label=str(table.cell(x, 0).value), hp=int(table.cell(x, 17).value), gold=int(table.cell(x, 20).value), \
			# 	feat=int(table.cell(x, 21).value), xp=int(table.cell(x, 18).value), hxp=int(table.cell(x, 19).value), \
			# 	prods=[str(one) for one in table.cell(x, 47).value.split(',')], minnum=int(table.cell(x, 54).value), maxnum=int(table.cell(x, 55).value))
			mid = repr(str(id))
			prod = repr(str(table.cell(x, 47).value))
			minnum=int(table.cell(x, 54).value)
			maxnum=int(table.cell(x, 55).value)	
			query = "INSERT INTO core_match(mid, prod, minnum, maxnum) VALUES (%s, %s, %s, %s)"
			params = (mid, prod, minnum, maxnum)
			sql =  query % params
			cur = conn.cursor()
			cur.execute(sql)
			conn.commit()
		if table.cell(x, 1).value:
			if str(table.cell(x, 1).value) != '-1.0' and str(table.cell(x, 1).value) != '-1':
				id = table.cell(x, 1).value
				if str(table.cell(x, 33).value) != '-1.0' and str(table.cell(x, 33).value) != '-1':
					# MATCH[str(id)] = dict(label=str(table.cell(x, 1).value), hp=int(table.cell(x, 36).value), gold=int(table.cell(x, 39).value), \
					# feat=int(table.cell(x, 40).value), xp=int(table.cell(x, 37).value), hxp=int(table.cell(x, 38).value), \
					# prods=[str(one) for one in table.cell(x, 48).value.split(',')], maxentry=int(table.cell(x, 42).value),\
					#  minnum=int(table.cell(x, 56).value), maxnum=int(table.cell(x, 57).value))
					# HARDBATT.append(str(id))
					mid = repr(str(id))
					prod = repr(str(table.cell(x, 48).value))
					minnum=int(table.cell(x, 56).value)
					maxnum=int(table.cell(x, 57).value)
				else:
					# MATCH[str(id)] = dict(label=str(table.cell(x, 0).value), hp=int(table.cell(x, 36).value), gold=int(table.cell(x, 39).value), \
					# feat=int(table.cell(x, 40).value), xp=int(table.cell(x, 37).value), hxp=int(table.cell(x, 38).value), \
					# prods=[], minnum=0, maxnum=0)
					mid = repr(str(id))
					prod = ''
					minnum = 0
					maxnum = 0
				query = "INSERT INTO core_match(mid, prod, minnum, maxnum) VALUES (%s, %s, %s, %s)"
				params = (mid, prod, minnum, maxnum)
				sql =  query % params
				cur = conn.cursor()
				cur.execute(sql)
				conn.commit()
	conn.close()