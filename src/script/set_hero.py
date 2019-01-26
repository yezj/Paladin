# -*- coding: utf-8 -*-
import sys
import xlwt
import xlrd
import time
from datetime import datetime
import json
from itertools import *
reload(sys)
sys.setdefaultencoding("utf-8")

import psycopg2
from pyExcelerator import *
#from xlwt import *
DATA_HOST = '127.0.0.1'
DATA_NAME = 'ptkingdom'
DATA_USER = 'deploy'
DATA_PASSWORD = 'asdf12345678'

__author__ = 'yezijian'

wb = xlwt.Workbook()
xlrd.Book.encoding = "utf-8"

conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port="5432")
cur = conn.cursor()

query = "SELECT id, slug FROM core_channel"
cur.execute(query)
rows = cur.fetchall()
for r in rows:
	id, slug = r
	table = wb.add_sheet(slug)
	table.write(0, 0, u'用户id')
	table.write(0, 1 ,u'设备')
	table.write(0, 2 ,u'付费时间')
	table.write(0, 3 ,u'购买商品')
	table.write(0, 4 ,u'金额(元)')

	if slug == 'putaogame':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_payrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"

	if slug == 'pt_xiaomi':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_xmpayrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"

	if slug == 'pt_letv':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_letvpayrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"

	if slug == 'pt_chinamobile':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_cmpayrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"
 
	if slug == 'pt_lovegame':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_lgpayrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"

	if slug == 'pt_ali':
		query = "SELECT a.user_id, b.model, a.created_at, a.pid, c.price FROM core_alipayrecord AS a,\
		 core_account AS b, core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid;"
		 
	cur.execute(query)
	res = cur.fetchall()
	i = 1
	for r in res:
		user_id, model, created_at, pid, amount = r
		table.write(i, 0, user_id)
		table.write(i, 1, model)
		table.write(i, 2, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(created_at)))
		table.write(i, 3, pid)
		table.write(i, 4, amount)
		i += 1

wb.save(u'后台支付明细.xls')

