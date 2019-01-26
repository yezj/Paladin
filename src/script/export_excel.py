# -*- coding: utf-8 -*-
import psycopg2
from pyExcelerator import *
#from xlwt import *
DATA_HOST = '127.0.0.1'
DATA_NAME = 'ptkingdom'
DATA_USER = 'deploy'
DATA_PASSWORD = 'asdf12345678'

__author__ = 'yezijian'

conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port="5432")
cur = conn.cursor()

# query = "select a.nickname, a.avat, a.xp, a.gold, a.rock, a.feat, a.book, a.jextra, a.jheros,\
#  a.jprods, a.jbatts, a.jseals, a.jtasks, a.jworks, a.jmails, a.jdoors, b.arena_coin, b.before_rank,\
#   b.now_rank, b.jguards, b.formation from core_user as a, core_arena as b where a.id=b.user_id;"
# query = "SELECT b.user_id, b.now_rank FROM core_user AS a, core_arena AS b WHERE a.id=b.user_id AND b.now_rank<=200 AND a.xp/100000>=9 ORDER BY b.now_rank asc;"
# cur.execute(query)
# rows = cur.fetchall()

# #print rows

w = Workbook()
ws = w.add_sheet(u'排行榜')
ws.write(0, 0, u'用户id')
ws.write(0, 1 ,u'排行')
ws.write(0, 2 ,u'accountid')
# # for row in rows:
# for x in xrange(0, len(rows)):
#     ws.write(x+1, 0, rows[x][0])
#     ws.write(x+1, 1, rows[x][1])
# w.save('enemy.xls')

# -*- coding: utf-8 -*-
import sys
import xlwt
import xlrd
from datetime import datetime
import json
from itertools import *
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'enemy.xls')
xlrd.Book.encoding = "utf-8"
table = wb.sheet_by_name(u'排行榜')
for x in xrange(1, table.nrows):
	query = "select id from core_account where user_id=%s" % int(table.cell(x, 0).value)
	#print int(table.cell(x, 0).value), (table.cell(x, 0).value)
	cur.execute(query)
	row, = cur.fetchone()
	ws.write(x, 0, int(table.cell(x, 0).value))
	ws.write(x, 1, int(table.cell(x, 0).value))
	ws.write(x, 2, row)
w.save('enemy.xls')

