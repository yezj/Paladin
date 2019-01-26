# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'1_过五关.xlsx')
xlrd.Book.encoding = "utf-8"
table0 = wb.sheet_by_name(u'远征商店出售物品')
table1 = wb.sheet_by_name(u'远征商店刷新花费')

ARENAPROD = {}

for x in xrange(2, table0.nrows):
    ARENAPROD[str(table0.cell(x, 0).value)] = dict(num=int(table0.cell(x, 1).value), exped_coin=int(table0.cell(x, 2).value), name=table0.cell(x, 5).value)

print 'EXPEDPROD =', json.dumps(ARENAPROD, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)

print 'EXPEDREFRESH = ['
for x in xrange(3, table1.nrows):
    print '	%s, %s, ' % (int(table1.cell(x, 0).value), int(table1.cell(x, 1).value))
print ']'

