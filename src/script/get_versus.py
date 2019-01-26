# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'1_群雄争霸.xlsx')
xlrd.Book.encoding = "utf-8"
table0 = wb.sheet_by_name(u'群雄币')
table1 = wb.sheet_by_name(u'售出物品')

VERSUSPROD = {}

for x in xrange(2, table1.nrows):
    VERSUSPROD[str(table1.cell(x, 0).value)] = dict(num=int(table1.cell(x, 1).value), versus_coin=int(table1.cell(x, 2).value), name=table1.cell(x, 5).value)

print 'VERSUSPROD =', json.dumps(VERSUSPROD, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)


print 'VERSUSOUTPUT = ['
for x in xrange(3, table0.nrows):
    print ' %s, %s, %s, ' % (int(table0.cell(x, 0).value), int(table0.cell(x, 1).value), int(table0.cell(x, 2).value))
print ']'


