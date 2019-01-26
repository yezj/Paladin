# -*- coding: utf-8 -*-

import xlwt
import xlrd
from datetime import datetime
import json

wb = xlrd.open_workbook(u'武将汇总.xlsx')
table = wb.sheet_by_name(u'风云物品')
STORMPROD = {}
# print 'STORMPROD = ['
for x in xrange(5, table.nrows):
	STORMPROD[str(table.cell(x, 0).value)] = dict(num=table.cell(x, 2).value, name=table.cell(x, 3).value)
	#print (table.cell(x, 1).value)
	#STORMPROD.extend([str(table.cell(x, 0).value), table.cell(x, 1).value, str(table.cell(x, 2).value), table.cell(x, 3).value])
	#STORMPROD.extend([int(table.cell(x, 0).value), table.cell(x, 1).value, int(table.cell(x, 2).value), table.cell(x, 3).value])
#print ']'
print 'STORMPROD =', json.dumps(STORMPROD, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)
