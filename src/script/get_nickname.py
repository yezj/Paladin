# -*- coding: utf-8 -*-
import sys
import xlwt
import xlrd
from datetime import datetime
import json
from itertools import *
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'玩家可选姓名清单.xlsx')
xlrd.Book.encoding = "utf-8"
table = wb.sheet_by_name(u'姓名清单')
#姓名清单
PREFIX =  []
POSTFIX = []
for x in xrange(1, table.nrows):
	if table.cell(x, 0).value != '':
		PREFIX.append(table.cell(x, 0).value)
	if table.cell(x, 1).value != '':
		POSTFIX.append(table.cell(x, 1).value)

print 'PREFIX =', json.dumps(PREFIX, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)
print 'POSTFIX =', json.dumps(POSTFIX, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)
NICKNAME = [i[0]+i[1] for i in product(PREFIX, POSTFIX)]
print 'NICKNAME =', json.dumps(NICKNAME, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)