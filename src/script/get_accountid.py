# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json

wb = xlrd.open_workbook(u'游戏账号前缀列表.xlsx')
table0 = wb.sheet_by_name(u'Sheet1')

#最大体力
ACCOUNTPRE = []

for x in xrange(0, table0.nrows):
    ACCOUNTPRE.append(table0.cell(x, 0).value)

print 'ACCOUNTPRE =', json.dumps(ACCOUNTPRE, sort_keys=True, indent=2, separators=(',', ': '))