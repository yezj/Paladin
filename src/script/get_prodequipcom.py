# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
wb = xlrd.open_workbook('武将汇总.xlsx')
table = wb.sheet_by_name(u'物品')
#武将汇总 物品
PRODEQUIPCOM = {}
PRODSELLOUT = {}
for x in xrange(2, table.nrows):
	if type(table.cell(x, 8).value) !=  float:
		if str(table.cell(x, 8).value.encode('utf-8')) == '-1':
			prods = {}
		else:
			try:
				prods = dict(zip([str(one) for one in table.cell(x, 8).value.split(',')], [int(one) for one in table.cell(x, 9).value.split(',')]))
			except:
				prods = dict(zip([table.cell(x, 8).value], [int(table.cell(x, 9).value)]))
		id = table.cell(x, 5).value
		if table.cell(x, 10).value:
			PRODEQUIPCOM[str(id)] = dict(gold=int(table.cell(x, 10).value), prods=prods)
	PRODSELLOUT[str(table.cell(x, 5).value)] = dict(gold=int(table.cell(x, 20).value))
		
print 'PRODEQUIPCOM =', json.dumps(PRODEQUIPCOM, sort_keys=True, indent=2, separators=(',', ': '))

print 'PRODSELLOUT =', json.dumps(PRODSELLOUT, sort_keys=True, indent=2, separators=(',', ': '))