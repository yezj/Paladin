# -*- coding: utf-8 -*-
import xlwt
import xlrd
from datetime import datetime
import json
HEROSTAR = {
    '01001': [
        {'gold': 0, 'prods': {}},
        {'gold': 10000, 'prods': {'03001': 20}},
        {'gold': 50000, 'prods': {'03001': 50}},
        {'gold': 200000, 'prods': {'03001': 100}},
        {'gold': 800000, 'prods': {'03001': 200}},
        {'gold': 1500000, 'prods': {'03001': 500}},
    ],
}
wb = xlrd.open_workbook(u'武将汇总.xlsx')
table = wb.sheet_by_name(u'武将')

wb1 = xlrd.open_workbook(u'经验经济汇总.xlsx')
table1 = wb1.sheet_by_name(u'武将升星')
STAR = {}
for x in xrange(2, table1.nrows):
	STAR[int(table1.cell(x, 0).value)] = int(table1.cell(x, 3).value)
print STAR
#武将
HERORECRUIT = {}
HEROSTAR = {}
for x in xrange(2, table.nrows):
	id = table.cell(x, 5).value
	hid = table.cell(x,22).value
	recruit = []
	prize = []
	#for x in xrange(2, table1.nrows):
	prods = {}
	prods[str(hid)] = STAR[int(table.cell(x, 10).value)]
	recruit.append({'prods': prods, 'gold': 0})

	HERORECRUIT[str(id)] = recruit

	for x in xrange(3, table1.nrows):
		prods = {}
		prods[str(hid)] = int(table1.cell(x, 1).value)
		prize.append({'prods': prods, 'gold': int(table1.cell(x, 2).value)})


	HEROSTAR[str(id)] = prize
print 'HERORECRUIT =', json.dumps(HERORECRUIT, sort_keys=True, indent=2, separators=(',', ': '))
print 'HEROSTAR =', json.dumps(HEROSTAR, sort_keys=True, indent=2, separators=(',', ': '))

