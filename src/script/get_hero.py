# -*- coding: utf-8 -*-

import xlwt
import xlrd
from datetime import datetime
import json
wb = xlrd.open_workbook(u'武将汇总.xlsx')
table = wb.sheet_by_name(u'武将')
#武将
HEROEQUIP = {}
for x in xrange(2, table.nrows):
	equip = []
	for y in xrange(23, 43):
		equip.append(tuple([str(one) for one in table.cell(x, y).value.split(',')]))
	id = table.cell(x, 5).value
	HEROEQUIP[str(id)] = equip
print 'HEROEQUIP =', json.dumps(HEROEQUIP, sort_keys=True, indent=2, separators=(',', ': '))


wb = xlrd.open_workbook(u'武将汇总.xlsx')
table = wb.sheet_by_name(u'物品')
#武将汇总 物品
PRODEQUIPCOM = {}
PRODSELLOUT = {}
PRODS = []
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

for x in xrange(2, table.nrows):
	id = table.cell(x, 5).value
	PRODSELLOUT[str(id)] = dict(gold=int(table.cell(x, 20).value))
	
	PRODS.append(str(table.cell(x, 5).value))
print 'PRODEQUIPCOM =', json.dumps(PRODEQUIPCOM, sort_keys=True, indent=2, separators=(',', ': '))

print 'PRODSELLOUT =', json.dumps(PRODSELLOUT, sort_keys=True, indent=2, separators=(',', ': '))

print 'PRODS =', json.dumps(PRODS, sort_keys=True, indent=2, separators=(',', ': '))

wb = xlrd.open_workbook(u'武将汇总.xlsx')
table = wb.sheet_by_name(u'武将')
table1 = wb.sheet_by_name(u'技能')
HERO = {}
SKILLS = {}
for x in xrange(3, table1.nrows):
	id = table1.cell(x, 0).value
	layer = table1.cell(x, 1).value
	if id:
		SKILLS[str(id)] = int(layer)

for x in xrange(2, table.nrows):
	id = table.cell(x, 5).value
	HERO[str(id)] = dict(hid=str(table.cell(x, 5).value), xp=0, star=int(table.cell(x, 10).value), color=0, skills=[0, 0, 0, 0], equips=[0, 0, 0, 0, 0, 0])
	#HERO[str(id)] = dict(hid=str(table.cell(x, 5).value), xp=0, star=0, color=0, skills=[SKILLS[str(one)] for one in table.cell(x, 9).value.split(',')], equips=[0, 0, 0, 0, 0, 0])
print 'HERO =', json.dumps(HERO, sort_keys=True, indent=2, separators=(',', ': '))

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
#print 'STORMPROD =', json.dumps(STORMPROD, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)
