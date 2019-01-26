# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json

wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table0 = wb.sheet_by_name(u'最大体力')
table1 = wb.sheet_by_name(u'等级经验')
table2 = wb.sheet_by_name(u'武将等级')
table3 = wb.sheet_by_name(u'武将突飞')
table4 = wb.sheet_by_name(u'体力购买')
table5 = wb.sheet_by_name(u'点金手价格')
table6 = wb.sheet_by_name(u'技能书购买')
table7 = wb.sheet_by_name(u'黑市')
table8 = wb.sheet_by_name(u'黑市刷新花费')
table9 = wb.sheet_by_name(u'集市刷新花费')
table10 = wb.sheet_by_name(u'集市')
table11 = wb.sheet_by_name(u'杂货铺刷新花费')
table12 = wb.sheet_by_name(u'杂货铺')
table13 = wb.sheet_by_name(u'缠绵消耗')
table14 = wb.sheet_by_name(u'夜明珠购买')
table15 = wb.sheet_by_name(u'精英重置价格')
table16 = wb.sheet_by_name(u'签到')
#最大体力
print 'LEVELHP = ['
for x in xrange(2, table0.nrows):
	print '	%s, ' % int(table0.cell(x, 1).value)
print ']'

#等级经验
print 'LEVELXP = ['
for x in xrange(2, table1.nrows):
	print '	%s, %s, ' % (int(table1.cell(x, 1).value), int(table1.cell(x, 2).value))
print ']'

#武将等级
print 'LEVELHXP = ['
for x in xrange(2, table2.nrows):
	print '	%s, %s, ' % (int(table2.cell(x, 1).value), int(table2.cell(x, 2).value))
print ']'

#武将突飞
print 'LEVELFEAT = ['
for x in xrange(2, table3.nrows):
	print '	%s, %s, ' % (int(table3.cell(x, 1).value), int(table3.cell(x, 2).value))
print ']'

#武将等级限制
wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'武将等级上限')
print 'LEVELIMIT = ['
for x in xrange(1, table.nrows):
	print '	%s, %s, ' % (int(table.cell(x, 0).value), int(table.cell(x, 1).value))
print ']'

#体力购买
print 'HPBUY = ['
for x in xrange(4, table4.nrows):
	print '	%s, %s, %s, ' % (int(table4.cell(x, 0).value), int(table4.cell(x, 1).value), int(table4.cell(x, 2).value))
print ']'

#点金手
print 'GOLDBUY = ['
for x in xrange(4, table5.nrows):
	print '	%s, %s, %s, ' % (int(table5.cell(x, 0).value), int(table5.cell(x, 1).value), int(table5.cell(x, 2).value))
print ']'


#点金手
print 'BOOKBUY = ['
for x in xrange(4, table6.nrows):
	print '	%s, %s, %s, ' % (int(table6.cell(x, 0).value), int(table6.cell(x, 1).value), int(table6.cell(x, 2).value))
print ']'

#黑市
print 'BLACKMARKET = ['
for x in xrange(6, table7.nrows):
	#print table7.cell(x, 0).value, table7.cell(x, 1).value, table7.cell(x, 2).value, table7.cell(x, 3).value
	print '	%s, %s, %s, %s, %s, %s, %s, %s, %s, ' % (int(table7.cell(x, 0).value), int(table7.cell(x, 1).value),\
	 int(table7.cell(x, 2).value), int(table7.cell(x, 3).value), int(table7.cell(x, 4).value),\
	  int(table7.cell(x, 5).value), int(table7.cell(x, 6).value), int(table7.cell(x, 7).value),\
	   int(table7.cell(x, 8).value))
print ']'
#黑市刷新花费
print 'BMREFRESH = ['
for x in xrange(4, table8.nrows):
    print '	%s, %s, ' % (int(table8.cell(x, 0).value), int(table8.cell(x, 1).value))
print ']'

#集市刷新花费
print 'MARKETREFRESH = ['
for x in xrange(4, table9.nrows):
    print '	%s, %s, ' % (int(table9.cell(x, 0).value), int(table9.cell(x, 1).value))
print ']'

#黑市
print 'MARKET = ['
for x in xrange(6, table10.nrows):
	print '	%s, %s, %s, %s, %s, %s, %s, %s, %s, ' % (int(table10.cell(x, 0).value), int(table10.cell(x, 1).value),\
	 int(table10.cell(x, 2).value), int(table10.cell(x, 3).value), int(table10.cell(x, 4).value),\
	  int(table10.cell(x, 5).value), int(table10.cell(x, 6).value), int(table10.cell(x, 7).value),\
	   int(table10.cell(x, 8).value))
print ']'

#杂货铺刷新花费
print 'STOREREFRESH = ['
for x in xrange(4, table11.nrows):
    print '	%s, %s, ' % (int(table11.cell(x, 0).value), int(table11.cell(x, 1).value))
print ']'

#杂货铺
print 'STORE = ['
for x in xrange(6, table12.nrows):
	print '	%s, %s, %s, %s, %s, %s, %s, %s, %s, ' % (int(table12.cell(x, 0).value), int(table12.cell(x, 1).value),\
	 int(table12.cell(x, 2).value), int(table12.cell(x, 3).value), int(table12.cell(x, 4).value),\
	  int(table12.cell(x, 5).value), int(table12.cell(x, 6).value), int(table12.cell(x, 7).value),\
	   int(table12.cell(x, 8).value))
print ']'

#缠绵消耗
print 'BEAUTY = ['
for x in xrange(4, table13.nrows):
	print '	%s, %s, %s, %s, %s, %s, %s, ' % (int(table13.cell(x, 0).value), int(table13.cell(x, 1).value), repr('04002'),\
	 int(table13.cell(x, 2).value), int(table13.cell(x, 3).value), repr('04002'), int(table13.cell(x, 4).value))
print ']'
#夜明珠购买
print 'PEARLBUY = ['
for x in xrange(4, table14.nrows):
	print '	%s, %s, %s, ' % (int(table14.cell(x, 0).value), int(table14.cell(x, 1).value), int(table14.cell(x, 2).value))
print ']'
#精英购买
print 'BATTBUY = ['
for x in xrange(5, table15.nrows):
	print '	%s, %s, %s, ' % (int(table15.cell(x, 0).value), int(table15.cell(x, 1).value), int(table15.cell(x, 2).value))
print ']'

wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'武将技能')
#等级经验
skill_0=[]
skill_1=[]
skill_2=[]
skill_3=[]
for x in xrange(2, table.nrows):
	prods = {}
	if int(table.cell(x, 1).value) == 0:
		skill_0.append(dict(xp=int(table.cell(x, 2).value)*100000, \
			gold=int(table.cell(x, 4).value), prods=prods))
	elif int(table.cell(x, 1).value) == 1:
		skill_1.append(dict(xp=int(table.cell(x, 2).value)*100000, \
			gold=int(table.cell(x, 4).value), prods=prods))
	elif int(table.cell(x, 1).value) == 2:
		skill_2.append(dict(xp=int(table.cell(x, 2).value)*100000, \
			gold=int(table.cell(x, 4).value), prods=prods))
	elif int(table.cell(x, 1).value) == 3:
		skill_3.append(dict(xp=int(table.cell(x, 2).value)*100000, \
			gold=int(table.cell(x, 4).value), prods=prods))
	else:pass
print 'HEROSKILL =', json.dumps([skill_0, skill_1, skill_2, skill_3], sort_keys=True, indent=2, separators=(',', ': '))


wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'武将升色')
HEROCOLOR = []
for x in xrange(2, table.nrows):
	HEROCOLOR.append(dict(gold=int(table.cell(x, 1).value)))
print 'HEROCOLOR =', json.dumps(HEROCOLOR, sort_keys=True, indent=2, separators=(',', ': '))

#每级所赠体力
wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'每级所赠体力')
print 'AWARDHP = ['
for x in xrange(2, table.nrows):
	print '	%s, %s, ' % (int(table.cell(x, 0).value), int(table.cell(x, 1).value))
print ']'

wb = xlrd.open_workbook(u'关卡汇总.xlsx')
table = wb.sheet_by_name(u'世界奖励')
print 'TAX = ['
for x in xrange(4, table.nrows):
	print '	%s, %s, %s, ' % (str(int(table.cell(x, 0).value)), int(table.cell(x, 4).value), int(table.cell(x, 5).value))
print ']'



wb = xlrd.open_workbook(u'经验经济汇总.xlsx')
table = wb.sheet_by_name(u'任务')
#特殊 lambda
#任务
TASKS = {}
TASKCAT = {'01':[], '02':[], '03':[]}
for x in xrange(2, table.nrows):
	id = table.cell(x, 0).value
	#tags = table.cell(x, 13).value.split(':')[1].split('}')[0]
	tags = eval(table.cell(x, 13).value)
	gold = 0
	rock = 0
	prods = {}
	if str(table.cell(x, 3).value) == 'gold' and str(table.cell(x, 4).value) != '-1':
		gold = int(table.cell(x, 4).value)

	if str(table.cell(x, 3).value) == 'rock' and str(table.cell(x, 4).value) != '-1':
		rock = int(table.cell(x, 4).value)

	if str(table.cell(x, 5).value) != '-1':
		prods[str(table.cell(x, 5).value)] = int(table.cell(x, 6).value)
	if str(table.cell(x, 7).value) == '0':
		TASKS[str(id)]=dict(tags=tags, cat=str(id)[:2], awards=dict(gold=gold, rock=rock, prods=prods), progress=str(table.cell(x, 14).value))
	else:
		TASKS[str(id)]=dict(tags=tags, cat=str(id)[:2], awards=dict(gold=gold, rock=rock, prods=prods), revdep=[str(table.cell(x, 7).value)], progress=str(table.cell(x, 14).value))

	if str(table.cell(x, 0).value).startswith('01'):
		TASKCAT['01'].append(str(table.cell(x, 0).value))
	elif str(table.cell(x, 0).value).startswith('02'):
		TASKCAT['02'].append(str(table.cell(x, 0).value))	
	elif str(table.cell(x, 0).value).startswith('03'):
		TASKCAT['03'].append(str(table.cell(x, 0).value))	

print 'TASK =', json.dumps(TASKS, sort_keys=True, indent=2, separators=(',', ': '))

print 'TASKCAT =', json.dumps(TASKCAT, sort_keys=True, indent=2, separators=(',', ': '))



