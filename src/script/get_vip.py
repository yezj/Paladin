# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json

wb = xlrd.open_workbook(u'VIP汇总.xlsx')
table0 = wb.sheet_by_name(u'购买体力次数')
table1 = wb.sheet_by_name(u'VIP特权')
table2 = wb.sheet_by_name(u'点金手次数')
table3 = wb.sheet_by_name(u'竞技场购买次数')
table4 = wb.sheet_by_name(u'钻石商店')
table5 = wb.sheet_by_name(u'铁骑征伐重置次数')
table6 = wb.sheet_by_name(u'反抗重置次数')
table7 = wb.sheet_by_name(u'购买技能书次数')
table8 = wb.sheet_by_name(u'可开采上限')
table9 = wb.sheet_by_name(u'技能点恢复上限')
table10 = wb.sheet_by_name(u'膜拜上限')
table11 = wb.sheet_by_name(u'夜明珠购买上限')
table12	 = wb.sheet_by_name(u'精英重置上限')
table13	 = wb.sheet_by_name(u'VIP体力上限')
table14	 = wb.sheet_by_name(u'黑市刷新上限')
table15	 = wb.sheet_by_name(u'集市刷新上限')
table16	 = wb.sheet_by_name(u'杂货铺刷新上限')
table17	 = wb.sheet_by_name(u'铁骑征伐掠夺加倍')
#等级经验

print 'HPBUYTIMES = ['
for x in xrange(4, table0.nrows):
	print '	%s, %s, ' % (int(table0.cell(x, 0).value), int(table0.cell(x, 1).value))
print ']'

print 'VIP = ['
for x in xrange(3, table1.nrows):
	print '	%s, %s, ' % (int(table1.cell(x, 0).value), int(table1.cell(x, 1).value))
print ']'

print 'GOLDBUYTIMES = ['
for x in xrange(4, table2.nrows):
	print '	%s, %s, ' % (int(table2.cell(x, 0).value), int(table2.cell(x, 1).value))
print ']'

print 'ARENARESETTIMES = ['
for x in xrange(4, table3.nrows):
	print '	%s, %s, ' % (int(table3.cell(x, 0).value), int(table3.cell(x, 1).value))
print ']'

print 'ROCKSTORE = ['
for x in xrange(3, table4.nrows):
	print '	%s, %s, %s, %s, %s, %s, %s, %s, ' % (str(table4.cell(x, 0).value), str(table4.cell(x, 2).value),\
	 int(table4.cell(x, 3).value), int(table4.cell(x, 4).value), int(table4.cell(x, 5).value), int(table4.cell(x, 6).value),\
	  int(table4.cell(x, 8).value), int(table4.cell(x, 9).value))
print ']'

print 'HUNTRESETTIMES = ['
for x in xrange(4, table5.nrows):
	print '	%s, %s, ' % (int(table5.cell(x, 0).value), int(table5.cell(x, 1).value))
print ']'

print 'GAINSTESETTIMES = ['
for x in xrange(4, table6.nrows):
	print '	%s, %s, ' % (int(table6.cell(x, 0).value), int(table6.cell(x, 1).value))
print ']'

print 'BOOKBUYTIMES = ['
for x in xrange(4, table7.nrows):
	print '	%s, %s, ' % (int(table7.cell(x, 0).value), int(table7.cell(x, 1).value))
print ']'

print 'MINELIMIT = ['
for x in xrange(4, table8.nrows):
	print '	%s, %s, ' % (int(table8.cell(x, 0).value), int(table8.cell(x, 1).value))
print ']'

print 'SPLIMIT = ['
for x in xrange(4, table9.nrows):
	print '	%s, %s, ' % (int(table9.cell(x, 0).value), int(table9.cell(x, 1).value))
print ']'

print 'WORSHIPTIMES = ['
for x in xrange(5, table10.nrows):
	print '	%s, %s, ' % (int(table10.cell(x, 0).value), int(table10.cell(x, 1).value))
print ']'

print 'PEARLBUYTIMES = ['
for x in xrange(5, table11.nrows):
	print '	%s, %s, ' % (int(table11.cell(x, 0).value), int(table11.cell(x, 1).value))
print ']'

print 'HARDBATTTIMES = ['
for x in xrange(5, table12.nrows):
	print '	%s, %s, ' % (int(table12.cell(x, 0).value), int(table12.cell(x, 1).value))
print ']'

print 'HPINCR = ['
for x in xrange(5, table13.nrows):
	print '	%s, %s, ' % (int(table13.cell(x, 0).value), int(table13.cell(x, 1).value))
print ']'

print 'BMRESETTIMES = ['
for x in xrange(4, table14.nrows):
	print '	%s, %s, ' % (int(table14.cell(x, 0).value), int(table14.cell(x, 1).value))
print ']'

print 'MARKETRESETTIMES = ['
for x in xrange(4, table15.nrows):
	print '	%s, %s, ' % (int(table15.cell(x, 0).value), int(table15.cell(x, 1).value))
print ']'

print 'STORERESETTIMES = ['
for x in xrange(4, table16.nrows):
	print '	%s, %s, ' % (int(table16.cell(x, 0).value), int(table16.cell(x, 1).value))
print ']'

print 'HUNTDOUBEL = ['
for x in xrange(2, table17.nrows):
	print '	%s, %s, ' % (int(table17.cell(x, 0).value), int(table17.cell(x, 1).value))
print ']'