# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'巅峰对决.xlsx')
xlrd.Book.encoding = "utf-8"
table0 = wb.sheet_by_name(u'商店出售物品')
table1 = wb.sheet_by_name(u'商店刷新花费')
table2 = wb.sheet_by_name(u'购买花费')
table3 = wb.sheet_by_name(u'对手刷新规则')
table4 = wb.sheet_by_name(u'历史最高奖励')
table5= wb.sheet_by_name(u'每日排名奖励')
table6 = wb.sheet_by_name(u'膜拜类型')
table7 = wb.sheet_by_name(u'被膜拜奖励')
ARENAPROD = {}

for x in xrange(3, table0.nrows):
    ARENAPROD[str(table0.cell(x, 0).value)] = dict(num=int(table0.cell(x, 1).value), arena_coin=int(table0.cell(x, 2).value), name=table0.cell(x, 4).value)

print 'ARENAPROD =', json.dumps(ARENAPROD, sort_keys=True, indent=2, separators=(',', ': '), ensure_ascii=False)

print 'ARENAREFRESH = ['
for x in xrange(3, table1.nrows):
    print '	%s, %s, ' % (int(table1.cell(x, 3).value), int(table1.cell(x, 4).value))
print ']'

print 'ARENARESET = ['
for x in xrange(3, table1.nrows):
    print '	%s, %s, ' % (int(table2.cell(x, 0).value), int(table2.cell(x, 1).value))
print ']'

print 'ARENARULE = ['
for x in xrange(3, table3.nrows):
    print '	%s, %s, %s, %s, %s, %s, %s, %s, ' % (int(table3.cell(x, 0).value), int(table3.cell(x, 1).value), int(table3.cell(x, 2).value), \
                                                  int(table3.cell(x, 3).value), int(table3.cell(x, 4).value), int(table3.cell(x, 5).value), \
                                                  int(table3.cell(x, 6).value), int(table3.cell(x, 7).value))
print ']'

print 'ARENAHISTORY = ['
for x in xrange(3, table4.nrows):
    print '	%s, %s, %s, %s, ' % (int(table4.cell(x, 0).value), int(table4.cell(x, 1).value), int(table4.cell(x, 2).value), float(table4.cell(x, 3).value))
print ']'

print 'ARENADAY = ['
for x in xrange(3, table5.nrows):
    print '	%s, %s, %s, %s, %s, %s, ' % (int(table5.cell(x, 0).value), int(table5.cell(x, 1).value), int(table5.cell(x, 2).value), int(table5.cell(x, 3).value),  int(table5.cell(x, 4).value),  int(table5.cell(x, 5).value))
print ']'

print 'WORSHIPTYPE = ['
for x in xrange(4, table6.nrows):
    print ' %s, %s, %s, %s, ' % (int(table6.cell(x, 0).value), int(table6.cell(x, 1).value), int(table6.cell(x, 2).value), int(table6.cell(x, 3).value))
print ']'

print 'WORSHIPAWRAD = ['
for x in xrange(5, table7.nrows):
    print ' %s, %s, %s, ' % (int(table7.cell(x, 0).value), int(table7.cell(x, 1).value), int(table7.cell(x, 2).value))
print ']'