# -*- coding: utf-8 -*-
#!python -m json.tool
import xlwt
import xlrd
from datetime import datetime
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

wb = xlrd.open_workbook(u'铁骑征伐.xlsx')
xlrd.Book.encoding = "utf-8"
table0 = wb.sheet_by_name(u'搜索花费初始值')
table1 = wb.sheet_by_name(u'搜索花费加成系数')
table2 = wb.sheet_by_name(u'搜索花费加成计数')
table3 = wb.sheet_by_name(u'掠夺收获')
table4 = wb.sheet_by_name(u'反抗收获')
table5 = wb.sheet_by_name(u'开荒收获')
table6 = wb.sheet_by_name(u'挖矿收获')
table7 = wb.sheet_by_name(u'奴隶清除CD')
table8 = wb.sheet_by_name(u'监狱位开启限制')
table9 = wb.sheet_by_name(u'金币生产率')
table10 = wb.sheet_by_name(u'功勋生产率')
table11 = wb.sheet_by_name(u'功勋生产率')
table12 = wb.sheet_by_name(u'功勋生产率')

print 'HUNTBASE = ['
for x in xrange(4, table0.nrows):
    print '	%s, %s, ' % (int(table0.cell(x, 0).value), int(table0.cell(x, 1).value))
print ']'

print 'HUNTRATIO = ['
for x in xrange(4, table1.nrows):
    print '	%s, %s, ' % (int(table1.cell(x, 0).value), int(table1.cell(x, 1).value))
print ']'

print 'HUNTTIMES = ['
for x in xrange(4, table2.nrows):
    print '	%s, %s, %s, ' % (int(table2.cell(x, 0).value), int(table2.cell(x, 1).value), int(table2.cell(x, 2).value))
print ']'


print 'HUNTRAPE = ['
for x in xrange(4, table3.nrows):
    print '	%s, %s, %s, %s, %s, ' % (int(table3.cell(x, 0).value), int(table3.cell(x, 1).value), int(table3.cell(x, 2).value), int(table3.cell(x, 3).value), int(table3.cell(x, 4).value))
print ']'

# print 'HUNTAGAINST = ['
# for x in xrange(4, table4.nrows):
#     print '	%s, %s, %s, %s, ' % (int(table4.cell(x, 0).value), int(table4.cell(x, 1).value), int(table4.cell(x, 2).value), int(table4.cell(x, 3).value))
# print ']'

# print 'HUNTAGAINST = ['
# for x in xrange(4, table4.nrows):
#     print '	%s, %s, %s, %s, ' % (int(table4.cell(x, 0).value), int(table4.cell(x, 1).value), int(table4.cell(x, 2).value), int(table4.cell(x, 3).value))
# print ']'

# print 'HUNTRECLAIM = ['
# for x in xrange(4, table5.nrows):
#     print '	%s, %s, %s, %s, ' % (int(table5.cell(x, 0).value), int(table5.cell(x, 1).value), int(table5.cell(x, 2).value), int(table5.cell(x, 3).value))
# print ']'

# print 'HUNTASSART = ['
# for x in xrange(4, table6.nrows):
#     print '	%s, %s, %s, %s, ' % (int(table6.cell(x, 0).value), int(table6.cell(x, 1).value), int(table6.cell(x, 2).value), int(table6.cell(x, 3).value))
# print ']'

# print 'HUNTINSTANTCOST = ['
# for x in xrange(4, table7.nrows):
#     print '	%s, %s, ' % (int(table7.cell(x, 0).value), int(table7.cell(x, 1).value))
# print ']'

# print 'PRISONID = ['
# for x in xrange(4, table8.nrows):
#     print '	%s, %s, %s, ' % (int(table8.cell(x, 0).value), int(table8.cell(x, 1).value), int(table8.cell(x, 2).value))
# print ']'

print 'GOLDOUTPUT = ['
for x in xrange(4, table9.nrows):
    print ' %s, %s, %s, %s, ' % (int(table9.cell(x, 0).value), int(table9.cell(x, 1).value), int(table9.cell(x, 2).value), int(table9.cell(x, 3).value))
print ']'

print 'FEATOUTPUT = ['
for x in xrange(4, table10.nrows):
    print ' %s, %s, %s, %s, ' % (int(table10.cell(x, 0).value), int(table10.cell(x, 1).value), int(table10.cell(x, 2).value), int(table10.cell(x, 3).value))
print ']'

