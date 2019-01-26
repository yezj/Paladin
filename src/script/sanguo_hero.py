# -*- coding: utf-8 -*-

import xlwt
import xlrd
from datetime import datetime
import json

wb = xlrd.open_workbook(u'三国武将表_后台导出.xlsx')
table = wb.sheet_by_name(u'武将导出')
# 武将
HEROE = {}
for x in xrange(1, 100):
    id = table.cell(x, 0).value
    name = table.cell(x, 1).value
    TONGYU = table.cell(x, 2).value
    WULI = table.cell(x, 3).value
    ZHILI = table.cell(x, 4).value
    ZHENGZHI = table.cell(x, 5).value
    nation = table.cell(x, 6).value
    grade = table.cell(x, 7).value
    BINGKE = table.cell(x, 8).value
    skill = table.cell(x, 9).value
    skillFinal = table.cell(x, 10).value
    star = table.cell(x, 11).value
    able = table.cell(x, 12).value
    strategy = table.cell(x, 13).value
    icon = table.cell(x, 14).value
    desc = table.cell(x, 15).value
    ct = table.cell(x, 16).value
    diff = table.cell(x, 17).value
    spec = table.cell(x, 18).value
    if int(able) == 1:
        HEROE[int(id)] = dict(id=int(id),
                              name=name,
                              TONGYU=int(TONGYU),
                              WULI=int(WULI),
                              ZHILI=int(ZHILI),
                              ZHENGZHI=int(ZHENGZHI) if ZHENGZHI else 0,
                              nation=int(nation),
                              grade=int(grade),
                              BINGKE=str(BINGKE),
                              skill=str(skill),
                              skillFinal=str(skillFinal),
                              star=int(star) if star else 0,
                              able=int(able),
                              strategy=int(strategy),
                              icon=int(icon) if icon else 0,
                              desc=desc,
                              ct=int(ct) if ct else 0,
                              diff=int(diff) if diff else 0,
                              spec=spec,
                              )

print 'HEROE =', HEROE
# print 'HEROE =', json.dumps(HEROE, sort_keys=True, indent=2, separators=(',', ': '))
# equip.append(tuple([str(one) for one in table.cell(x, y).value.split(',')]))
# 	id = table.cell(x, 5).value
# 	HEROEQUIP[str(id)] = equip
# print 'HEROEQUIP =', json.dumps(HEROEQUIP, sort_keys=True, indent=2, separators=(',', ': '))
#
