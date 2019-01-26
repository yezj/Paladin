from core.models import *
import json
us = User.objects.all()
# li = ['80001', '80002', '80003', '80004', '80005', '80006']
# for u in us:
#     jrecharge = json.loads(u.jrecharge)
#     if type(jrecharge) is int:
#         pass
#     else:
#         for l in li:
#             if l in jrecharge:
#                 if jrecharge[l] == 1:
#                     inp = UserInpourRecord()
#                     inp.user = u
#                     inp.bid = '10010001'
#                     inp.rid = l
#                     inp.save()
for u in us:
    user = UserInpour.objects.filter(user=u, bid='10010001')
    if user:
        inp = UserInpour.objects.get(user=u, bid='10010001')
        inp.rock=u.vrock
        inp.save()
    else:
        inp = UserInpour()
        inp.user = u
        inp.bid='10010001'
        inp.rock=u.vrock
        inp.save()

