# -*- coding: utf-8 -*-
import simplejson as json
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from front.utils import E
from front.settings import settings
from local_settings import *
import redis, time, pickle

# @login_required
# def gm(request):
#     try:
#         gmlog = GmLog()
#         gmlog.user = request.user
#         gmlog.content = request.GET.items()
#         gmlog.save()
#     except Exception, e:
#         print e
# 
#     if request.GET.has_key('hero'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 hero = request.GET['hero']
#                 if hero in HERO:
#                     if request.GET['type'] == 'init':
#                         try:
#                             jheros = json.loads(user.jheros)
#                             if hero in jheros:
#                                 jheros[hero] = HERO.get(hero)
#                                 user.jheros = json.dumps(jheros)
#                                 user.save()
#                                 return HttpResponse(json.dumps(dict(error=u"武将初始化完成")), mimetype="application/json", status=200)
#                             else:
#                                 return HttpResponse(json.dumps(dict(error=u"武将不存在，请招募武将")), mimetype="application/json", status=404)
#                         except Exception, e:
#                             return HttpResponse(json.dumps(dict(error=e.message)), mimetype="application/json", status=500)
#                     elif request.GET['type'] == 'dispear':
#                         try:
#                             jheros = json.loads(user.jheros)
#                             if hero in jheros:
#                                 del jheros[hero]
#                                 user.jheros = json.dumps(jheros)
#                                 user.save()
#                                 return HttpResponse(json.dumps(dict(error=u"武将已消失")), mimetype="application/json", status=200)
#                             else:
#                                 return HttpResponse(json.dumps(dict(error=u"武将不存在，请招募武将")), mimetype="application/json", status=404)
#                         except Exception, e:
#                             return HttpResponse(json.dumps(dict(error=e.message)), mimetype="application/json", status=500)
#                     elif request.GET['type'] == 'recruit':
#                         try:
#                             jheros = json.loads(user.jheros)
#                             if hero in jheros:
#                                 return HttpResponse(json.dumps(dict(error=u"武将已存在，请输入正确的武将")), mimetype="application/json", status=404)
#                             else:
#                                 jheros[hero] = HERO.get(hero)
#                                 user.jheros = json.dumps(jheros)
#                                 user.save()
#                                 return HttpResponse(json.dumps(dict(error=u"武将招募完成")), mimetype="application/json", status=400)
#                         except Exception, e:
#                             return HttpResponse(json.dumps(dict(error=e.message)), mimetype="application/json", status=500)
#                 else:
#                     return HttpResponse(json.dumps(dict(error=u"武将不存在，请输入正确的武将")), mimetype="application/json", status=404)
#                     
#             else:
#                 return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"用户不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('prod'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 prod = request.GET['prod']
#                 num = request.GET['num']
#                 if prod in PRODS:
#                     prods = json.loads(user.jprods)
#                     if prod in prods:
#                         prods[prod] += int(num)
#                     else:
#                         prods[prod] = int(num)
#                     if prods[prod] < 0:
#                         prods[prod] = 0
#                     user.jprods = json.dumps(prods)
#                     user.save()
#                     return HttpResponse(json.dumps(dict(error=u"物品修改成功")), mimetype="application/json", status=200)
#                 else:
#                     return HttpResponse(json.dumps(dict(error=u"物品不存在")), mimetype="application/json", status=404)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('batt'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 batt = request.GET['batt']
#                 if batt in MATCH:
#                     batts = json.loads(user.jbatts)
#                     batt_list = sorted(MATCH.keys())
#                     for one in xrange(0, batt_list.index(batt)):
#                         if batts.has_key(batt_list[one]):
#                             pass
#                         else:
#                             batts[batt_list[one]] = {"gain": 3}
#                     user.jbatts = json.dumps(batts)
#                     user.save()
#                     return HttpResponse(json.dumps(dict(error=u"关卡修改成功")), mimetype="application/json", status=200)
#                 else:
#                     return HttpResponse(json.dumps(dict(error=u"关卡不存在")), mimetype="application/json", status=404)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('inst'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 inst = request.GET['inst']
#                 if inst in MATCH:
#                     insts = json.loads(user.jbatts)
#                     inst_list = sorted(MATCH.keys())
#                     for one in xrange(0, inst_list.index(inst)):
#                         if insts.has_key(inst_list[one]):
#                             pass
#                         else:
#                             insts[inst_list[one]] = {"gain": 3}
#                     user.jbatts = json.dumps(insts)
#                     user.save()
#                     return HttpResponse(json.dumps(dict(error=u"精英关卡修改成功")), mimetype="application/json", status=200)
#                 else:
#                     return HttpResponse(json.dumps(dict(error=u"精英关卡不存在")), mimetype="application/json", status=404)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('gold'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 gold = request.GET['gold']
#                 user.gold += int(gold)
#                 user.save()
#                 return HttpResponse(json.dumps(dict(error=u"金币修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('acoin'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 arena_coin = request.GET['acoin']
#                 try:
#                     arena = Arena.objects.get(user=user)
#                     arena.arena_coin += int(arena_coin)
#                     arena.save()
#                 except Exception:
#                     return HttpResponse(json.dumps(dict(error=u"竞技场用户不存在")), mimetype="application/json", status=404)
# 
#                 return HttpResponse(json.dumps(dict(error=u"巅峰币修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('rock'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 rock = request.GET['rock']
#                 user.rock += int(rock)
#                 user.save()
#                 return HttpResponse(json.dumps(dict(error=u"宝石修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('vrock'):
#         print request.GET['name'], request.GET['vrock']
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 vrock = request.GET['vrock']
#                 user.vrock += int(vrock)
#                 user.save()
#                 return HttpResponse(json.dumps(dict(error=u"VIP宝石修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('exped_coin'):
#         print request.GET['name'], request.GET['exped_coin']
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 userexped = UserExped.objects.get(user=user)
#                 userexped.exped_coin += int(request.GET['exped_coin'])
#                 userexped.save()
#                 return HttpResponse(json.dumps(dict(error=u"五关币修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('versus_coin'):
#         print request.GET['name'], request.GET['versus_coin']
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 try:
#                     userstate = UserState.objects.get(user=user)
#                     userstate.versus_coin += int(request.GET['versus_coin'])
#                     userstate.save()
#                     return HttpResponse(json.dumps(dict(error=u"群雄币修改成功")), mimetype="application/json", status=200)
#                 except Exception, e:
#                     return HttpResponse(json.dumps(dict(error=u"用户未参加群雄争霸")), mimetype="application/json", status=404)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('feat'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 feat = request.GET['feat']
#                 user.feat += int(feat)
#                 user.save()
#                 return HttpResponse(json.dumps(dict(error=u"功勋修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('xp'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 xp = request.GET['xp']
#                 uxp, ahp = E.normxp(user, user.xp + int(xp))
#                 user.xp = uxp
#                 user.save()
#                 return HttpResponse(json.dumps(dict(error=u"经验修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
# 
#     elif request.GET.has_key('hp'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 red = redis.StrictRedis(host=DATA_HOST, port='6379', db=DATA_DBID)
#                 value = int(request.GET['hp'])
#                 hpmax = E.hpmax(user.xp) + value
#                 hpup = E.hpup
#                 hptick = E.hptick
#                 hpcur = 0
#                 hp = int(red.hget("hp", user.pk))
#                 tick = 0
#                 timenow = int(time.time()) - settings["timepoch"]
#                 if not hp:
#                     hpcur = hpmax
#                 else:
#                     timestamp, hpsnap = divmod(hp, 100000)
#                     if hpsnap >= hpmax:
#                         hpcur = hpsnap
#                     else:
#                         n, r = divmod((timenow - timestamp), hptick)
#                         hpuped = hpsnap + n * hpup
#                         if hpuped < hpmax:
#                             hpcur = hpuped
#                             if r != 0:
#                                 tick = hptick - r
#                             else:
#                                 tick = hptick
#                         else:
#                             hpcur = hpmax
#                 hpcur = hpcur + value
#                 #print 'hpcur', hpcur
#                 if hpcur < hpmax:
#                     if hpcur < 0:
#                         hpcur = 0
#                     if 0 < tick < hptick:
#                         timetick = timenow - (hptick - tick)
#                     else:
#                         tick = hptick
#                         timetick = timenow
#                 else:
#                     #hpcur = hpmax
#                     timetick = timenow
#                 red.hset("hp", user.pk, timetick * 100000 + hpcur)
#                 try:
#                     user_hp = Hp.objects.get(user=user)
#                     user_hp.hp = hpcur
#                     user_hp.timestamp = timetick
#                     user_hp.save()
#                 except Exception:
#                     user_hp = Hp()
#                     user_hp.user = user
#                     user_hp.hp = hpcur
#                     user_hp.timestamp = timetick
#                     user_hp.save()
# 
#                 return HttpResponse(json.dumps(dict(error=u"体力修改成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
#     else:pass
#     return render_to_response('index.html')
# 
# @login_required
# def mail(request):
#     if request.GET.has_key('name'):
#         name = request.GET['name']
#         account = Account.objects.filter(accountid=name)
#         if account:
#             account = Account.objects.get(accountid=name)
#             if account:
#                 user = User.objects.get(pk=account.user_id)
#                 mail = Mail()
#                 mail.sender = u"倩儿"
#                 mail.to = user
#                 if request.GET.has_key('title'):
#                     mail.title = request.GET['title']
#                 if request.GET.has_key('content'):
#                     mail.content = request.GET['content']
#                 #{"gold":5000, "rock":100, "feat":100, "prods":{"04001":200}}
#                 awards = {}
#                 if request.GET.has_key('gold'):
#                     awards['gold'] = request.GET['gold']
#                 if request.GET.has_key('rock'):
#                     awards['rock'] = request.GET['rock']
#                 if request.GET.has_key('feat'):
#                     awards['feat'] = request.GET['feat']
#                 prods = {}
#                 if request.GET.has_key('prod1'):
#                     if request.GET['prod1'] in HEROEQUIP:
#                         prods[request.GET['prod1']] = request.GET['prod1_num']
#                 if request.GET.has_key('prod2'):
#                     if request.GET['prod2'] in HEROEQUIP:
#                         prods[request.GET['prod2']] = request.GET['prod2_num']
#                 if request.GET.has_key('prod3'):
#                     if request.GET['prod3'] in HEROEQUIP:
#                         prods[request.GET['prod3']] = request.GET['prod3_num']
#                 if request.GET.has_key('prod4'):
#                     if request.GET['prod4'] in HEROEQUIP:
#                         prods[request.GET['prod4']] = request.GET['prod4_num']
#                 if request.GET.has_key('prod5'):
#                     if request.GET['prod5'] in HEROEQUIP:
#                         prods[request.GET['prod5']] = request.GET['prod5_num']
#                 awards['prods'] = prods
#                 mail.jawards = json.dumps(awards)
#                 mail.save()
#                 return HttpResponse(json.dumps(dict(error=u"邮件发送成功")), mimetype="application/json", status=200)
#         else:
#             return HttpResponse(json.dumps(dict(error=u"账号不存在")), mimetype="application/json", status=404)
#     return render_to_response('mail.html')