 # -*- coding: utf-8 -*-

import random
import time
import zlib
import uuid
import datetime
from twisted.internet import defer
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front import D
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder
from local_settings import *
@handler
class GetHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Activity get', '/activity/get/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Activity get")
    def post(self):
        try:
            channel = self.get_argument("channel", "test1")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        uid = self.uid
        user = yield self.get_user(uid)

        channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", (channel, ))
        if channels:
            channel_id, = channels[0]
        else:
            raise web.HTTPError(404)
        yield self.get_dayrecharge(user, self.arg('channel'))

        event = yield self.sql.runQuery("SELECT a.id, a.bid, a.index, a.created_at, a.ended_at, a.type FROM core_bigevent AS a,\
         core_bigevent_channels AS b WHERE b.channel_id=%s AND a.id=b.bigevent_id", (channel_id, ))
        out = {}
        for e in event:
            bigevent_id, bid, index, created_at, ended_at, type = e
            created_at = int(time.mktime(created_at.timetuple()))
            ended_at = int(time.mktime(ended_at.timetuple()))
            now = int(time.mktime(datetime.datetime.now().timetuple()))
            if now >= created_at and now <= ended_at:
                activity = {}
                res = yield self.sql.runQuery("SELECT b.bid, a.rid, a.name, a.gold, a.rock, a.feat, a.hp, a.prods, a.nums, a.total,\
                             a.type FROM core_inpour AS a, core_bigevent AS b WHERE a.bigevent_id=%s AND a.bigevent_id=b.id", (bigevent_id, ))
                if res:
                    for r in res:
                        bid, rid, name, gold, rock, feat, hp, prods, nums, total, atype = r
                        if len(prods):
                            prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
                        res = yield self.sql.runQuery("SELECT * FROM core_userinpourrecord WHERE user_id=%s AND bid=%s AND rid=%s", (uid, bid, rid))
                        if res:
                            status = 1
                        else:
                            inpour = yield self.get_inpour(user)
                            if inpour.get(bid, 0) >= int(total):
                                status = 0
                            else:
                                status = -1
                        activity[rid] = dict(name=name, gold=gold, rock=rock, feat=feat, hp=hp, prods=prods, total=total, \
                                          type=atype, status=status)

                res = yield self.sql.runQuery("SELECT b.bid, a.rid, a.name, a.prod, a.num, a.total FROM core_zhangfei AS a,\
                    core_bigevent AS b WHERE a.bigevent_id=%s AND a.bigevent_id=b.id", (bigevent_id, ))
                if res:
                    #print 'res', res
                    for r in res:
                        bid, rid, name, prod, num, total = r
                        prods = {}
                        if len(prod):
                            prods[prod] = int(num)

                        res = yield self.sql.runQuery("SELECT * FROM core_userzhangfeirecord WHERE user_id=%s AND bid=%s", (uid, bid))
                        if res:
                            status = 1
                        else:
                            if user['rock'] >= int(total):
                                status = 0
                            else:
                                status = -1

                        activity[rid] = dict(name=name, gold=0, rock=0, feat=0, hp=0, prods=prods, total=total, type=0,\
                                             status=status)

                res = yield self.sql.runQuery("SELECT b.bid, a.id, a.rid, a.name, a.gold, a.rock, a.feat, a.hp, a.prods, a.nums,\
                  a.total FROM core_consume AS a, core_bigevent AS b WHERE a.bigevent_id=%s AND a.bigevent_id=b.id", (bigevent_id, ))
                if res:
                    for r in res:
                        bid, iid, rid, name, gold, rock, feat, hp, prods, nums, total = r
                        if len(prods):
                            prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
                        consumerecord = yield self.sql.runQuery("SELECT * FROM core_userconsumerecord WHERE user_id=%s AND bid=%s AND rid=%s",\
                                                                (uid, bid, rid))
                        if not consumerecord:
                            consume = yield self.get_consume(user)
                            if consume.get(bid, 0) >= int(total):
                                status = 0
                            else:
                                status = -1
                        else:
                            status = 1

                        activity[rid] = dict(name=name, gold=gold, rock=rock, feat=feat, hp=hp, prods=prods, total=total, type=0,\
                                             status=status)
                        #print 'activity', activity
                res = yield self.sql.runQuery("SELECT id, rid, name, gold, rock, feat, hp, prods, nums, exgold, exrock, exfeat, exhp,\
                  exprods, exnums, times, bigevent_id FROM core_propexchange WHERE bigevent_id=%s", (bigevent_id, ))
                if res:
                    for r in res:
                        iid, rid, name, gold, rock, feat, hp, prods, nums, exgold, exrock, exfeat, exhp, exprods, exnums, max_times, bigevent_id = r
                        if len(prods):
                            prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
                        else:
                            prods = {}
                        if len(exprods):
                            exprods = dict(zip(exprods.split(','), [int(n) for n in exnums.split(',')]))
                        else:
                            exprods = {}
                        propexchange = yield self.sql.runQuery("SELECT times FROM core_userpropexchange WHERE user_id=%s AND rid=%s LIMIT 1",\
                                                               (uid, rid))
                        if not propexchange:
                            now_times = 0
                        else:
                            now_times, = propexchange[0]
                        left_times = max_times - now_times
                        if left_times < 0:
                            left_times = 0
                        activity[rid] = dict(name=name, gold=gold, rock=rock, feat=feat, hp=hp, prods=prods, exprods=exprods, exgold=exgold,\
                                             exrock=exrock, exfeat=exfeat, exhp=exhp, max_times=max_times, left_times=left_times)

                out[bid] = dict(index=index, activity=activity, created_at=created_at, ended_at=ended_at, type=type)

        ret = dict(out=out, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)

@handler
class InpourHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Activity inpour', '/activity/inpour/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('aid', True, str, '80001', '80001', 'aid'),
        Param('rid', True, str, '80001', '80001', 'rid'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Activity inpour")
    def post(self):

        uu = self.get_argument("uu", None)
        if uu:
            uu = "%s:%s:%s" % (self.request.remote_ip, self.request.path, uu)
            ret = yield self.get_uu(uu)
            if ret:
                self.write(ret)
                return
        try:
            bid = self.get_argument("aid")
            rid = self.get_argument("rid")
            channel = self.get_argument("channel", "test1")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", (channel, ))
        if channels:
            channel_id, = channels[0]
        else:
            raise web.HTTPError(404)

        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT * FROM core_userinpourrecord WHERE user_id=%s AND bid=%s AND rid=%s", (uid, bid, rid))
        if res:
            self.write(dict(err=E.ERR_ACTIVITY_ALREADYGOT, msg=E.errmsg(E.ERR_ACTIVITY_ALREADYGOT)))
            return
        else:
            res = yield self.sql.runQuery("SELECT a.gold, a.rock, a.feat, a.hp, a.prods, a.nums, a.total FROM core_inpour AS a WHERE a.rid=%s LIMIT 1", (rid, ))
            if res:
                gold, rock, feat, hp, prods, nums, total = res[0]
                inpour = yield self.get_inpour(user)
                if inpour.get(bid, 0) < int(total):
                    self.write(dict(err=E.ERR_ACTIVITY_DISSATISFY, msg=E.errmsg(E.ERR_ACTIVITY_DISSATISFY)))
                    return
                if len(prods) > 0:
                    prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
                else:
                    prods = {}
                user['gold'] += gold
                user['rock'] += rock
                user['feat'] += feat
                hp, tick = yield self.add_hp(user, hp, u'大事件累充奖励')
                hp, tick = yield self.get_hp(user)
                for prod, n in prods.items():
                    if prod in user['prods']:
                        user['prods'][prod] += n
                    else:
                        user['prods'][prod] = n
                    if user['prods'][prod] > 9999:
                        user['prods'][prod] = 9999
                    elif user['prods'][prod] == 0:
                        del user['prods'][prod]
                    else:pass
                yield self.set_inpourresult(user, bid, rid)
                cuser = dict(gold=user['gold'], rock=user['rock'], hp=hp, feat=user['feat'], prods=user['prods'], recharge=user['recharge'])
                nuser = dict(gold=user['gold'], rock=user['rock'], feat=user['feat'], prods=user['prods'], recharge=user['recharge'])
                logmsg = u'大事件:累充%s奖励' % rid
                yield self.set_user(uid, logmsg, **nuser)
                ret = dict(out=dict(user=cuser), timestamp=int(time.time()))
                reb = zlib.compress(escape.json_encode(ret))
                self.write(ret)
            else:
                self.write(dict(err=E.ERR_ACTIVITY_NOTFOUND, msg=E.errmsg(E.ERR_ACTIVITY_NOTFOUND)))
                return

@handler
class ZhangfeiHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Activity zhangfei', '/activity/zhangfei/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('aid', True, str, '80001', '80001', 'aid'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Activity zhangfei")
    def post(self):

        uu = self.get_argument("uu", None)
        if uu:
            uu = "%s:%s:%s" % (self.request.remote_ip, self.request.path, uu)
            ret = yield self.get_uu(uu)
            if ret:
                self.write(ret)
                return
        try:
            bid = self.get_argument("aid")
            channel = self.get_argument("channel", "test1")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", (channel, ))
        if channels:
            channel_id, = channels[0]
        else:
            raise web.HTTPError(404)

        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT * FROM core_userzhangfeirecord WHERE user_id=%s AND bid=%s", (uid, bid))
        if res:
            self.write(dict(err=E.ERR_ACTIVITY_ALREADYGOT, msg=E.errmsg(E.ERR_ACTIVITY_ALREADYGOT)))
            return

        res = yield self.sql.runQuery("SELECT a.total, a.prod, a.num FROM core_zhangfei AS a LIMIT 1")
        if res:
            total, prod, num = res[0]
            if user['rock'] < total:
                self.write(dict(err=E.ERR_NOTENOUGH_ROCK, msg=E.errmsg(E.ERR_NOTENOUGH_ROCK)))
                return
            if prod in user['prods']:
                user['prods'][prod] += int(num)
            else:
                user['prods'][prod] = int(num)
            if user['prods'][prod] > 9999:
                user['prods'][prod] = 9999
            cuser = dict(prods=user['prods'])
            logmsg = u'大事件:送武将%s奖励' % bid
            yield self.set_zhangfeiresult(user, bid)
            yield self.set_user(uid, logmsg, **cuser)
            ret = dict(out=dict(user=cuser), timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)
        else:
            self.write(dict(err=E.ERR_ACTIVITY_NOTFOUND, msg=E.errmsg(E.ERR_ACTIVITY_NOTFOUND)))
            return

@handler
class ActivityHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Activity consume', '/activity/consume/', [
        Param('channel', False, str, 'putaogame', 'putaogame', 'channel'),
        Param('aid', True, str, '80001', '80001', 'aid'),
        Param('rid', True, str, '80001', '80001', 'rid'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
    ], filters=[ps_filter], description="Activity consume")
    def post(self):

        uu = self.get_argument("uu", None)
        if uu:
            uu = "%s:%s:%s" % (self.request.remote_ip, self.request.path, uu)
            ret = yield self.get_uu(uu)
            if ret:
                self.write(ret)
                return
        try:
            bid = self.get_argument("aid")
            rid = self.get_argument("rid")
            channel = self.get_argument("channel", "putaogame")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", (channel, ))
        if channels:
            channel_id, = channels[0]
        else:
            raise web.HTTPError(404)

        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT * FROM core_userconsumerecord WHERE user_id=%s AND bid=%s AND rid=%s", (uid, bid, rid))
        if not res:
            res = yield self.sql.runQuery("SELECT gold, rock, feat, hp, prods, nums, total FROM core_consume WHERE rid=%s LIMIT 1", (rid, ))
            if not res:
                self.write(dict(err=E.ERR_ACTIVITY_NOTFOUND, msg=E.errmsg(E.ERR_ACTIVITY_NOTFOUND)))
                return
            gold, rock, feat, hp, prods, nums, total = res[0]
            consume = yield self.get_consume(user)
            print 'consume', consume, rid, bid
            if consume.get(bid, 0) < total:
                self.write(dict(err=E.ERR_ACTIVITY_DISSATISFY, msg=E.errmsg(E.ERR_ACTIVITY_DISSATISFY)))
                return
            if len(prods) > 0:
                prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
            else:
                prods = {}
            user['gold'] += gold
            user['rock'] += rock
            user['feat'] += feat
            cost = hp
            hp, tick = yield self.add_hp(user, hp, u'大事件累消奖励')
            hp, tick = yield self.get_hp(user)
            for prod, n in prods.items():
                if prod in user['prods']:
                    user['prods'][prod] += n
                else:
                    user['prods'][prod] = n
                if user['prods'][prod] > 9999:
                    user['prods'][prod] = 9999
                elif user['prods'][prod] == 0:
                    del user['prods'][prod]
                else:pass

            consume = yield self.set_consumeresult(user, bid, rid)
            cuser = dict(gold=user['gold'], rock=user['rock'], hp=hp, feat=user['feat'], prods=user['prods'])
            nuser = dict(gold=user['gold'], rock=user['rock'], feat=user['feat'], prods=user['prods'])
            logmsg = u'大事件:累消%s奖励' % rid
            yield self.set_user(uid, logmsg, **nuser)
            cuser['consume'] = consume
            ret = dict(out=dict(user=cuser), timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)
        else:
            self.write(dict(err=E.ERR_ACTIVITY_ALREADYGOT, msg=E.errmsg(E.ERR_ACTIVITY_ALREADYGOT)))
            return

@handler
class ExchangeHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Activity set', '/activity/exchange/', [
        Param('channel', False, str, 'putaogame', 'putaogame', 'channel'),
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '4GRwMApTJ3VpZCcKcDEKUyc4NycKcDIKcyK', '_sign'),
        Param('rid', True, str, '', 1, 'rid'),
    ], filters=[ps_filter], description="Activity exchange")
    def post(self):
        try:
            rid = self.get_argument("rid")
            print 'rid', rid
        except Exception:
            raise web.HTTPError(400, "Argument error")
        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT gold, rock, feat, hp, prods, nums, exgold, exrock, exfeat, exhp, exprods,\
          exnums, times FROM core_propexchange WHERE rid=%s LIMIT 1", (rid,))
        if res:
            gold, rock, feat, hp, prods, nums, exgold, exrock, exfeat, exhp, exprods, exnums, max_times = res[0]
            if user['gold'] < gold:
                self.write(dict(err=E.ERR_NOTENOUGH_GOLD, msg=E.errmsg(E.ERR_NOTENOUGH_GOLD)))
                return
            if user['rock'] < rock:
                self.write(dict(err=E.ERR_NOTENOUGH_ROCK, msg=E.errmsg(E.ERR_NOTENOUGH_ROCK)))
                return
            if user['feat'] < feat:
                self.write(dict(err=E.ERR_NOTENOUGH_FEAT, msg=E.errmsg(E.ERR_NOTENOUGH_FEAT)))
                return
            propexchange = yield self.sql.runQuery("SELECT times FROM core_userpropexchange WHERE user_id=%s AND rid=%s LIMIT 1", \
                                                    (uid, rid))
            if propexchange:
                times, = propexchange[0]
            else:
                times = 0
            if times >= max_times:
                self.write(dict(err=E.ERR_MAXEXCHANGETIMES_ALREADYGOT, msg=E.errmsg(E.ERR_MAXEXCHANGETIMES_ALREADYGOT)))
                return
            if len(prods) > 0:
                prod_list = prods.split(',')
                prods = dict(zip(prods.split(','), [int(n) for n in nums.split(',')]))
                for key, value in prods.iteritems():
                    if key in user['prods']:
                        if user['prods'][key] < int(prods[key]):
                            self.write(dict(err=E.ERR_NOTENOUGH_PROD, msg=E.errmsg(E.ERR_NOTENOUGH_PROD)))
                            return
                    else:
                        self.write(dict(err=E.ERR_NOTENOUGH_PROD, msg=E.errmsg(E.ERR_NOTENOUGH_PROD)))
                        return
            else:
                prods = {}
            if len(exprods) > 0:
                exprods = dict(zip(exprods.split(','), [int(n) for n in exnums.split(',')]))
            else:
                exprods = {}

            now_hp, tick = yield self.get_hp(user)
            if now_hp < hp:
                self.write(dict(err=E.ERR_NOTENOUGH_HP, msg=E.errmsg(E.ERR_NOTENOUGH_HP)))
                return
            user['gold'] += exgold - gold
            user['rock'] += exrock - rock
            user['feat'] += exfeat - feat
            hp, tick = yield self.add_hp(user, exhp-hp, u'大事件物品兑换')

            for pid, num in prods.iteritems():
                if pid in user['prods'] and user['prods'][pid] >= int(num):
                    user['prods'][pid] -= int(num)
                else:
                    self.write(dict(err=E.ERR_NOTENOUGH_PROD, msg=E.errmsg(E.ERR_NOTENOUGH_PROD)))
                    return
                if user['prods'][pid] == 0:
                    del user['prods'][pid]

            for expid, exnum in exprods.iteritems():
                if expid in user['prods']:
                    user['prods'][expid] += int(exnum)
                else:
                    user['prods'][expid] = int(exnum)
                if user['prods'][expid] > 9999:
                    user['prods'][expid] = 9999
                elif user['prods'][expid] == 0:
                    del user['prods'][expid]
            now_times = yield self.set_propexchange(user, rid)
            left_times = max_times - now_times
            if left_times < 0:
                left_times = 0
            cuser = dict(gold=user['gold'], rock=user['rock'], hp=hp, feat=user['feat'], prods=user['prods'])
            nuser = dict(gold=user['gold'], rock=user['rock'], feat=user['feat'], prods=user['prods'])
            logmsg = u'大事件:物品%s兑换' % rid
            yield self.set_user(uid, logmsg, **nuser)
            yield self.set_consume(user, rock)
            consume = yield self.get_consume(user)
            cuser['consume'] = consume
            ret = dict(user=cuser, max_times=max_times, left_times=left_times, timestamp=int(time.time()))
            self.write(ret)
        else:
            raise web.HTTPError(400, "Argument error")