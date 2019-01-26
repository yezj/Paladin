# -*- coding: utf-8 -*-

import random
import time
import zlib
import uuid
import json
import datetime

import pickle
import copy
from twisted.internet import defer
from twisted.python import log
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front.wiapi import *
from front import D
from front.handlers.base import ApiHandler, ApiJSONEncoder


@handler
class InfoHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.token
    @api('Arena info', '/arena/info/', [
        Param('stage_id', True, str, '010208_0', '010208_0', 'stage_id'),
        Param('access_token', True, str, '010208_0', '010208_0', 'access_token'),
        Param('idcard', True, str, '010208_0', '010208_0', 'idcard'),
        Param('user_id', True, str, '7', '7', 'user_id'),
        Param('heros1P', True, str, '[]', '[]', 'heros1P'),
    ], filters=[ps_filter], description="Get arena info")
    def post(self):
        try:
            stage_id = self.get_argument("stage_id")
            opponent_Id_card = self.get_argument("opponent_Id_card", None)
            access_token = self.get_argument("access_token")
            user_id = self.get_argument("user_id")
            idcard = self.get_argument("idcard")
            heros1P = self.get_argument("heros1P")

        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        query = """SELECT rank, star FROM core_arena WHERE user_id=%s LIMIT 1"""
        res = yield self.sql.runQuery(query, (user_id,))
        if res:
            rank, star = res[0]
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        query = """SELECT MIN(id), MAX(id) FROM core_arena WHERE user_id<>%s AND rank=%s AND star>=%s"""
        res = yield self.sql.runQuery(query, (user_id, rank, star))
        if res:
            min_id, max_id = res[0]
            seed_id = random.randint(min_id, max_id)
            query = """SELECT user_id FROM core_arena WHERE id>=%s LIMIT 1"""
            res = yield self.sql.runQuery(query, (seed_id,))
            if res:
                user_id, = res[0]
                cuser = yield self.get_user(user_id)
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        arenatime = yield self.redis.get('arenatime:%s' % user_id) or 0
        arenatimes = yield self.redis.get('arenatimes:%s' % user_id) or 0
        if not arenatime:
            arenatime = 0
        if not arenatimes:
            arenatimes = 0
            yield self.redis.set('arenatimes:%s' % user_id, arenatimes)
        if arenatimes >= E.limit_by_arena:
            cuser['interval'] = 0
            cuser['left_times'] = 0
            cuser['total_times'] = E.limit_by_arena
        else:
            interval_times = int(time.time()) - arenatime
            if interval_times >= E.timer_by_arena:
                interval = 0
            else:
                interval = E.timer_by_arena - interval_times
            if arenatimes == 0:
                interval = 0
            if interval < 0:
                interval = 0
            cuser['interval'] = interval
            cuser['left_times'] = E.limit_by_arena - arenatimes
            cuser['total_times'] = E.limit_by_arena
        # if user['xp'] / 100000 < D.ARENA_OPEN_LIMIT:
        #     arenamark = 0
        # else:
        #     arenamark = yield self.redis.get('arenamark:%s' % uid)
        #     if not arenamark:
        #         arenamark = 0
        reset_times = yield self.redis.get('arenaresets:%s' % user_id)
        if not reset_times:
            reset_times = 0
        cuser['reset_times'] = reset_times
        ret = dict(user=cuser, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class GetHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.token
    @api('Arena get', '/arena/get/', [
        Param('stage_id', True, str, '010208_0', '010208_0', 'stage_id'),
        Param('access_token', True, str, '010208_0', '010208_0', 'access_token'),
        Param('idcard', True, str, '010208_0', '010208_0', 'idcard'),
        Param('user_id', True, str, '7', '7', 'user_id'),
        Param('heros1P', True, str, '[]', '[]', 'heros1P'),
        Param('cuser_id', True, str, '1', '1', 'cuser_id'),
        Param('heros2P', True, str, '[]', '[]', 'heros2P'),
    ], filters=[ps_filter], description="Arena get")
    def post(self):
        try:
            user_id = self.get_argument("user_id")
            cuser_id = self.get_argument("cuser_id")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        user = yield self.get_user(user_id)
        cuser = yield self.get_user(cuser_id)

        arenatimes = yield self.redis.get('arenatimes:%s' % user_id)
        if arenatimes:
            if arenatimes >= E.limit_by_arena:
                self.write(dict(err=E.ERR_DISSATISFY_MAXENTRY, msg=E.errmsg(E.ERR_DISSATISFY_MAXENTRY)))
                return
        arenatime = yield self.redis.get('arenatime:%s' % user_id)
        if arenatime:
            if arenatime != 0:
                interval_times = int(time.time()) - arenatime
                if interval_times >= E.timer_by_arena:
                    interval = 0
                else:
                    interval = E.timer_by_arena - interval_times
                if arenatimes == 0:
                    interval = 0
                if interval != 0:
                    self.write(dict(err=E.ERR_DISSATISFY_COLDDOWN, msg=E.errmsg(E.ERR_DISSATISFY_COLDDOWN)))
                    return
            else:
                self.write(dict(err=E.ERR_DISSATISFY_COLDDOWN, msg=E.errmsg(E.ERR_DISSATISFY_COLDDOWN)))
                return
        else:
            arenatime = 0

        cwork = E.tagworks(user, {'ARENA': 1})
        if cwork:
            nuser = dict(works=user['works'])
            yield self.set_user(user_id, **nuser)

        fid = uuid.uuid4().hex
        yield self.set_matchflush(fid, cuser_id)
        ret = dict(out=dict(flush=fid), timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)

        yield self.redis.incr('arenatimes:%s' % user_id)
        yield self.redis.set('arenatime:%s' % user_id, int(time.time()))


@handler
class SetHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.token
    @api('Arena set', '/arena/set/', [
        Param('fid', True, str, '0cb9abd2f380497198c769344279a7cd', '0cb9abd2f380497198c769344279a7cd', 'fid'),
        Param('access_token', True, str, '010208_0', '010208_0', 'access_token'),
        Param('idcard', True, str, '010208_0', '010208_0', 'idcard'),
        Param('user_id', True, str, '7', '7', 'user_id'),
        Param('result', True, int, 1, 1, 'result'),
    ], filters=[ps_filter], description="Arena set")
    def post(self):
        try:
            fid = self.get_argument("fid")
            user_id = self.get_argument("user_id")
            result = self.get_argument("result")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        user = yield self.get_user(user_id)
        cuser_id = yield self.get_matchflush(fid)
        cuser = yield self.get_user(cuser_id)
        # print 'result', result
        if int(result) == 1:
            if str(user_id) != str(cuser_id):
                if cuser:
                    now_rank, before_rank, last_rank = yield self.update_arenaresult(user, cuser, E.true)
                    if now_rank < before_rank:
                        before = before_rank
                        up = before - now_rank
                        now_rank, before_rank, last_rank, awards = yield self.update_arenarank(user, now_rank,
                                                                                               before_rank, last_rank)
                        # sender, to_id, title, content, awards
                        mail = D.ARENAMAIL
                        content = mail['content'] % (now_rank, up)
                        if awards:
                            yield self.send_mails(mail['sender'], user_id, mail['title'], content, awards)
                        if now_rank == 1:
                            yield self.redis.rpush("message", pickle.dumps(
                                [D.ARENATOPMESSAGE[0] % user['nickname'], D.ARENATOPMESSAGE[1] % int(time.time())]))
                        cuser = dict(now_rank=now_rank, before_rank=before, last_rank=last_rank, up=up, awards=awards)
                    else:
                        cuser = dict(now_rank=now_rank, before_rank=before_rank, last_rank=last_rank)
                arenawintimes = yield self.redis.incr('arenawintimes:%s' % user_id)
                if arenawintimes >= 3:
                    yield self.redis.rpush("message", pickle.dumps(
                        [D.ARENAWINMESSAGE[0] % user['nickname'], D.ARENAWINMESSAGE[1] % int(time.time())]))
                yield self.set_arena(user_id)
            ret = dict(user=cuser, timestamp=int(time.time()))

        else:
            yield self.update_arenaresult(user, cuser, E.false)
            yield self.redis.delete('arenawintimes:%s' % user_id)
            ret = dict(timestamp=int(time.time()))
        yield self.redis.set('arenamark:%s' % cuser_id, E.true)
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)

@handler
class MatchHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena match', '/arena/match/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
        Param('cid', True, str, '1', '1', 'cid'),
        Param('hids', True, str, '01002,01001', '01002,01001', 'hids'),
        Param('formation', True, str, '1', '1', 'formation'),
        Param('chids', True, str, '01002,01001', '01002,01001', 'chids'),
        Param('cformation', True, str, '1', '1', 'formation'),
        Param('result', True, int, 1, 1, 'result'),
    ], filters=[ps_filter], description="Arena match")
    def post(self):
        try:
            cid = self.get_argument("cid")
            result = self.get_argument("result")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        uid = self.uid
        user = yield self.get_user(uid)

        if self.arg('cid') == str(uid):
            raise web.HTTPError(400, "Argument error")
        arenatimes = yield self.redis.get('arenatimes:%s' % uid)
        if arenatimes:
            if arenatimes >= E.limit_by_arena:
                self.write(dict(err=E.ERR_DISSATISFY_MAXENTRY, msg=E.errmsg(E.ERR_DISSATISFY_MAXENTRY)))
                return
        arenatime = yield self.redis.get('arenatime:%s' % uid)
        if arenatime:
            if arenatime != 0:
                interval_times = int(time.time()) - arenatime
                if interval_times >= E.timer_by_arena:
                    interval = 0
                else:
                    interval = E.timer_by_arena - interval_times
                if arenatimes == 0:
                    interval = 0
                if interval != 0:
                    self.write(dict(err=E.ERR_DISSATISFY_COLDDOWN, msg=E.errmsg(E.ERR_DISSATISFY_COLDDOWN)))
                    return
            else:
                self.write(dict(err=E.ERR_DISSATISFY_COLDDOWN, msg=E.errmsg(E.ERR_DISSATISFY_COLDDOWN)))
                return
        else:
            arenatime = 0
        if int(result):
            cuser = yield self.get_user(self.arg('cid'))
            if cuser:
                now_rank, before_rank, last_rank = yield self.update_arenaresult(user, self.arg('cid'),
                                                                                 self.arg('result'))
                if now_rank < before_rank:
                    before = before_rank
                    up = before - now_rank
                    now_rank, before_rank, last_rank, awards = yield self.update_arenarank(user, now_rank, before_rank,
                                                                                           last_rank)
                    # sender, to_id, title, content, awards
                    mail = D.ARENAMAIL
                    content = mail['content'] % (now_rank, up)
                    if up > 0:
                        yield self.send_mails(mail['sender'], uid, mail['title'], content, awards)
                        yield self.update_mails(cuser)
                    cuser = dict(now_rank=now_rank, before_rank=before, last_rank=last_rank, up=up, awards=awards,
                                 mails=cuser['mails'])
                else:
                    cuser = dict(now_rank=now_rank, before_rank=before_rank, last_rank=last_rank)
                nuser = dict(works=user['works'])

                yield self.set_arena(uid)
                cwork = E.tagworks(user, {'ARENA': 1})
                if cwork:
                    cuser['works'] = user['works']
                    yield self.set_user(uid, **nuser)

                ret = dict(user=cuser, timestamp=int(time.time()))
                reb = zlib.compress(escape.json_encode(ret))
                self.write(ret)
        else:
            ret = dict(timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)

        yield self.redis.set('arenamark:%s' % cid, E.true)
        yield self.redis.incr('arenatimes:%s' % uid)
        yield self.redis.set('arenatime:%s' % uid, int(time.time()))


@handler
class GuardHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena guard', '/arena/guard/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
        Param('heros', True, str, '01001', '01001', 'heros'),
        Param('formation', True, str, '1', '1', 'formation'),
    ], filters=[ps_filter], description="Arena guard")
    def post(self):
        try:
            heros = self.get_argument("heros")
            positions = self.get_argument("positions")
            formation = self.get_argument("formation")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        if len(self.arg('heros').split(',')) > 5:
            raise web.HTTPError(400, "Argument error")
        uid = self.uid
        user = yield self.get_user(uid)
        if heros and positions and formation:
            yield self.update_arenaguard(uid, self.arg('heros').split(','), self.arg('positions').split(','), formation)
            ret = dict(timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)
        else:
            raise web.HTTPError(400, "Argument error")


@handler
class ResetHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena reset', '/arena/reset/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena reset")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        arenaresets = yield self.redis.get('arenaresets:%s' % uid)
        if arenaresets:
            if arenaresets >= E.arenamaxtimes(user['vrock']):
                self.write(dict(err=E.ERR_DISSATISFY_MAXRESETS, msg=E.errmsg(E.ERR_DISSATISFY_MAXRESETS)))
                return
        else:
            arenaresets = 0
            yield self.redis.set('arenaresets:%s' % uid, arenaresets)
        if user['rock'] < D.ARENAREFRESH[arenaresets * 2 + 1]:
            self.write(dict(err=E.ERR_NOTENOUGH_ROCK, msg=E.errmsg(E.ERR_NOTENOUGH_ROCK)))
            return
        user['rock'] -= D.ARENAREFRESH[arenaresets * 2 + 1]
        consume = yield self.set_consume(user, D.ARENAREFRESH[arenaresets * 2 + 1])
        cuser = dict(rock=user['rock'])
        yield self.redis.incr('arenaresets:%s' % uid)
        yield self.redis.set('arenatimes:%s' % uid, 0)
        logmsg = u'竞技场:第%s次重置次数' % (arenaresets + 1)
        yield self.set_user(uid, logmsg, **cuser)
        cuser['left_times'] = E.limit_by_arena
        consume = yield self.get_consume(user)
        cuser['consume'] = consume
        ret = dict(user=cuser, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class ResetCDHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena resetcd', '/arena/resetcd/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena resetcd")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        if user['rock'] < E.cost_for_resetcd:
            self.write(dict(err=E.ERR_NOTENOUGH_ROCK, msg=E.errmsg(E.ERR_NOTENOUGH_ROCK)))
            return
        arenatimes = yield self.redis.get('arenatimes:%s' % uid)
        arenatime = yield self.redis.get('arenatime:%s' % uid)
        if not arenatimes:
            arenatimes = 0

        if arenatimes > E.limit_by_arena:
            self.write(dict(err=E.ERR_DISSATISFY_MAXENTRY, msg=E.errmsg(E.ERR_DISSATISFY_MAXENTRY)))
            return
        yield self.redis.set('arenatime:%s' % uid, 0)
        user['rock'] -= E.cost_for_resetcd
        consume = yield self.set_consume(user, E.cost_for_resetcd)
        cuser = dict(rock=user['rock'])
        logmsg = u'竞技场:重置冷却时间'
        yield self.set_user(uid, logmsg, **cuser)

        cuser['interval'] = 0
        cuser['left_times'] = E.limit_by_arena - arenatimes
        cuser['total_times'] = E.limit_by_arena
        consume = yield self.get_consume(user)
        cuser['consume'] = consume
        ret = dict(user=cuser, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class RankHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena rank', '/arena/rank/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena rank")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT user_id, now_rank FROM core_arena ORDER BY now_rank LIMIT 50", (uid,))
        rank = {}
        if res:
            for r in res:
                arenas = yield self.get_arena(r[0])
                cuser = yield self.get_user(r[0])
                beautys = yield self.get_beautys(cuser)
                if arenas:
                    guards = {}
                    for key in arenas['guards'].keys():
                        guards[key] = cuser['heros'].get(key, {})
                    rank[r[1]] = dict(uid=arenas['uid'], guards=guards, now_rank=arenas['now_rank'],
                                      nickname=arenas['nickname'], \
                                      xp=arenas['xp'], avat=arenas['avat'], win_times=arenas['win_times'],
                                      formation=arenas['formation'], \
                                      beautys=beautys)
        ret = dict(rank=rank, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class ChangeHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena change', '/arena/change/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena change")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)

        arenas = yield self.get_arena(uid)
        if arenas:
            rand_num = 10
            competitor = yield self.random_competitor(arenas, uid, rand_num)
            ret = dict(competitor=competitor, timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)
        else:
            raise web.HTTPError(400, "Argument error")


@handler
class RecordHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena record', '/arena/record/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena record")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        arenas = yield self.get_arena(uid)
        yield self.redis.set('arenamark:%s' % uid, E.false)
        res = yield self.sql.runQuery(
            "SELECT competitor_id, result, ascend, timestamp FROM core_arenaresult WHERE user_id=%s ORDER BY timestamp",
            (uid,))
        records = []
        for r in res:
            arenas = yield self.get_arena(r[0])
            cuser = yield self.get_user(r[0])
            beautys = yield self.get_beautys(cuser)
            result = r[1]
            if arenas:
                guards = {}
                for key in arenas['guards'].keys():
                    guards[key] = arenas['heros'].get(key, {})
                records.append(
                    dict(uid=r[0], result=result, ascend=r[2], timestamp=r[3], xp=arenas['xp'], avat=arenas['avat'], \
                         nickname=arenas['nickname'], heros=guards, formation=arenas['formation'], beautys=beautys))
        res = yield self.sql.runQuery(
            "SELECT user_id, result, ascend, timestamp FROM core_arenaresult WHERE competitor_id=%s ORDER BY timestamp",
            (uid,))
        for r in res:
            arenas = yield self.get_arena(r[0])
            cuser = yield self.get_user(r[0])
            beautys = yield self.get_beautys(cuser)
            result = r[1]
            if arenas:
                guards = {}
                for key in arenas['guards'].keys():
                    guards[key] = arenas['heros'].get(key, {})
                if result == E.true:
                    result = E.false
                    ascend = -r[2]
                else:
                    result = E.true
                    ascend = r[2]
                records.append(
                    dict(uid=r[0], result=result, ascend=ascend, timestamp=r[3], xp=arenas['xp'], avat=arenas['avat'], \
                         nickname=arenas['nickname'], heros=guards, formation=arenas['formation'], beautys=beautys))
        ret = dict(records=records, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class ShopHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena shop', '/arena/shop/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
    ], filters=[ps_filter], description="Arena shop")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        arenas = yield self.get_arena(uid)
        if arenas:
            arena_coin = arenas['arena_coin']
        else:
            arena_coin = 0
        arenaprod_list = yield self.redis.lrange('arenaprod:%s' % uid, 0, -1)
        arenarefresh = yield self.redis.get('arenarefreshes:%s' % uid)
        if not arenarefresh:
            arenarefresh = 0
        prods = copy.deepcopy(D.ARENAPROD)

        for pid in prods.keys():
            if int(pid) in arenaprod_list:
                prods[pid]['is_buy'] = 1
            else:
                prods[pid]['is_buy'] = 0
        ret = dict(prods=prods, timestamp=int(time.time()), refresh_times=arenarefresh, arena_coin=arena_coin)
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class BuyHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena buy', '/arena/buy/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '4GRwMApTJ3VpZCcKcDEKUyc4NycKcDIKcyK', '_sign'),
        Param('pid', True, str, '01030', '01030', 'pid'),
    ], filters=[ps_filter], description="Arena buy")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        if self.has_arg('pid') and self.arg('pid') in D.ARENAPROD:
            pid = self.arg('pid')
            num = D.ARENAPROD[pid]['num']
            arenaprod_list = yield self.redis.lrange('arenaprod:%s' % uid, 0, -1)
            if int(pid) in arenaprod_list:
                self.write(dict(err=E.ERR_DUPLICATE_BUY, msg=E.errmsg(E.ERR_DUPLICATE_BUY)))
                return
            cost = E.cost4arena(pid)
            if cost:
                arenas = yield self.get_arena(uid)
                arenarefresh = yield self.redis.get('arenarefreshes:%s' % uid)
                if not arenarefresh:
                    arenarefresh = 0
                if arenas:
                    if arenas['arena_coin'] < cost:
                        self.write(dict(err=E.ERR_NOTENOUGH_ARENACOIN, msg=E.errmsg(E.ERR_NOTENOUGH_ARENACOIN)))
                        return
                    else:
                        arena_coin = arenas['arena_coin'] - cost
                        query = "UPDATE core_arena SET arena_coin=%s WHERE user_id=%s RETURNING id"
                        params = (arena_coin, uid)
                        for i in range(5):
                            try:
                                yield self.sql.runQuery(query, params)
                                break
                            except storage.IntegrityError:
                                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                                continue

                        if pid in user['prods']:
                            user['prods'][pid] += int(num)
                        else:
                            user['prods'][pid] = int(num)
                        if user['prods'][pid] > 9999:
                            user['prods'][pid] = 9999
                        cuser = dict(prods=user['prods'])
                        yield self.redis.lpush('arenaprod:%s' % uid, pid)
                        yield self.set_arena(uid)
                        logmsg = u'竞技场:竞技物品购买%s' % pid
                        yield self.set_user(uid, logmsg, **cuser)
                        cuser = dict(cuser=cuser, arena_coin=arena_coin)
                        ret = dict(user=cuser, timestamp=int(time.time()))
                        reb = zlib.compress(escape.json_encode(ret))
                        self.write(ret)
                else:
                    raise web.HTTPError(400, "Argument error")
        else:
            raise web.HTTPError(400, "Argument error")


@handler
class RefreshHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Arena refresh', '/arena/refresh/', [
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '4GRwMApTJ3VpZCcKcDEKUyc4NycKcDIKcyK', '_sign'),
    ], filters=[ps_filter], description="Arena refresh")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        arenas = yield self.get_arena(uid)
        arenarefresh = yield self.redis.get('arenarefreshes:%s' % uid)
        if not arenarefresh:
            arenarefresh = 0
            yield self.redis.set('arenarefreshes:%s' % uid, arenarefresh)
        if arenas:
            # arenas = pickle.loads(arenas)
            if arenarefresh >= 0:
                if arenarefresh < E.limit_by_refresh:
                    if arenas['arena_coin'] < D.ARENAREFRESH[arenarefresh * 2 + 1]:
                        self.write(dict(err=E.ERR_NOTENOUGH_ARENACOIN, msg=E.errmsg(E.ERR_NOTENOUGH_ARENACOIN)))
                        return
                    arena_coin = arenas['arena_coin'] - D.ARENAREFRESH[arenarefresh * 2 + 1]
                    query = "UPDATE core_arena SET arena_coin=%s WHERE user_id=%s RETURNING id"
                    params = (arena_coin, uid)
                    for i in range(5):
                        try:
                            yield self.sql.runQuery(query, params)
                            break
                        except storage.IntegrityError:
                            log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                            continue
                    yield self.set_arena(uid)
                    yield self.redis.incr('arenarefreshes:%s' % uid)
                    yield self.redis.delete('arenaprod:%s' % uid)
                    cuser = dict(arena_coin=arena_coin)
                    ret = dict(user=cuser, prods=D.ARENAPROD, timestamp=int(time.time()))
                    reb = zlib.compress(escape.json_encode(ret))
                    self.write(ret)
                else:
                    self.write(dict(err=E.ERR_DISSATISFY_MAXREFRESHES, msg=E.errmsg(E.ERR_DISSATISFY_MAXREFRESHES)))
                    return
        else:
            raise web.HTTPError(500)


            # @handler
            # class CreateUserHandler(ApiHandler):
            #     @storage.databaseSafe
            #     @defer.inlineCallbacks
            #     @utils.signed
            #     @api('Arena create', '/arena/create/', [
            #         Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '4GRwMApTJ3VpZCcKcDEKUyc4NycKcDIKcyK', '_sign'),
            #         ], filters=[ps_filter], description="Arena create")
            #     def get(self):
            #         uid = self.uid
            #         user = yield self.get_user(uid)
            #         nuser = E.initdata4user()
            #         res = yield self.sql.runQuery("SELECT id FROM core_user")
            #         for r in res:
            #             uid = r[0]
            #             query = "INSERT INTO core_arena(user_id, arena_coin, before_rank, now_rank, last_rank, jguards, formation, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
            #             params = (uid, 0, uid, uid, uid, nuser['jheros'], E.default_formation, int(time.time()))
            #             print query % params
            #             for i in range(5):
            #                 try:
            #                     yield self.sql.runQuery(query, params)
            #                     break
            #                 except storage.IntegrityError:
            #                     log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
            #                     continue
