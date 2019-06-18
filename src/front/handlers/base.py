# -*- coding: utf-8 -*-

import base64
import pickle
import time
import uuid
import json
import datetime
import random
import decimal
from itertools import *
from cyclone import web, escape
from django.db.models.query import QuerySet
from django.core.serializers.json import DjangoJSONEncoder
from twisted.internet import defer
from twisted.python import log
from front import storage, D
from front.utils import E
# from M2Crypto import RSA, BIO, EVP
from local_settings import ZONE_ID


class BaseHandler(web.RequestHandler, storage.DatabaseMixin):
    def get_current_user(self):
        return None

    # def generate_token(self, **kwargs):
    #     token = base64.urlsafe_b64encode(pickle.dumps(kwargs))
    #     return token

    @storage.databaseSafe
    @defer.inlineCallbacks
    def generate_sign(self, idcard, zone):
        firstsign = False
        ahex, aid = idcard.split('h', 1)
        res = yield self.sql.runQuery("SELECT state, user_id FROM core_account WHERE accountid=%s AND hex=%s LIMIT 1",
                                      (aid, ahex))
        if not res:
            raise E.USERNOTFOUND
        state, uid = res[0]
        if not uid and state == 0:
            firstsign = True
            uid = yield self.create_user()
            query = "UPDATE core_account SET user_id=%s, accountid=%s WHERE id=%s"
            params = (uid, str(aid), aid)
            for i in range(5):
                try:
                    yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        if state != 0:
            if state == 2:
                raise E.USERABNORMAL
            elif state == 3:
                raise E.USERBEBANKED
            else:
                raise E.UNKNOWNERROR
        token = dict(uid=str(uid))
        s = base64.urlsafe_b64encode(pickle.dumps(token)).rstrip('=')
        sign = s[-1] + s[1:-1] + s[0]

        yield self.predis.lpush('all:log:sign', pickle.dumps([int(time.time()), ZONE_ID, uid, aid, firstsign, state]))
        defer.returnValue(sign)

    def ping_server(self):
        return dict(domain=self.settings['domain'], notice='')

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_user_token(self, user_id, access_token, refresh_token=None):
        if refresh_token:
            query = "UPDATE core_user SET access_token=%s, refresh_token=%s, modified=%s WHERE id=%s"
            params = (access_token, refresh_token, int(time.time()), user_id)
        else:
            query = "UPDATE core_user SET access_token=%s, modified=%s WHERE id=%s"
            params = (access_token, int(time.time()), user_id)
        for i in range(5):
            try:
                yield self.sql.runOperation(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue

        defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def generate_player(self, model, serial, channel, user_id):
        user = dict()
        nickname = D.USERINIT["nickname"]
        avatar = D.USERINIT["avatar"]
        gold = D.USERINIT["gold"]
        rock = D.USERINIT["rock"]
        star = D.USERINIT["star"]
        point = D.USERINIT["point"]
        phone = D.USERINIT["phone"]
        ####
        props = escape.json_encode(D.USERINIT["props"])
        gates = escape.json_encode(D.USERINIT["gates"])
        mails = escape.json_encode(D.USERINIT["mails"])
        ips = escape.json_encode(D.USERINIT["ips"])

        query = """INSERT INTO core_player(user_id, channel_id, model, serial, phone, nickname, avatar, gold,\
                rock, star, point, props, gates, mails, ips, created, modified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,\
                 %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"""
        params = (
            user_id, channel, model, serial, phone, nickname, avatar, gold, rock, star, point, props, gates, \
            mails, ips, datetime.datetime.now(), datetime.datetime.now())
        # print query, params
        for i in range(5):
            try:
                res = yield self.sql.runQuery(query, params)
                if res:
                    user = dict(nickname=nickname,
                                avatar=avatar,
                                gold=gold,
                                rock=rock,
                                star=star,
                                phone=phone,
                                props=D.USERINIT["props"],
                                gates=D.USERINIT["gates"],
                                mails=D.USERINIT["mails"],
                                ips=D.USERINIT["ips"])
                    now_hp, tick = yield self.get_hp(user)
                    user.update(dict(hp=now_hp, tick=tick))

                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue

        defer.returnValue(user)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_player(self, use_id):
        #user = yield self.get_cache("user:%s" % use_id)
        user = dict()
        query = """SELECT b.user_id, b.nickname, b.avatar, b.gold, b.rock, b.star, b.phone, b.props, b.gates, b.mails,\
                b.ips FROM core_user AS a, core_player AS b WHERE a.id=%s AND a.id=b.user_id LIMIT 1"""
        params = (use_id,)
        res = yield self.sql.runQuery(query, params)
        if res:
            user_id, nickname, avatar, gold, rock, star, phone, props, gates, mails, ips = res[0]
            props = escape.json_decode(props)
            for key in list(props.keys()):
                if 'l' in key.split('_'):
                    if props[key] - int(time.time()) <= 0:
                        del props[key]
                    else:
                        props[key] = props[key] - int(time.time())
            user = dict(user_id=user_id,
                        nickname=nickname,
                        avatar=avatar,
                        gold=gold,
                        rock=rock,
                        star=star,
                        phone=phone,
                        props=props,
                        gates=escape.json_decode(gates),
                        mails=escape.json_decode(mails),
                        ips=escape.json_decode(ips))
            #yield self.set_cache("user:%s" % use_id, user)
        defer.returnValue(user)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_player(self, user_id, nickname=None, avat=None, gold=None, rock=None,
                   star=None, phone=None, props=None, gates=None, mails=None, ips=None):
        # olduser = yield self.get_user(uid)
        suser = {'user_id': user_id}
        subqueries = []
        if nickname is not None:
            suser['nickname'] = nickname
            subqueries.append("nickname=%(nickname)s")
        if avat is not None:
            suser['avat'] = avat
            subqueries.append("avat=%(avat)s")
        if gold is not None:
            suser['gold'] = gold
            subqueries.append("gold=%(gold)s")
        if rock is not None:
            suser['rock'] = rock
            subqueries.append("rock=%(rock)s")
        if star is not None:
            suser['star'] = star
            subqueries.append("star=%(star)s")
        if phone is not None:
            suser['phone'] = phone
            subqueries.append("phone=%(phone)s")
        if props is not None:
            suser['props'] = escape.json_encode(props)
            subqueries.append("props=%(props)s")
        if gates is not None:
            suser['gates'] = escape.json_encode(gates)
            subqueries.append("gates=%(gates)s")
        if mails is not None:
            suser['mails'] = escape.json_encode(mails)
            subqueries.append("mails=%(mails)s")
        if ips is not None:
            suser['ips'] = escape.json_encode(ips)
            subqueries.append("ips=%(ips)s")

        # suser['timestamp'] = str(int(time.time()))
        # subqueries.append("timestamp=%(timestamp)s")
        # SQL UPDATE START
        query = "UPDATE core_player SET " + ",".join(
            subqueries) + " WHERE user_id=%(user_id)s RETURNING nickname, avatar, gold, rock, star, phone, props," \
                          " gates, mails, ips"
        params = suser
        user = None
        for i in range(5):
            try:
                res = yield self.sql.runQuery(query, params)
                if not res:
                    user = None
                    yield self.del_cache("user:%s" % user_id)
                else:
                    r = res[0]
                    user = dict(user_id=user_id, nickname=r[0], avatar=r[1], gold=r[2], rock=r[3], star=r[4],
                                phone=r[5])
                    user['props'] = r[6] and escape.json_decode(r[6]) or {}
                    user['gates'] = r[7] and escape.json_decode(r[7]) or []
                    user['mails'] = r[8] and escape.json_decode(r[8]) or {}
                    user['ips'] = r[9] and escape.json_decode(r[9]) or {}
                    yield self.set_cache("user:%s" % user_id, user)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        # SQL UPDATE END
        defer.returnValue(user)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_cache(self, key, value, timeout=2592000):  # default 30 days
        yield self.redis.setex("cache:%s" % key, timeout, pickle.dumps(value))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def del_cache(self, key):
        yield self.redis.delete("cache:%s" % key)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_cache(self, key):
        value = yield self.redis.get("cache:%s" % key)
        if value:
            defer.returnValue(pickle.loads(value))
        else:
            defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_flush(self, key, value):
        yield self.redis.setex("flush:%s" % key, 36000, pickle.dumps(value))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_flush(self, key):
        value = yield self.redis.get("flush:%s" % key)
        if value:
            yield self.redis.delete("flush:%s" % key)
            defer.returnValue(pickle.loads(value))
        else:
            defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_nickname(self):
        for i in xrange(10, len(D.PREFIX) + 10, 10):
            randname = [(one[0] + one[1]).decode('utf-8') for one in product(D.PREFIX[:i], D.POSTFIX[:i])]
            res = yield self.sql.runQuery("SELECT nickname FROM core_user")
            usedname = [r[0] for r in res]
            nickname = list(set(randname) - set(usedname))
            random.shuffle(nickname)
            if len(nickname) >= 100:
                nickname = nickname[:100]
                break
            else:
                continue
        defer.returnValue(nickname)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_notice(self):
        notice_dict = {}
        notices = yield self.sql.runQuery("SELECT notice_id, position FROM core_noticeship")
        if notices:
            for n in notices:
                res = yield self.sql.runQuery(
                    "SELECT id, title, content, screenshot, sign, created_at, ended_at, url FROM core_notice WHERE id=%s",
                    (n[0],))
                nid, title, content, screenshot, sign, created_at, ended_at, url = res[0]
                if screenshot and FileObject(screenshot).exists():
                    url = FileObject(screenshot).url
                else:
                    url = url
                    # created_at, ended_at = res[0]
                created_at = int(time.mktime(created_at.timetuple()))
                ended_at = int(time.mktime(ended_at.timetuple()))
                now = int(time.mktime(datetime.datetime.now().timetuple()))
                if now >= created_at and now <= ended_at:
                    notice_dict[nid] = dict(title=title, content=content, url=url, sign=sign, create_at=created_at,
                                            ended_at=ended_at, position=n[1])

        defer.returnValue(notice_dict)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_hp(self, user):
        uid = user['user_id']
        hpmax = E.hpmax
        hpup = E.hpup
        hptick = E.hptick
        timenow = int(time.time()) - self.settings["timepoch"]
        hp = yield self.redis.hget("hp", uid)
        tick = 0
        if not hp:
            res = yield self.sql.runQuery("SELECT hp, timestamp FROM core_hp WHERE user_id=%s LIMIT 1", (uid,))
            if res:
                hp = res[0][1] * 100000 + res[0][0]
                yield self.redis.hset("hp", uid, hp)
            else:
                hpcur = hpmax
                yield self.sql.runQuery(
                    "INSERT INTO core_hp (user_id, hp, timestamp, created, modified) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (uid, hpcur, timenow, datetime.datetime.now(), datetime.datetime.now()))
                yield self.redis.hset("hp", uid, timenow * 100000 + hpcur)
        if hp:
            timestamp, hpsnap = divmod(hp, 100000)
            if hpsnap >= hpmax:
                hpcur = hpsnap
            else:
                timenow = int(time.time()) - self.settings["timepoch"]
                n, r = divmod((timenow - timestamp), hptick)
                hpuped = hpsnap + n * hpup
                if hpuped < hpmax:
                    hpcur = hpuped
                    if r != 0:
                        tick = hptick - r
                    else:
                        tick = hptick
                else:
                    hpcur = hpmax
        defer.returnValue((hpcur, tick))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def add_hp(self, user, value, reason=''):
        uid = user['user_id']
        hpmax = E.hpmax
        hpup = E.hpup
        hptick = E.hptick
        timenow = int(time.time()) - self.settings["timepoch"]
        hpcur, tick = yield self.get_hp(user)
        hpnow = hpcur + value
        if hpnow < hpmax:
            if hpnow < 0:
                hpnow = 0
            if 0 < tick:
                timetick = timenow - (hptick - tick)
            else:
                tick = hptick
                timetick = timenow
        else:
            timetick = timenow
        yield self.redis.hset("hp", uid, timetick * 100000 + hpnow)
        res = yield self.sql.runQuery("UPDATE core_hp SET hp=%s, timestamp=%s WHERE user_id=%s RETURNING id",
                                      (hpnow, timetick, uid))
        if not res:
            res = yield self.sql.runQuery(
                "INSERT INTO core_hp (user_id, hp, timestamp, created, modified) VALUES (%s, %s, %s, %s, %s)",
                (uid, hpnow, timetick, datetime.datetime.now(), datetime.datetime.now()))
        yield self.predis.lpush('all:log:hp',
                                pickle.dumps([int(time.time()), ZONE_ID, uid, value, hpcur, hpmax, reason]))
        defer.returnValue((hpnow, tick))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_sp(self, user):
        uid = user['uid']
        spmax = E.spmax(user['vrock'])
        spup = E.spup
        sptick = E.sptick
        timenow = int(time.time()) - self.settings["timepoch"]
        sp = yield self.predis.hget("sp:%s" % ZONE_ID, uid)
        tick = 0
        if not sp:
            spcur = spmax
            yield self.predis.hset("sp:%s" % ZONE_ID, uid, timenow * 10000 + spcur)
        else:
            timestamp, spsnap = divmod(sp, 10000)
            if spsnap >= spmax:
                spcur = spsnap
            else:
                timenow = int(time.time()) - self.settings["timepoch"]
                n, r = divmod((timenow - timestamp), sptick)
                spuped = spsnap + n * spup
                if spuped < spmax:
                    spcur = spuped
                    if r != 0:
                        tick = sptick - r
                    else:
                        tick = sptick
                else:
                    spcur = spmax
        defer.returnValue((spcur, tick))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def add_sp(self, user, value):
        uid = user['uid']
        spmax = E.spmax(user['vrock'])
        spup = E.spup
        sptick = E.sptick
        sp = yield self.predis.hget("sp:%s" % ZONE_ID, uid)
        tick = 0
        timenow = int(time.time()) - self.settings["timepoch"]
        if not sp:
            spcur, stick = yield self.get_sp(user)
        else:
            timestamp, spsnap = divmod(sp, 10000)
            if spsnap >= spmax:
                spcur = spsnap
            else:
                n, r = divmod((timenow - timestamp), sptick)
                spuped = spsnap + n * spup
                if spuped < spmax:
                    spcur = spuped
                    if r != 0:
                        tick = sptick - r
                    else:
                        tick = sptick
                else:
                    spcur = spmax
        spnow = spcur + value
        if spnow < spmax:
            if spnow < 0:
                spnow = 0
            if 0 < tick < sptick:
                timetick = timenow - (sptick - tick)
            else:
                tick = sptick
                timetick = timenow
        else:
            timetick = timenow
        yield self.predis.hset("sp:%s" % ZONE_ID, uid, timetick * 10000 + spnow)
        yield self.predis.lpush('all:log:sp', pickle.dumps([int(time.time()), ZONE_ID, uid, value, spcur, spmax]))
        defer.returnValue((spnow, tick))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_prop(self, uid, label):
        prop = yield self.get_cache("prop:%s:%s" % (uid, label))
        if not prop:
            res = yield self.sql.runQuery("SELECT num,txt FROM core_prop WHERE user_id=%s AND label=%s LIMIT 1",
                                          (uid, label))
            if not res:
                prop = dict(uid=uid, label=label, num=None, txt=None)
                # SQL UPDATE START
                query = "INSERT INTO core_prop(user_id, label, num, txt, timestamp) VALUES (%(uid)s, %(label)s, %(num)s, %(txt)s," \
                        + str(int(time.time())) + ") RETURNING id"
                params = prop
                for i in range(5):
                    try:
                        yield self.sql.runQuery(query, params)
                        break
                    except storage.IntegrityError:
                        log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                        continue
                        # SQL UPDATE END
            else:
                r = res[0]
                prop = dict(uid=uid, label=label, num=r[0], txt=r[1])
            yield self.set_cache(("prop:%s:%s" % (uid, label)), prop)
        defer.returnValue(prop)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_prop(self, uid, label, num=None, txt=None):
        prop = yield self.get_prop(uid, label)
        if prop:
            if num is not None:
                prop['num'] = num
            if txt is not None:
                prop['txt'] = txt
            yield self.set_cache(("prop:%s:%s" % (uid, label)), prop)
            # SQL UPDATE START
            query = "UPDATE core_prop SET num=%(num)s, txt=%(txt)s, timestamp=" \
                    + str(int(time.time())) + "WHERE user_id=%(uid)s AND label=%(label)s RETURNING id"
            params = prop
            for i in range(5):
                try:
                    res = yield self.sql.runQuery(query, params)
                    if not res:
                        query2 = "INSERT INTO core_prop(user_id, label, num, txt, timestamp) VALUES (%(uid)s, %(label)s, %(num)s, %(txt)s, " \
                                 + str(int(time.time())) + ") RETURNING id"
                        params2 = prop
                        for ii in range(5):
                            try:
                                yield self.sql.runQuery(query2, params2)
                                break
                            except storage.IntegrityError:
                                log.msg("SQL integrity error, retry(%i): %s" % (ii, (query2 % params2)))
                                continue
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
                    # SQL UPDATE END
        defer.returnValue(prop)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_mails(self, user, mids):
        mails = [key for key in user['mails'] if user['mails'][key] == 0]
        ballmails = yield self.redis.get("allmails:%s" % user['uid'])
        if not ballmails:
            allmails = {}
            if mails:
                res = yield self.sql.runQuery(
                    "SELECT id, sender, title, content, jawards, created_at, type FROM core_mail WHERE to_id=%s or"
                    " to_id ISNULL AND id in %s AND created_at::DATE=NOW()::DATE ORDER BY type ASC, created_at DESC",
                    (user['uid'], tuple(mails)))
                if res:
                    for r in res:
                        mid = str(r[0])
                        mail = dict(mid=mid, sender=r[1], title=r[2], content=r[3],
                                    timestamp=time.mktime(r[5].timetuple()), type=r[6])
                        mail['awards'] = r[4] and escape.json_decode(r[4]) or {}
                        allmails[mid] = mail
            yield self.redis.set("allmails:%s" % user['uid'], pickle.dumps(allmails))
        else:
            allmails = pickle.loads(ballmails)
        mails = []
        for mid in mids:
            if mid in allmails:
                mails.append(allmails[mid])
        defer.returnValue(mails)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_mails(self, user):
        uid = user['uid']
        res = yield self.sql.runQuery(
            "SELECT id, sender, title, content, jawards, created_at FROM core_mail WHERE to_id=%s or to_id ISNULL AND"
            " created_at::DATE=NOW()::DATE ORDER BY type ASC, created_at DESC",
            (uid,))
        for r in res:
            if str(r[0]) not in user['mails']:
                user['mails'][r[0]] = -1

        cuser = dict(mails=user['mails'])
        yield self.set_user(uid, **cuser)
        defer.returnValue(user['mails'])

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_mails(self, user):
        allmails = {}
        res = yield self.sql.runQuery(
            "SELECT id, sender, title, content, jawards, created_at, type FROM core_mail WHERE to_id=%s or to_id ISNULL"
            " AND created_at::DATE=NOW()::DATE ORDER BY type ASC, created_at DESC",
            (user['uid'],))
        if res:
            for r in res:
                mid = str(r[0])
                mail = dict(mid=mid, sender=r[1], title=r[2], content=r[3], timestamp=time.mktime(r[5].timetuple()),
                            type=r[6])
                if r[6] == 1:
                    if str(mid) not in user['mails']:
                        user['mails'][mid] = 0
                    mail['awards'] = r[4] and escape.json_decode(r[4]) or {}
                    allmails[mid] = mail
                else:
                    if str(r[0]) in user['mails']:
                        if user['mails'][str(r[0])] == 1:
                            pass
                        else:
                            mail['awards'] = r[4] and escape.json_decode(r[4]) or {}
                            allmails[mid] = mail
                    else:
                        mail['awards'] = r[4] and escape.json_decode(r[4]) or {}
                        allmails[mid] = mail
                        user['mails'][mid] = 0
        yield self.redis.set("allmails:%s" % user['uid'], pickle.dumps(allmails))
        defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def send_mails(self, sender, to_id, title, content, awards):
        query = "INSERT INTO core_mail(sender, to_id, title, content, jawards, comment, created_at, type) VALUES" \
                " (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (sender, to_id, title, content, escape.json_encode(awards), '', datetime.datetime.now(), 0)
        # print query, params
        for i in range(5):
            try:
                yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue

        defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def check_redpot(self, user):
        # lottmark
        lott = yield self.update_lott(user)
        free_by_gold = 0
        free_by_rock = 0
        if lott[E.lott_by_gold]['left_times'] > 0 and lott[E.lott_by_gold]['interval'] == 0:
            free_by_gold = 1
        if lott[E.lott_by_rock]['left_times'] > 0 and lott[E.lott_by_rock]['interval'] == 0:
            free_by_rock = 1
        lottmark = free_by_gold or free_by_rock
        # mailmark
        cuser = {}
        mailcomming = E.checkmails(user)
        if mailcomming:
            cuser['mails'] = user['mails']
            yield self.set_user(user['uid'], **cuser)
        mails = yield self.set_mails(user)
        mailmark = 0
        if E.false in mails.values():
            mailmark = 1
        # arenamark
        if user['xp'] / 100000 < D.ARENA_OPEN_LIMIT:
            arenamark = 0
        else:
            arenamark = yield self.redis.get('arenamark:%s' % user['uid'])
            if not arenamark:
                arenamark = 0
        defer.returnValue((lottmark, mailmark, arenamark))

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_firstlott(self, user, lotttype):
        res = yield self.sql.runQuery(
            "SELECT user_id FROM core_firstlott WHERE user_id=%s AND first=True AND lotttype=%s LIMIT 1",
            (user['uid'], lotttype))
        firstlott = False
        if not res:
            query = "INSERT INTO core_firstlott(user_id, first, created_at, lotttype) VALUES (%s, %s, %s, %s) RETURNING id"
            params = (user['uid'], True, int(time.time()), lotttype)
            for i in range(5):
                try:
                    yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue

            firstlott = True
        defer.returnValue(firstlott)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_daylott(self, user):
        res = yield self.sql.runQuery("select updated_at::date from core_daylott where updated_at::date=current_date")
        times = None
        if not res:
            query = "INSERT INTO core_daylott(user_id, times, updated_at) VALUES (%s, %s, %s) RETURNING times"
            params = (user['uid'], 1, datetime.datetime.now())
            for i in range(5):
                try:
                    times = yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        else:
            query = "UPDATE core_daylott SET times=times+1, updated_at=%s WHERE user_id=%s RETURNING times"
            params = (datetime.datetime.now(), user['uid'])
            for i in range(5):
                try:
                    times = yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        defer.returnValue(times)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_freelott(self, is_free, user, ltype):
        query = "UPDATE core_freelott SET times=times+1, free=%s, timestamp=%s WHERE user_id=%s AND lotttype=%s RETURNING times"
        params = (is_free, int(time.time()), user['uid'], ltype)
        for i in range(5):
            try:
                yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_lott(self, user):
        lott = D.LOTT
        for lot in lott.keys():
            res = yield self.sql.runQuery(
                "select free, timestamp from core_freelott where user_id=%s and lotttype=%s limit 1",
                (user['uid'], lot))
            if res:
                lottfree = yield self.redis.get('lottfree:%s:%s' % (user['uid'], lot))
                freetimes = yield self.redis.get('freetimes:%s:%s' % (user['uid'], lot))
                if not freetimes:
                    freetimes = 0
                if res[0][0] == True:
                    if lot == E.lott_by_gold and freetimes > E.limit_by_gold:
                        lott[lot]['left_times'] = 0
                    else:
                        if lot == E.lott_by_gold:
                            lott[lot]['left_times'] = E.limit_by_gold - freetimes
                        else:
                            lott[lot]['left_times'] = 1

                    lott[lot]['interval'] = 0
                    yield self.redis.set('lottfree:%s:%s' % (user['uid'], lot), True)

                else:
                    if lot == E.lott_by_gold and freetimes > E.limit_by_gold:
                        lott[lot]['interval'] = 0
                        lott[lot]['left_times'] = 0
                        continue
                    if lot == E.lott_by_gold:
                        interval_times = E.timer_by_gold
                        lott[lot]['left_times'] = E.limit_by_gold - freetimes
                    else:
                        interval_times = E.timer_by_rock
                        lott[lot]['left_times'] = 1
                    interval = int(time.time()) - int(res[0][1])
                    if interval <= 0:
                        interval = 0
                    if interval > interval_times:
                        lott[lot]['interval'] = 0
                        yield self.update_freelott(True, user, lot)
                        if lott[lot]['left_times'] > 0:
                            yield self.redis.set('lottfree:%s:%s' % (user['uid'], lot), True)
                    else:
                        lott[lot]['interval'] = interval_times - interval
        defer.returnValue(lott)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_buyrecord(self, user, gid):
        res = yield self.sql.runQuery("SELECT * FROM core_buyrecord WHERE user_id=%s LIMIT 1", (user['uid'],))
        if not res:
            mail = D.TRIOMAIL
            yield self.send_mails(mail['sender'], user['uid'], mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_buyrecord(user_id, gid, created_at) VALUES (%s, %s, %s) RETURNING id"
        params = (user['uid'], gid, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def first_payrecord(self, uid):
        putao = yield self.sql.runQuery("SELECT * FROM core_payrecord WHERE user_id=%s LIMIT 1", (uid,))
        ali = yield self.sql.runQuery("SELECT * FROM core_alipayrecord WHERE user_id=%s LIMIT 1", (uid,))
        xm = yield self.sql.runQuery("SELECT * FROM core_xmpayrecord WHERE user_id=%s LIMIT 1", (uid,))
        dangbei = yield self.sql.runQuery("SELECT * FROM core_dangbeipayrecord WHERE user_id=%s LIMIT 1", (uid,))
        letv = yield self.sql.runQuery("SELECT * FROM core_letvpayrecord WHERE user_id=%s LIMIT 1", (uid,))
        atet = yield self.sql.runQuery("SELECT * FROM core_atetpayrecord WHERE user_id=%s LIMIT 1", (uid,))
        cm = yield self.sql.runQuery("SELECT * FROM core_cmpayrecord WHERE user_id=%s AND hret='0' LIMIT 1", (uid,))
        lg = yield self.sql.runQuery("SELECT * FROM core_lgpayrecord WHERE user_id=%s LIMIT 1", (uid,))
        if putao or ali or xm or dangbei or letv or cm or lg or atet:
            status = 1
        else:
            status = 0
        defer.returnValue(status)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_payrecord(self, uid, pid, trans_no, result, trade_time, amount, currency):
        # res = yield self.sql.runQuery("SELECT * FROM core_payrecord WHERE user_id=%s LIMIT 1", (uid, ))
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_payrecord(user_id, pid, trans_no, result, trade_time, amount, currency, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, pid, str(trans_no), str(result), trade_time, amount, str(currency), int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_alipayrecord(self, uid, pid, app_order_id, coin_order_id, consume_amount, credit_amount, Ts, is_success,
                         error_code, sign):
        # res = yield self.sql.runQuery("SELECT * FROM core_alipayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])
        res = yield self.sql.runQuery("SELECT * FROM core_alipayrecord WHERE app_order_id=%s", (app_order_id,))
        if not res:
            query = "INSERT INTO core_alipayrecord(user_id, pid, app_order_id, coin_order_id, consume_amount, credit_amount, ts,\
                 is_success, error_code, sign, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
            params = (
                uid, pid, app_order_id, coin_order_id, consume_amount, credit_amount, Ts, is_success, error_code, sign, \
                int(time.time()))
            # print query % params
            for i in range(5):
                try:
                    rid = yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        else:
            rid = None
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_xmpayrecord(self, uid, pid, orderId, orderStatus, payFee, payTime):
        # res = yield self.sql.runQuery("SELECT * FROM core_xmpayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_xmpayrecord(user_id, pid, app_order_id, orderstatus, payfee, paytime, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, pid, orderId, orderStatus, payFee, payTime, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @defer.inlineCallbacks
    def set_dangbeipayrecord(self, uid, app_order_id, pid, extra, fee, out_trade_no, state):
        # res = yield self.sql.runQuery("SELECT * FROM core_dangbeipayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_dangbeipayrecord(user_id, pid, app_order_id, state ,fee, out_trade_no, extra,\
             paied_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, pid, app_order_id, state, fee, out_trade_no, extra, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @defer.inlineCallbacks
    def set_atetpayrecord(self, uid, app_order_id, pid, exOrderno, amount, counts, paypoint, paytype, cpprivateinfo,
                          result):
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_atetpayrecord(user_id, pid, app_order_id, result ,amount,counts,paypoint, exorderno, cpprivateinfo,paytype,\
             paied_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (
            uid, pid, app_order_id, result, amount, counts, paypoint, exOrderno, cpprivateinfo, paytype,
            int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                raise
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @defer.inlineCallbacks
    def set_letvpayrecord(self, uid, app_order_id, externalProductId, total, quantity):
        # res = yield self.sql.runQuery("SELECT * FROM core_letvpayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_letvpayrecord(user_id, pid, app_order_id, total, quantity,\
             created_at) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, externalProductId, app_order_id, total, quantity, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_cmpayrecord(self, uid, pid, amount, orderId, contentId, consumeCode, cpid, hRet):
        # res = yield self.sql.runQuery("SELECT * FROM core_cmpayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status and hRet == '0':
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_cmpayrecord(user_id, pid, amount, app_order_id, contentid, consumecode, cpid,\
             hret, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, pid, amount, orderId, contentId, consumeCode, cpid, hRet, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_lgpayrecord(self, uid, pid, app_order_id, transaction_id, fee):
        # res = yield self.sql.runQuery("SELECT * FROM core_lgpayrecord WHERE user_id=%s LIMIT 1", (uid, ))
        # if not res:
        status = yield self.first_payrecord(uid)
        if not status:
            mail = D.PAYMAIL
            yield self.send_mails(mail['sender'], uid, mail['title'], mail['content'], mail['jawards'])

        query = "INSERT INTO core_lgpayrecord(user_id, pid, app_order_id, transaction_id, fee,\
             created_at) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
        params = (uid, pid, app_order_id, transaction_id, fee, int(time.time()))
        for i in range(5):
            try:
                rid = yield self.sql.runQuery(query, params)
                break
            except storage.IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue
        defer.returnValue(rid)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_payrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery("select created_at from core_payrecord where user_id=%s and pid=%s limit 1",
                                          (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:

                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_alipayrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery(
                "select created_at from core_alipayrecord where user_id=%s and pid=%s limit 1",
                (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:

                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_xmpayrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery("select created_at from core_xmpayrecord where user_id=%s and pid=%s limit 1",
                                          (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:

                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_letvpayrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery(
                "select created_at from core_letvpayrecord where user_id=%s and pid=%s limit 1",
                (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:

                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_cmpayrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery("select created_at from core_cmpayrecord where user_id=%s and pid=%s limit 1",
                                          (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:

                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_lgpayrecord(self, user, rockstore):
        payrecords = {}
        for i in xrange(0, len(rockstore) / 8):
            res = yield self.sql.runQuery("select created_at from core_lgpayrecord where user_id=%s and pid=%s limit 1",
                                          (user['uid'], rockstore[i * 8]))
            lefttime = '-1'
            if res:
                if rockstore[i * 8 + 2] == 0:
                    if rockstore[i * 8 + 3] == 1:
                        res = yield self.sql.runQuery(
                            "SELECT created_at, ended_at FROM core_card WHERE user_id=%s AND gid=%s LIMIT 1",
                            (user['uid'], rockstore[i * 8]))
                        if res:
                            created_at, ended_at = res[0]
                            t = datetime.datetime.today().date()
                            lefttime = (ended_at - int(time.mktime(t.timetuple()))) / 3600 / 24
                            if lefttime < 0:
                                lefttime = '-1'
                        payrecords[rockstore[i * 8]] = lefttime

                    elif rockstore[i * 8 + 3] == 2:
                        payrecords[rockstore[i * 8]] = lefttime
                    else:
                        gid, = [rockstore[j * 8] for j in xrange(0, len(rockstore) / 8) if
                                rockstore[j * 8 + 1] == rockstore[i * 8 + 1] and rockstore[j * 8 + 3] == 2]
                        payrecords[gid] = lefttime
            else:
                if rockstore[i * 8 + 2] == 0:
                    payrecords[rockstore[i * 8]] = lefttime
        yield self.predis.set('payrecords:%s:%s' % (ZONE_ID, user['uid']), pickle.dumps(payrecords))
        defer.returnValue(payrecords)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_dayrecharge(self, user, channel):
        if channel == 'putaogame':
            query = "SELECT SUM(c.value) FROM core_payrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"

        elif channel == 'pt_xiaomi':
            query = "SELECT SUM(c.value) FROM core_xmpayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"

        elif channel == 'pt_letv':
            query = "SELECT SUM(c.value) FROM core_letvpayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"

        elif channel == 'pt_chinamobile':
            query = "SELECT SUM(c.value) FROM core_cmpayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"

        elif channel == 'pt_lovegame':
            query = "SELECT SUM(c.value) FROM core_lgpayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"

        elif channel == 'pt_ali':
            query = "SELECT SUM(c.value) FROM core_alipayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"
        elif channel == 'atet':
            query = "SELECT SUM(c.value) FROM core_atetpayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.paied_at)::date=%s"

        elif channel == 'dangbei':
            query = "SELECT SUM(c.value) FROM core_dangbeipayrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.paied_at)::date=%s"
        else:
            query = "SELECT SUM(c.value) FROM core_payrecord AS a, core_account AS b,\
                 core_product AS c WHERE a.user_id=b.user_id AND a.pid=c.pid AND a.user_id=%s AND to_timestamp(a.created_at)::date=%s"
        params = (user['uid'], str(datetime.datetime.today().date()))
        res = yield self.sql.runQuery(query, params)
        amount, = res[0]
        if not amount:
            amount = 0
        res = yield self.sql.runQuery("SELECT a.rid, a.total, a.created_at, a.ended_at FROM core_dayrecharge AS a, core_dayrecharge_channels AS b,\
             core_channel as c WHERE b.channel_id=c.id AND a.id=b.dayrecharge_id AND c.slug=%s", (channel,))
        if res:
            for r in res:
                rid, total, created_at, ended_at = r
                created_at = int(time.mktime(created_at.timetuple()))
                ended_at = int(time.mktime(ended_at.timetuple()))
                now = int(time.mktime(datetime.datetime.now().timetuple()))
                recharge = yield self.predis.get(
                    'recharge:%s:%s:%s:%s' % (ZONE_ID, user['uid'], rid, str(datetime.datetime.today().date())))
                if now >= created_at and now <= ended_at:
                    if int(total) <= int(amount):
                        if not recharge:
                            status = 0
                        elif recharge == -1:
                            status = 0
                        else:
                            status = recharge
                    else:
                        if recharge:
                            status = recharge
                        else:
                            status = -1
                    yield self.predis.set(
                        'recharge:%s:%s:%s:%s' % (ZONE_ID, user['uid'], rid, str(datetime.datetime.today().date())),
                        status)
        defer.returnValue(amount)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_expedition(self, user):
        uid = user['uid']
        sword = E.calc_topsword(user)
        # print uid, sword
        expedition = yield self.predis.hget("expeditions:%s" % ZONE_ID, uid)
        expedzuids = yield self.predis.hget("expedzuids:%s" % ZONE_ID, uid)
        reset = yield self.redis.get('expedresets:%s' % uid)
        if not reset:
            left_times = E.expedmaxtimes(user['vrock'])
        else:
            left_times = E.expedmaxtimes(user['vrock']) - reset
            if left_times < 0:
                left_times = 0
        if not expedzuids:
            zuid_list = []
        else:
            zuid_list = pickle.loads(expedzuids)
        if expedition:
            expedition = pickle.loads(expedition)
            expedition['reset'] = left_times
        else:
            all_expedition = yield self.predis.get('all:expedition:match')
            # print datetime.datetime.now()
            if all_expedition:
                all_expedition = pickle.loads(all_expedition)
                comps = {}
                for ex in all_expedition:
                    if ex['mode'] == 1:
                        min, max = ex['rule'].split(':')
                        if sword >= int(min) and sword <= int(max):
                            for key, value in ex['opponents'].items():
                                # print key, value
                                log.msg("expedition uid %s sword %s, key is %s, value lens is %s" % (
                                    uid, sword, key, len(value)))
                                if value:
                                    for i in xrange(1, 11):
                                        cuid = random.choice(value)
                                        if isinstance(cuid, basestring):
                                            if (cuid in zuid_list) and (i != 10):
                                                continue
                                            else:
                                                cuser = yield self.predis.hget('all:users', cuid)
                                                cuser = pickle.loads(cuser)
                                                out = {
                                                    'zuid': cuser['id'],
                                                    'nickname': cuser['nickname'],
                                                    'avat': cuser['avat'],
                                                    'xp': cuser['xp'],
                                                    'heros': {hid: cuser['heros'][hid] for hid in cuser['tophids']},
                                                    'beautys': cuser['beautys'],
                                                    'positions': cuser['toppositions'],
                                                    'formation': cuser['topformation']
                                                }
                                                cuser = out
                                                zuid_list.append(cuid)
                                                break
                                        else:
                                            if (cuid['zuid'] in zuid_list) and (i != 10):
                                                continue
                                            else:
                                                cuser = cuid
                                                zuid_list.append(cuser['zuid'])
                                                break
                                    for hero in cuser['heros'].keys():
                                        cuser['heros'][hero]['blood'] = 0
                                        cuser['heros'][hero]['gas'] = 0
                                    comps[key] = cuser
                # print 'all_expedition', all_expedition
                # print 'comps', comps
                heros = user['heros']
                our = {hid: dict(blood=0, gas=0) for hid in heros}
                our['exped_coin'] = yield self.get_expedcoin(user)
                expedition = dict(our=our, comps=comps, reset=left_times)
                print datetime.datetime.now()
                yield self.predis.hset("expeditions:%s" % ZONE_ID, uid, pickle.dumps(expedition))
                yield self.predis.hset("expedzuids:%s" % ZONE_ID, uid, pickle.dumps(zuid_list))
            else:
                expedition = {}

        defer.returnValue(expedition)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_expedition(self, user, label, our, comps):
        expedition = yield self.get_expedition(user)
        expedition['our'] = our
        cuser = expedition['comps'][label]
        for key, value in comps.items():
            if key in cuser['heros']:
                cuser['heros'][key]['blood'] = value['blood']
                cuser['heros'][key]['gas'] = value['gas']
        yield self.predis.hset("expeditions:%s" % ZONE_ID, user['uid'], pickle.dumps(expedition))
        defer.returnValue(expedition)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def update_expedcoin(self, user, exped_coin):
        res = yield self.sql.runQuery("SELECT * FROM core_userexped WHERE user_id=%s", (user['uid'],))
        if res:
            query = "UPDATE core_userexped SET exped_coin=exped_coin+%s WHERE user_id=%s RETURNING exped_coin"
            params = (exped_coin, user['uid'])
        else:
            query = "INSERT INTO core_userexped (user_id, exped_coin) VALUES (%s, %s) RETURNING exped_coin"
            params = (user['uid'], exped_coin)
        res = yield self.sql.runQuery(query, params)
        exped_coin, = res[0]
        defer.returnValue(exped_coin)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_expedcoin(self, user):
        res = yield self.sql.runQuery("SELECT exped_coin FROM core_userexped WHERE user_id=%s LIMIT 1", (user['uid'],))
        if res:
            exped_coin, = res[0]
        else:
            query = "INSERT INTO core_userexped (user_id, exped_coin) VALUES (%s, %s) RETURNING exped_coin"
            params = (user['uid'], 0)
            res = yield self.sql.runQuery(query, params)
            exped_coin, = res[0]
        defer.returnValue(exped_coin)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def open_market(self, user, times):
        market = 0
        uid = user['uid']
        marketopen = yield self.redis.get('marketopen:%s' % uid)
        if E.vip(user['vrock']) < 9 and not marketopen:
            marketseed = yield self.predis.get('marketseed')
            if not marketseed:
                marketseed = random.randint(40, 51)
                yield self.predis.set('marketseed', marketseed)
            batttimes = (yield self.predis.hget("batttimes:%s" % ZONE_ID, uid)) or 0
            if not batttimes:
                batttimes = 0
            if ((batttimes + 1 * times) == marketseed) or (
                            batttimes < marketseed and (batttimes + 1 * times) > marketseed):
                market = 1
                yield self.redis.setex('market:%s' % uid, 3600, int(time.time()) + 3600)
                yield self.redis.delete('marketprod:%s' % uid)
                yield self.redis.set('marketopen:%s' % uid, market)
                # print 'batttimes', batttimes+1*times, marketseed
        yield self.predis.hincrby("batttimes:%s" % ZONE_ID, uid, 1 * times)
        defer.returnValue(market)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def open_bmmarket(self, user, times):
        blackmarket = 0
        uid = user['uid']
        bmopen = yield self.redis.get('bmopen:%s' % uid)
        if E.vip(user['vrock']) < 11 and not bmopen:
            bmseed = yield self.predis.get('bmseed')
            if not bmseed:
                bmseed = random.randint(12, 16)
                yield self.predis.set('bmseed', bmseed)
            hardbatttimes = (yield self.predis.hget("hardbatttimes:%s" % ZONE_ID, uid)) or 0
            if not hardbatttimes:
                hardbatttimes = 0
            if (hardbatttimes + 1 * times) == bmseed or (
                            hardbatttimes < bmseed and (hardbatttimes + 1 * times) > bmseed):
                blackmarket = 1
                yield self.redis.setex("blackmarket:%s" % uid, 3600, int(time.time()) + 3600)
                yield self.redis.delete('bmprod:%s' % uid)
                yield self.redis.set('bmopen:%s' % uid, blackmarket)
                # print 'hardbatttimes', hardbatttimes+1*times, bmseed
        yield self.predis.hincrby("hardbatttimes:%s" % ZONE_ID, uid, 1 * times)
        defer.returnValue(blackmarket)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get_consume(self, user):
        consume = {}
        event = yield self.sql.runQuery("SELECT DISTINCT a.bid, a.created_at, a.ended_at FROM core_bigevent AS a,\
              core_consume AS b WHERE a.id=b.bigevent_id ORDER BY a.bid")
        for e in event:
            bid, created_at, ended_at = e
            created_at = int(time.mktime(created_at.timetuple()))
            ended_at = int(time.mktime(ended_at.timetuple()))
            now = int(time.mktime(datetime.datetime.now().timetuple()))
            if now >= created_at and now <= ended_at:
                res = yield self.sql.runQuery(
                    "SELECT SUM(rock) FROM core_userconsume WHERE user_id=%s AND bid=%s LIMIT 1", \
                    (user['uid'], bid))
                if res:
                    rock, = res[0]
                    if not rock:
                        rock = 0
                else:
                    rock = 0
                consume[bid] = int(rock)

        defer.returnValue(consume)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_consume(self, user, rock):
        event = yield self.sql.runQuery("SELECT DISTINCT a.bid, a.created_at, a.ended_at FROM core_bigevent AS a,\
                core_consume AS b WHERE a.id=b.bigevent_id ORDER BY a.bid")
        for e in event:
            bid, created_at, ended_at = e
            created_at = int(time.mktime(created_at.timetuple()))
            ended_at = int(time.mktime(ended_at.timetuple()))
            now = int(time.mktime(datetime.datetime.now().timetuple()))
            if now >= created_at and now <= ended_at:
                query = "INSERT INTO core_userconsume (user_id, bid, rock) VALUES (%s, %s, %s) RETURNING id"
                params = (user['uid'], bid, rock)
                for i in range(5):
                    try:
                        yield self.sql.runQuery(query, params)
                        break
                    except storage.IntegrityError:
                        log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                        continue
        defer.returnValue(None)

    @storage.databaseSafe
    @defer.inlineCallbacks
    def set_consumeresult(self, user, bid, rid):
        res = yield self.sql.runQuery("SELECT * FROM core_userconsumerecord WHERE user_id=%s AND bid=%s AND rid=%s", \
                                      (user['uid'], bid, rid))
        if not res:
            query = "INSERT INTO core_userconsumerecord (user_id, bid, rid) VALUES (%s, %s, %s) RETURNING id"
            params = (user['uid'], bid, rid)
            for i in range(5):
                try:
                    rock = yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        defer.returnValue(None)


class ApiHandler(BaseHandler):
    def _(self, message, plural_message=None, count=None):
        return self.locale.translate(message, plural_message, count)

    def auth_login(self, user):
        self.user_key = self.create_signed_value("user_id", str(user.id))
        self.set_cookie("user_id", self.user_key, expires_days=1)
        self._current_user = user

    def auth_logout(self):
        self.clear_cookie("user_id")
        self._current_user = None

    def has_arg(self, name):
        return self.request.arguments.has_key(name)

    def arg(self, name, default=web.RequestHandler._ARG_DEFAULT, strip=True):
        return self.get_argument(name, default, strip)

    def arg_bool(self, name):
        return self.arg(name, 'false') == 'true'

    def args(self, name, default=[], separator=','):
        value = self.get_argument(name, None)
        if value:
            return value.split(',')
        else:
            return ''

    def keyword_filte(self, content):
        return checkword.mark_filte(content)

    def out_content(self, content):
        return checkword.output(content)

    def static_media_url(self, url):
        return self.settings.get('static_url', '') + (url[0] == '/' and url[1:] or url)

    def file_url(self, f, tag='phone'):
        if f is None:
            return ''
        try:
            if hasattr(f, 'extra_thumbnails'):
                dpi = 1
                if dpi > 1 and f.extra_thumbnails.has_key('%dx%s' % (dpi, tag)):
                    f = f.extra_thumbnails['%dx%s' % (dpi, tag)]
                elif f.extra_thumbnails.has_key(tag):
                    f = f.extra_thumbnails[tag]

            if hasattr(f, 'url'):
                url = f.url
            else:
                url = unicode(f)
            return self.static_media_url(url)
        except Exception, e:
            print e
            return

    def get_cookie(self, name, default=None):
        if name == 'user_id' and self.has_arg('session_key'):
            return self.arg('session_key')
        return super(ApiHandler, self).get_cookie(name, default)

    @property
    def user(self):
        return self.current_user

    def send_error(self, status_code=403, **kwargs):
        if self.settings.get("debug", False):
            print kwargs
        if self._headers_written:
            logging.error("Cannot send error response after headers written")
            if not self._finished:
                self.finish()
            return
        self.clear()
        self.set_status(status_code)
        if status_code < 500:
            if kwargs.has_key('exception') and not kwargs.has_key('msg'):
                kwargs['msg'] = str(kwargs['exception'])
                del kwargs['exception']

            self.write(kwargs)
        self.finish()

    def write(self, chunk):
        assert not self._finished

        if type(chunk) in (QuerySet,):
            chunk = self.ps(chunk)

        if type(chunk) in (dict, list):
            chunk = json.dumps(chunk, cls=ApiJSONEncoder, ensure_ascii=False, indent=4)
            if self.arg('cb', False):
                chunk = '%s(%s)' % (self.arg('cb'), chunk)
            self.set_header("Content-Type", "text/javascript; charset=UTF-8")
            # self.set_header("Content-Encoding", "gzip")
            # self.set_header("Content-Type", "application/json; charset=UTF-8")
            chunk = web.utf8(chunk)
            self._write_buffer.append(chunk)
        else:
            super(ApiHandler, self).write(chunk)

    def ps(self, qs, convert_func=None, **kwargs):
        start = int(self.get_argument('start', 0))
        count = int(self.get_argument('count', 25))
        if (type(qs) in (list, set)):
            total_count = len(qs)
        else:
            total_count = qs.count()
            if type(qs) not in (QuerySet,):
                qs = qs.all()

        if total_count > start:
            if start == -1:
                import math
                start = (math.ceil(float(total_count) / count) - 1) * count
            items = convert_func is None and qs[start:start + count] or [convert_func(item, **kwargs) for item in
                                                                         qs[start:start + count]]
        else:
            items = []
        return {'total_count': total_count, 'items': items}

    def format_params(self, params, urlencode):
        slist = sorted(params)
        buff = []
        for k in slist:
            v = quote(params[k]) if urlencode else params[k]
            buff.append("{0}={1}".format(k, v))

        return "&".join(buff)

    def verify_sign(self, params, sign):
        md = EVP.MessageDigest('sha1')
        md.update(params.encode('utf-8'))
        digest = md.final()
        bio = BIO.MemoryBuffer(D.PUB_KEY)
        key = RSA.load_pub_key_bio(bio)
        try:
            result = key.verify(digest, base64.b64decode(sign))
        except Exception:
            result = None
        return result

    def create_sign(self, params):
        md = EVP.MessageDigest('sha1')
        md.update(params.encode('utf-8'))
        digest = md.final()
        bio = BIO.MemoryBuffer(D.PRI_KEY)
        key = RSA.load_key_bio(bio)
        try:
            result = base64.b64encode(key.sign(digest))
        except Exception:
            result = None
        return result


class ApiJSONEncoder(DjangoJSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return str(o)
        # return dt2ut(o)
        elif isinstance(o, decimal.Decimal):
            return str(o)
        else:
            try:
                return super(ApiJSONEncoder, self).default(o)
            except Exception:
                return smart_unicode(o)
