# -*- coding: utf-8 -*-

import time
import zlib
import random
import pickle
import datetime
from twisted.internet import defer
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front import D
from twisted.python import log
# from front.handlers.base import BaseHandler
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder
from local_settings import ZONE_ID


class HomeHandler(ApiHandler):
    def get(self):
        self.render('index.html')


class CrossdomainHandler(ApiHandler):
    def get(self):
        self.render('crossdomain.xml')

    def post(self):
        self.render('crossdomain.xml')


@handler
class StartupHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Startup', '/startup/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('model', True, str, 'bigfish@hi3798mv100', 'bigfish@hi3798mv100', 'model'),
        Param('serial', True, str, '0066cf0456732122121', '0066cf0456732122121', 'serial'),
        Param('idcard', False, str, None, None, 'idcard'),
        Param('user_id', False, str, '1', '1', 'user_id'),
        Param('access_token', False, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
    ], filters=[ps_filter], description="Startup")
    def get(self):
        try:
            model = self.get_argument("model")
            serial = self.get_argument("serial")
            idcard = self.get_argument("idcard", None)
            channel = self.get_argument("channel", "test1")
            user_id = self.get_argument("user_id")
            access_token = self.get_argument("access_token")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", (channel,))
        if not channels:
            channels = yield self.sql.runQuery("SELECT id FROM core_channel WHERE slug=%s LIMIT 1", ("test1",))
        channel, = channels[0]
        try:
            # server = {domain:..., notice: ...}
            server = self.ping_server()
        except E.SERVERERROR, e:
            self.write(dict(err=E.ERR_SERVER, msg=e.message))
            return
        except Exception:
            self.write(dict(err=E.ERR_UNKNOWN, msg=E.errmsg(E.ERR_UNKNOWN)))
            return

        idcard = yield self.refresh_idcard(idcard, model, serial, channel, access_token, user_id)
        if not idcard:
            self.write(dict(err=E.ERR_USER_CREATED, msg=E.errmsg(E.ERR_USER_CREATED)))
            return
        else:
            record = yield self.predis.get('zone:%s:%s' % (ZONE_ID, user_id))
            if not record:
                yield self.predis.set('zone:%s:%s' % (ZONE_ID, user_id), idcard)
                #yield self.bind_token(idcard, self.arg('access_token'))
        # if self.has_arg('access_token'):
        #     record = yield self.predis.get('zone:%s:%s' % (ZONE_ID, self.arg('access_token')))
        #     if not record:
        #         yield self.predis.set('zone:%s:%s' % (ZONE_ID, self.arg('access_token')), idcard)
        #         yield self.bind_token(idcard, self.arg('access_token'))

        ret = dict(idcard=idcard, zone=ZONE_ID, server=server)
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)


@handler
class ActiveHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Active', '/active/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', False, str, '1', '1', 'user_id'),
        Param('access_token', False, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
        Param('zone', True, str, '0', '0', 'zone'),
    ], filters=[ps_filter], description="Active")
    def get(self):
        try:
            channel = self.get_argument("channel", "test1")
            idcard = self.get_argument("idcard")
            zone = self.get_argument("zone")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        if idcard:
            ahex, aid = idcard.split('h', 1)
            query = """SELECT nickname, avat, "playerLevel", "playerXp", "goldcoin", gem, "honorPoint",\
                  "arena5v5Rank", "arena5v5Place",  "arenaOtherRank", "arenaOtherPlace", "heroList", "soldierList",\
                   formations, items, "headIconList", "titleList", achievement, "playerConfig", "buddyList",\
                    "playerStatusInfo", jmails, "annalNormal", "annelCurrentGateNormal", "annalHero",\
                     "annelCurrentGateHero", "annalEpic", "dungeonAnnelHero", "dungeonAnnelEpic",\
                      "dungeonAnnelGatesNormal", "dungeonAnnelGatesHero", "dungeonAnnelGatesEpic"\
                       FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
            params = (ahex, aid)
            res = yield self.sql.runQuery(query, params)
            if res:
                nickname, avat, playerLevel, playerXp, goldcoin, gem, honorPoint, arena5v5Rank, arena5v5Place,\
                arenaOtherRank, arenaOtherPlace, heroList, soldierList, formations, items, headIconList, titleList,\
                achievement, playerConfig, buddyList, playerStatusInfo, jmails, annalNormal, annelCurrentGateNormal,\
                annalHero, annelCurrentGateHero, annalEpic, dungeonAnnelHero, dungeonAnnelEpic, dungeonAnnelGatesNormal, \
                dungeonAnnelGatesHero, dungeonAnnelGatesEpic = res[0]
            else:
                self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
                return
                # yield self.predis.hset('zone:%s:%s' % (zone, datetime.datetime.now().strftime('%Y%m%d')), aid, E.true)
            hero_list = []
            heroList = escape.json_decode(heroList)
            for index, one in enumerate(heroList):
                if int(one['unlock']) != 0:
                    hero_list.append(one)
            for index, one in enumerate(hero_list):
                one.pop("unlock")
            users = dict(avat=avat,
                         playerLevel=playerLevel,
                         playerXp=playerXp,
                         goldcoin=goldcoin,
                         gem=gem,
                         honorPoint=honorPoint,
                         arena5v5Rank=arena5v5Rank,
                         arena5v5Place=arena5v5Place,
                         arenaOtherRank=arenaOtherRank,
                         arenaOtherPlace=arenaOtherPlace,
                         unlockSlotNum=0,
                         heroList=hero_list,
                         soldierList=escape.json_decode(soldierList),
                         formations=escape.json_decode(formations),
                         items=escape.json_decode(items),
                         headIconList=escape.json_decode(headIconList),
                         titleList=escape.json_decode(titleList),
                         achievement=escape.json_decode(achievement),
                         playerConfig=escape.json_decode(playerConfig),
                         buddyList=escape.json_decode(buddyList),
                         playerStatusInfo=escape.json_decode(playerStatusInfo),
                         jmails=escape.json_decode(jmails),
                         annalNormal=escape.json_decode(annalNormal),
                         annelCurrentGateNormal=escape.json_decode(annelCurrentGateNormal),
                         annalHero=escape.json_decode(annalHero),
                         annelCurrentGateHero=escape.json_decode(annelCurrentGateHero),
                         annalEpic=escape.json_decode(annalEpic),
                         dungeonAnnelHero=escape.json_decode(dungeonAnnelHero),
                         dungeonAnnelEpic=escape.json_decode(dungeonAnnelEpic),
                         dungeonAnnelGatesNormal=escape.json_decode(dungeonAnnelGatesNormal),
                         dungeonAnnelGatesHero=escape.json_decode(dungeonAnnelGatesHero),
                         dungeonAnnelGatesEpic=escape.json_decode(dungeonAnnelGatesEpic),
                         )
        # try:
        #     sign = yield self.generate_sign(idcard=idcard, zone=zone)
        # except E.USERNOTFOUND:
        #     self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
        #     return
        # except E.USERABNORMAL:
        #     self.write(dict(err=E.ERR_USER_ABNORMAL, msg=E.errmsg(E.ERR_USER_ABNORMAL)))
        #     return
        # except E.USERBEBANKED:
        #     self.write(dict(err=E.ERR_USER_BEBANKED, msg=E.errmsg(E.ERR_USER_BEBANKED)))
        #     return
        # except Exception:
        #     self.write(dict(err=E.ERR_UNKNOWN, msg=E.errmsg(E.ERR_UNKNOWN)))
        #     return
        # print 'sign', sign
        #ret = dict(users=users)
        #reb = zlib.compress(escape.json_encode(users))
        self.write(users)


@handler
class SyncHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Sync', '/sync/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', False, str, '1', '1', 'user_id'),
        Param('access_token', False, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
        Param('zone', True, str, '0', '0', 'zone'),
    ], filters=[ps_filter], description="Sync")
    def get(self):
        try:
            channel = self.get_argument("channel", "test1")
            idcard = self.get_argument("idcard")
            zone = self.get_argument("zone")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        if idcard:
            ahex, aid = idcard.split('h', 1)
            query = """SELECT nickname, avat, "playerLevel", "playerXp", "goldcoin", gem, "honorPoint",\
                  "arena5v5Rank", "arena5v5Place",  "arenaOtherRank", "arenaOtherPlace", "heroList", "soldierList",\
                   formations, items, "headIconList", "titleList", achievement, "playerConfig", "buddyList",\
                    "playerStatusInfo", jmails, "annalNormal", "annelCurrentGateNormal", "annalHero",\
                     "annelCurrentGateHero", "annalEpic", "dungeonAnnelHero", "dungeonAnnelEpic",\
                      "dungeonAnnelGatesNormal", "dungeonAnnelGatesHero", "dungeonAnnelGatesEpic"\
                       FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
            params = (ahex, aid)
            res = yield self.sql.runQuery(query, params)
            if res:
                nickname, avat, playerLevel, playerXp, goldcoin, gem, honorPoint, arena5v5Rank, arena5v5Place,\
                arenaOtherRank, arenaOtherPlace, heroList, soldierList, formations, items, headIconList, titleList,\
                achievement, playerConfig, buddyList, playerStatusInfo, jmails, annalNormal, annelCurrentGateNormal,\
                annalHero, annelCurrentGateHero, annalEpic, dungeonAnnelHero, dungeonAnnelEpic, dungeonAnnelGatesNormal, \
                dungeonAnnelGatesHero, dungeonAnnelGatesEpic = res[0]
            else:
                self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
                return
                # yield self.predis.hset('zone:%s:%s' % (zone, datetime.datetime.now().strftime('%Y%m%d')), aid, E.true)
            hero_list = []
            heroList = escape.json_decode(heroList)
            for index, one in enumerate(heroList):
                if int(one['unlock']) != 0:
                    hero_list.append(one)
            for index, one in enumerate(hero_list):
                one.pop("unlock")
            users = dict(avat=avat,
                         playerLevel=playerLevel,
                         playerXp=playerXp,
                         goldcoin=goldcoin,
                         gem=gem,
                         honorPoint=honorPoint,
                         arena5v5Rank=arena5v5Rank,
                         arena5v5Place=arena5v5Place,
                         arenaOtherRank=arenaOtherRank,
                         arenaOtherPlace=arenaOtherPlace,
                         unlockSlotNum=0,
                         heroList=hero_list,
                         soldierList=escape.json_decode(soldierList),
                         formations=escape.json_decode(formations),
                         items=escape.json_decode(items),
                         headIconList=escape.json_decode(headIconList),
                         titleList=escape.json_decode(titleList),
                         achievement=escape.json_decode(achievement),
                         playerConfig=escape.json_decode(playerConfig),
                         buddyList=escape.json_decode(buddyList),
                         playerStatusInfo=escape.json_decode(playerStatusInfo),
                         jmails=escape.json_decode(jmails),
                         annalNormal=escape.json_decode(annalNormal),
                         annelCurrentGateNormal=escape.json_decode(annelCurrentGateNormal),
                         annalHero=escape.json_decode(annalHero),
                         annelCurrentGateHero=escape.json_decode(annelCurrentGateHero),
                         annalEpic=escape.json_decode(annalEpic),
                         dungeonAnnelHero=escape.json_decode(dungeonAnnelHero),
                         dungeonAnnelEpic=escape.json_decode(dungeonAnnelEpic),
                         dungeonAnnelGatesNormal=escape.json_decode(dungeonAnnelGatesNormal),
                         dungeonAnnelGatesHero=escape.json_decode(dungeonAnnelGatesHero),
                         dungeonAnnelGatesEpic=escape.json_decode(dungeonAnnelGatesEpic),
                         )

        self.write(users)

#         uid = self.uid
#         user = yield self.get_user(uid)
#         notice = yield self.get_notice()
#         userseals = yield self.get_seal(uid)
#         if userseals:
#             sealnu, sealday = userseals
#         else:
#             sealnu, sealday = 0, 0
# 
#         userseals = [sealnu, sealday]
#         today = datetime.date.today().day
#         status = 0
#         if sealnu != 0 and sealday - 1 == today:
#             status = 1
#         if sealnu >= 14:
#             status = 1
# 
#         # yield self.set_mails(user)
#         if not user:
#             self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
#             return
#         # check mails
#         cuser = {}
#         mailcomming = E.checkmails(user)
#         if mailcomming:
#             cuser['mails'] = user['mails']
#             yield self.set_user(uid, **cuser)
#         yield self.set_mails(user)
#         # get hp and tick
#         hp, tick = yield self.get_hp(user)
#         user['hp'] = hp
#         user['tick'] = tick
#         user['card'] = yield self.get_card(user)
#         sp, tick = yield self.get_sp(user)
#         user['sp'] = sp
#         user['works'] = E.checkworks(user)
#         # entry related
#         entryopens = E.entryopens(user)
#         entrycounts = yield self.redis.hgetall("entrycount:%s" % uid)
#         entrytimers = yield self.redis.hgetall("entrytimer:%s" % uid)
#         lott = yield self.update_lott(user)
#         lottmark, mailmark, arenamark = yield self.check_redpot(user)
#         yield self.set_arena(uid)
#         bminterval = yield self.redis.get('blackmarket:%s' % uid)
#         if not bminterval:
#             bminterval = 0
#         else:
#             bminterval = bminterval - int(time.time())
#         minterval = yield self.redis.get('market:%s' % uid)
#         if not minterval:
#             minterval = 0
#         else:
#             minterval = minterval - int(time.time())
#         res = yield self.sql.runQuery("SELECT beautyid FROM core_beauty")
#         beautys = yield self.get_beautys(user)
#         user['beautys'] = beautys
#         res = yield self.sql.runQuery("SELECT created_at, ended_at FROM core_bigevent WHERE bid='20160001'")
#         if res:
#             created_at, ended_at = res[0]
#             created_at = int(time.mktime(created_at.timetuple()))
#             ended_at = int(time.mktime(ended_at.timetuple()))
#             now = int(time.mktime(datetime.datetime.now().timetuple()))
#             if now >= created_at and now <= ended_at:
#                 created_at, ended_at = res[0]
#                 created_at = created_at.date()
#                 now = datetime.datetime.now().date()
#                 day = (now - created_at).days
#                 hebdo = yield self.predis.get('hebdo:%s:%s:%s' % (ZONE_ID, uid, day))
#                 if not hebdo:
#                     yield self.send_hebdomail(user, day)
#         user['dayrecharge'] = 0
#         user['consume'] = yield self.get_consume(user)
#         user['inpour'] = yield self.get_inpour(user)
#         # print 'consume', user['consume'], user['inpour']
#         ret = dict(out=dict(user=user, entryopens=entryopens, entrycounts=entrycounts, entrytimers=entrytimers,
#                             lottfree=lottmark, \
#                             bminterval=bminterval, minterval=minterval, arena=arenamark, notice=notice,
#                             fourteenseal=[sealnu, status]), timestamp=int(time.time()))
#         reb = zlib.compress(escape.json_encode(ret))
#         self.write(ret)


