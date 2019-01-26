# -*- coding: utf-8 -*-

import random
import time
import zlib
import uuid
from twisted.internet import defer
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front import storage, D
from twisted.python import log
# from front.handlers.base import BaseHandler
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder
from local_settings import ZONE_ID


@handler
class GetHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Hero get', '/hero/get/', [
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
    ], filters=[ps_filter], description="Hero get")
    def get(self):
        try:
            idcard = self.get_argument("idcard")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        hero_list = []
        if idcard:
            ahex, aid = idcard.split('h', 1)
            query = """SELECT "heroList" FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
            params = (ahex, aid)
            res = yield self.sql.runQuery(query, params)
            if res:
                heroList, = res[0]
                heroList = escape.json_decode(heroList)
                for index, one in enumerate(heroList):
                    if int(one['unlock']) != 0:
                        hero_list.append(one)
                for index, one in enumerate(hero_list):
                    one.pop("unlock")
            else:
                self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
                return
            users = dict(heroList=hero_list)
            self.write(users)
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return


@handler
class SetHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Hero set', '/hero/set/', [
        Param('id', True, str, '1', '1', 'id'),
        Param('level', False, str, '1', '1', 'level'),
        Param('xp', False, str, '0', '0', 'xp'),
        Param('goldSkin', False, int, 0, 0, 'goldSkin'),
        Param('unlock', False, int, 0, 0, 'unlock'),
        Param('star', False, int, 0, 0, 'star'),
        Param('grade', False, int, 0, 0, 'grade'),
        Param('skillLevel', False, str, '[7,3,1]', '[7,3,1]', 'skillLevel'),
        Param('talent', False, str, '[0,2,1,2]', '[0,2,1,2]', 'talent'),
        Param('cardNum', False, int, 120, 120, 'cardNum'),
        Param('soldierCooldownTime', False, int, 3600, 3600, 'soldierCooldownTime'),
        Param('currentSoldierIndex', False, int, 0, 0, 'currentSoldierIndex'),
        Param('leadingSoldierData', False, str, '{}', '{}', 'leadingSoldierData'),
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
    ], filters=[ps_filter], description="Hero set")
    def get(self):
        try:
            id = self.get_argument("id")
            level = self.get_argument("level", None)
            xp = self.get_argument("xp", None)
            goldSkin = self.get_argument("goldSkin", None)
            unlock = self.get_argument("unlock", None)
            star = self.get_argument("star", None)
            grade = self.get_argument("grade", None)
            skillLevel = self.get_argument("skillLevel", None)
            talent = self.get_argument("talent", None)
            cardNum = self.get_argument("cardNum", None)
            soldierCooldownTime = self.get_argument("soldierCooldownTime", None)
            currentSoldierIndex = self.get_argument("currentSoldierIndex", None)
            leadingSoldierData = self.get_argument("leadingSoldierData", None)

            idcard = self.get_argument("idcard")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        ahex, aid = idcard.split('h', 1)
        query = """SELECT "heroList" FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
        params = (ahex, aid)
        res = yield self.sql.runQuery(query, params)
        if res:
            IS_EXISTED = True
            heros = {}
            if level:
                heros.update(dict(level=level))
            if xp:
                heros.update(dict(xp=xp))
            if goldSkin:
                heros.update(dict(goldSkin=goldSkin))
            if unlock:
                heros.update(dict(unlock=unlock))
            if star:
                heros.update(dict(star=star))
            if grade:
                heros.update(dict(grade=grade))
            if skillLevel:
                heros.update(dict(skillLevel=skillLevel))
            if talent:
                heros.update(dict(talent=talent))
            if cardNum:
                heros.update(dict(cardNum=cardNum))
            if soldierCooldownTime:
                heros.update(dict(soldierCooldownTime=soldierCooldownTime))
            if currentSoldierIndex:
                heros.update(dict(currentSoldierIndex=currentSoldierIndex))
            if leadingSoldierData:
                heros.update(dict(leadingSoldierData=leadingSoldierData))
            print 'heros', heros
            heroList, = res[0]
            heroList = escape.json_decode(heroList)
            for index, one in enumerate(heroList):
                print index, one
                if int(one["id"]) == int(id):
                    heroList[index].update(heros)
                    IS_EXISTED = False
            if unlock:
                if IS_EXISTED and int(unlock) == 1:
                    for one in D.HERO:
                        if "id" in one:
                            if int(one["id"]) == int(id) and int(one["able"]) == 1:
                                heroList.append(
                                    dict(id=id,
                                         level=1,
                                         xp=0,
                                         goldSkin=0,
                                         unlock=1, star=0,
                                         grade=0,
                                         skillLevel=[2, 3],
                                         talent=[2, 3])
                                )

            # for index, one in enumerate(heroList):
            #     print index, one
            #     if int(one["level"]) != 0:
            #         hero_list.append(one)

            query = """UPDATE core_user SET "heroList"=%s WHERE hex=%s and id=%s"""
            params = (escape.json_encode(heroList), ahex, aid)
            for i in range(5):
                try:
                    yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
            # ret = dict(timestamp=int(time.time()))
            # reb = zlib.compress(escape.json_encode(ret))
            hero_list = []
            for index, one in enumerate(heroList):
                if int(one["unlock"]) != 0:
                    one.pop("unlock")
                    hero_list.append(one)
            self.write(dict(heroList=hero_list))
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
