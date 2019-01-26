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
    @api('Battle get', '/battle/get/', [
        Param('stage_id', True, str, '010208_0', '010208_0', 'stage_id'),
        Param('opponent_Id_card', True, str, '', '', 'opponent_Id_card'),
        Param('access_token', True, str, '010208_0', '010208_0', 'access_token'),
        Param('idcard', True, str, '010208_0', '010208_0', 'idcard'),
        Param('user_id', True, str, '7', '7', 'user_id'),
        Param('heros1P', True, str, '[]', '[]', 'heros1P'),
    ], filters=[ps_filter], description="Battle get")
    def get(self):
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

        query = """SELECT "playerLevel", avat, nickname FROM core_user WHERE user_id=%s LIMIT 1"""
        res = yield self.sql.runQuery(query, (user_id,))
        if res:
            playerLevel, avat, nickname = res[0]
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        battleId = uuid.uuid4().hex

        res = yield self.sql.runQuery("""SELECT gate_id, type, "is1PLeft", "winCondition", "winTarget", "winTargetNum",
                    "lostTarget", "lostTargetNum", "winTime", "rageTime", "supplyNow1p", "supplyMax1p", "supplyGrowSpeed1p",
                    "supplyNow2p", "supplyMax2p", "supplyGrowSpeed2p",
                     map, barrie, "wave1P", "wave2P", "name_2P", "level_2P", "icon_2P", "herosNum1P", "herosPermit1P",
                      "herosLevelPermit1P", "soldiersPermit1P", "heros2P", "initTeam1P", "initTeam2P", "pathType", "barricade" FROM core_gate
                       WHERE gate_id=%s LIMIT 1""", (stage_id,))

        if res:
            gate_id, type, is1PLeft, winCondition, winTarget, winTargetNum, lostTarget, lostTargetNum, winTime, \
            rageTime, supplyNow1p, supplyMax1p, supplyGrowSpeed1p, supplyNow2p, supplyMax2p, supplyGrowSpeed2p, map, barrie, wave1P, wave2P, name_2P, level_2P, \
            icon_2P, herosNum1P, herosPermit1P, herosLevelPermit1P, soldiersPermit1P, heros2P, initTeam1P, \
            initTeam2P, pathType, barricade = res[0]
            # print 'jgates', jgates
            # jgates = escape.json_decode(jgates)
            jgates = dict(battleId=battleId,
                          level_1P=playerLevel,
                          icon_1P=avat,
                          name_1P=nickname,
                          heros1P=escape.json_decode(heros1P),
                          gate_id=gate_id,
                          type=type,
                          is1PLeft=is1PLeft,
                          winCondition=winCondition,
                          winTarget=winTarget,
                          winTargetNum=winTargetNum,
                          lostTarget=lostTarget,
                          lostTargetNum=lostTargetNum,
                          winTime=winTime,
                          rageTime=rageTime,
                          supplyNow1p=supplyNow1p,
                          supplyMax1p=supplyMax1p,
                          supplyGrowSpeed1p=supplyGrowSpeed1p,
                          supplyNow2p=supplyNow2p,
                          supplyMax2p=supplyMax2p,
                          supplyGrowSpeed2p=supplyGrowSpeed2p,
                          map=map,
                          barrie=escape.json_decode(barrie),
                          wave1P=escape.json_decode(wave1P),
                          wave2P=escape.json_decode(wave2P),
                          name_2P=name_2P,
                          level_2P=level_2P,
                          icon_2P=icon_2P,
                          herosNum1P=herosNum1P,
                          herosPermit1P=escape.json_decode(herosPermit1P),
                          herosLevelPermit1P=herosLevelPermit1P,
                          soldiersPermit1P=escape.json_decode(soldiersPermit1P),
                          heros2P=escape.json_decode(heros2P),
                          initTeam1P=escape.json_decode(initTeam1P),
                          initTeam2P=escape.json_decode(initTeam2P),
                          pathType=pathType,
                          barricade=barricade,
                          timestamp=int(time.time())
                          )
            # jgates.update(dict(battleId=battleId,
            #                    level_1P=playerLevel,
            #                    icon_1P=avat,
            #                    name_1P=nickname,
            #                    heros1P=escape.json_decode(heros1P),
            #                    timestamp=int(time.time())))
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        yield self.set_flush(battleId, jgates)

        # ret = dict(result=jgates)
        # reb = zlib.compress(escape.json_encode(ret))
        self.write(jgates)


@handler
class SetHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Battle set', '/battle/set/', [
        Param('battleId', True, str, '010208_0', '010208_0', 'stage_id'),
        Param('access_token', True, str, '010208_0', '010208_0', 'access_token'),
        Param('idcard', True, str, '010208_0', '010208_0', 'idcard'),
        Param('user_id', True, str, '7', '7', 'user_id'),
        Param('totalFrame', True, int, 23432, 23432, 'totalFrame'),
        Param('record', True, str, '-1', '-1', 'record'),
        Param('antiCheatCode', True, str, '2327e474291a71427e474291a725', '2327e474291a71427e474291a725',
              'antiCheatCode'),
    ], filters=[ps_filter], description="Battle set")
    def get(self):
        try:
            battleId = self.get_argument("battleId")
            access_token = self.get_argument("access_token")
            idcard = self.get_argument("idcard")
            user_id = self.get_argument("user_id")
            totalFrame = self.get_argument("totalFrame")
            record = self.get_argument("record")
            antiCheatCode = self.get_argument("antiCheatCode")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        batt = yield self.get_flush(battleId)
        if batt:
            heroLvl = [
                          "23", "24", "5", "4"
                      ],
            heroExp = [
                          2134, 2314, 213, 213
                      ],
            # 胜利时间
            timestamp = 2421432143,
            # 保存录像的话，录像ID，唯一，不保存则为空
            recordId = ""
            # 获得的金币
            goldCoin = 0,
            # 获得的钻石, 没有钻石则为0
            gem = 0,
            # 获得物品的话，物品列表
            item = [
                {"id": 214322, "num": 1},
                {"id": 214322, "num": 1},
                {"id": 214322, "num": 1}
            ]
            data = dict(heroLvl=heroLvl,
                        heroExp=heroExp,
                        timestamp=timestamp,
                        recordId=recordId,
                        goldCoin=goldCoin,
                        gem=gem,
                        item=item
                        )
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        # ret = dict(timestamp=int(time.time()), data=data)
        # reb = zlib.compress(escape.json_encode(ret))
        self.write(data)
