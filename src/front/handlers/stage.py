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
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Stage get', '/stage/get/', [
        Param('stage_id', True, str, '010208_0', '010208_0', 'gate_id'),
    ], filters=[ps_filter], description="Stage get")
    def get(self):
        try:
            stage_id = self.get_argument("stage_id")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        jstages = dict(stage_id=stage_id,
                       battleId=uuid.uuid4().hex,
                       rs1=456897635,
                       rs2=12345,
                       is1PLeft=False,
                       name_1P=u"我要霸占你你秀发",
                       name_2P=u"浅时光Bonns",
                       level_1P=23,
                       level_2P=35,
                       icon_1P=301,
                       icon_2P=401,
                       resource=500,
                       resourceLimit=1000,
                       resourceGrowSpeed=1,
                       timestamp=int(time.time()))
        jstages = dict(stage_id=stage_id, battleId=uuid.uuid4().hex, rs1=456897635, rs2=12345, is1PLeft=False,
                       name_1P=u"我要霸占你你秀发",
                       name_2P=u"浅时光Bonns", level_1P=23, level_2P=35, icon_1P=301, icon_2P=401, resource=500,
                       resourceLimit=1000, resourceGrowSpeed=1, timestamp=int(time.time()))


        res = yield self.sql.runQuery("SELECT jgates FROM core_gate WHERE gate_id=%s LIMIT 1", (stage_id,))
        if res:
            jgates, = res[0]
            jgates = escape.json_decode(jgates)
        else:
            jgates = {}

        jgates.update(jstages)
        self.write(jgates)


@handler
class SetHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Stage set', '/stage/set/', [
        Param('gate_id', True, str, '010208_0', '010208_0', 'gate_id'),
        Param('jgates', True, str, '{}', '{}', 'jgates'),
    ], filters=[ps_filter], description="Stage set")
    def get(self):

        try:
            stage_id = self.get_argument("stage_id")
            jgates = self.get_argument("jgates")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        res = yield self.sql.runQuery("SELECT jgates FROM core_gate WHERE gate_id=%s LIMIT 1", (gate_id,))
        if not res:
            query = "INSERT INTO core_gate (gate_id, jgates, timestamp) VALUES (%s, %s, %s) RETURNING id"
            params = (gate_id, escape.json_encode(jgates), int(time.time()))
            for i in range(5):
                try:
                    gid = yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        else:
            query = "UPDATE core_gate SET jgates=%s,  timestamp=%s WHERE gate_id=%s"
            params = (escape.json_encode(jgates), int(time.time()), gate_id)
            for i in range(5):
                try:
                    yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        # jgates = escape.json_encode(jgates)
        ret = dict(timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)
