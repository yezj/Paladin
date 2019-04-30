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
    @api('Gate get', '/gate/get/', [
        Param('gate_id', True, str, '010208_0', '010208_0', 'gate_id'),
    ], filters=[ps_filter], description="Gate get")
    def get(self):
        try:
            gate_id = self.get_argument("gate_id")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        res = yield self.sql.runQuery("""SELECT gate_id, vers, rs, "itemTypes",  props, "taskStep", tasks, scores, gird,
                    "newGridTypes", "newGrid", portal, item, "itemBg", "wallH", "wallV", "taskBgItem", "wayDownOut",
                     attach, diff, "taskType", "trackBelt", "movingFloor", "flipBlocker" FROM core_gate WHERE gate_id=%s LIMIT 1""",
                                      (gate_id,))
        if res:
            gate_id, vers, rs, itemTypes, props, taskStep, tasks, scores, gird, newGridTypes, newGrid, portal, item, \
            itemBg, wallH, wallV, taskBgItem, wayDownOut, attach, diff, taskType, trackBelt, movingFloor, flipBlocker = \
            res[0]
            # print 'jgates', jgates
            # jgates = escape.json_decode(jgates)
            # print type(name_2P)
            # print type(escape.json_decode(name_2P))

            jgates = dict(gate_id=gate_id,
                          vers=vers,
                          rs=escape.json_decode(rs),
                          itemTypes=escape.json_decode(itemTypes),
                          props=escape.json_decode(props),
                          taskStep=taskStep,
                          tasks=escape.json_decode(tasks),
                          scores=escape.json_decode(scores),
                          gird=escape.json_decode(gird),
                          newGridTypes=escape.json_decode(newGridTypes),
                          newGrid=escape.json_decode(newGrid),
                          portal=escape.json_decode(portal),
                          item=escape.json_decode(item),
                          itemBg=escape.json_decode(itemBg),
                          wallH=escape.json_decode(wallH),
                          wallV=escape.json_decode(wallV),
                          taskBgItem=escape.json_decode(taskBgItem),
                          wayDownOut=escape.json_decode(wayDownOut),
                          attach=escape.json_decode(attach),
                          diff=diff,
                          taskType=taskType,
                          trackBelt=escape.json_decode(trackBelt),
                          flipBlocker=escape.json_decode(flipBlocker),
                          movingFloor=escape.json_decode(movingFloor)
                          )
        else:
            jgates = dict(timestamp=int(time.time()))
        # jgates.update(dict(gate_id=gate_id, timestamp=int(time.time())))
        # ret = dict(result=jgates)
        # reb = zlib.compress(escape.json_encode(ret))
        self.write(jgates)


@handler
class SetHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    # @utils.signed
    @api('Gate set', '/gate/set/', [
        Param('gate_id', True, str, '010208_0', '010208_0', 'gate_id'),
        Param('vers', True, str, '1.0', '1.0', 'vers'),
        Param('rs', True, str, '[]', '[]', 'rs'),
        Param('itemTypes', True, str, '[]', '[]', 'itemTypes'),
        Param('props', True, str, '[]', '[]', 'props'),

        Param('taskStep', True, int, 0, 0, 'taskStep'),
        Param('tasks', True, str, '[]', '[]', 'tasks'),
        Param('scores', True, str, '[]', '[]', 'scores'),
        Param('gird', True, str, '[]', '[]', 'gird'),
        Param('newGridTypes', True, str, '[]', '[]', 'newGridTypes'),
        Param('newGrid', True, str, '[]', '[]', 'newGrid'),
        Param('portal', True, str, '[]', '[]', 'portal'),
        Param('item', True, str, '[]', '[]', 'item'),

        Param('itemBg', True, str, '[]', '[]', 'itemBg'),
        Param('wallH', True, str, '[]', '[]', 'wallH'),
        Param('wallV', True, str, '[]', '[]', 'wallV'),

        Param('taskBgItem', True, str, '[]', '[]', 'taskBgItem'),
        Param('wayDownOut', True, str, '[]', '[]', 'wayDownOut'),
        Param('attach', True, str, '[]', '[]', 'attach'),
        Param('diff', True, str, 'h', 'h', 'diff'),
        Param('taskType', True, str, '0', '0', 'taskType'),
        Param('trackBelt', True, str, '[]', '[]', 'trackBelt'),
        Param('flipBlocker', True, str, '[]', '[]', 'flipBlocker'),
        Param('movingFloor', True, str, '[]', '[]', 'movingFloor'),

    ], filters=[ps_filter], description="Gate set")
    def get(self):
        import sys
        reload(sys)
        sys.setdefaultencoding("utf-8")
        try:
            gate_id = self.get_argument("gate_id")
            vers = self.get_argument("vers")
            rs = self.get_argument("rs")
            itemTypes = self.get_argument("itemTypes")
            props = self.get_argument("props")

            taskStep = self.get_argument("taskStep")
            tasks = self.get_argument("tasks")
            scores = self.get_argument("scores")
            gird = self.get_argument("gird")
            newGridTypes = self.get_argument("newGridTypes")
            newGrid = self.get_argument("newGrid")
            portal = self.get_argument("portal")
            item = self.get_argument("item")

            itemBg = self.get_argument("itemBg")
            wallH = self.get_argument("wallH")
            wallV = self.get_argument("wallV")

            taskBgItem = self.get_argument("taskBgItem")
            wayDownOut = self.get_argument("wayDownOut")
            attach = self.get_argument("attach")
            diff = self.get_argument("diff")
            taskType = self.get_argument("taskType")
            trackBelt = self.get_argument("trackBelt")
            flipBlocker = self.get_argument("flipBlocker")
            movingFloor = self.get_argument("movingFloor")

        except Exception:
            raise web.HTTPError(400, "Argument error")
        res = yield self.sql.runQuery("SELECT * FROM core_gate WHERE gate_id=%s LIMIT 1", (gate_id,))
        if not res:
            query = """INSERT INTO core_gate (gate_id, vers, rs, "itemTypes", props, "taskStep", tasks, scores, gird,
                    "newGridTypes", "newGrid", portal, item, "itemBg", "wallH", "wallV", "taskBgItem", "wayDownOut",
                     attach, diff, "taskType", "trackBelt", "movingFloor", "flipBlocker", created, modified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) RETURNING id"""
            params = (
                gate_id, vers, rs, itemTypes, props, taskStep, tasks, scores, gird, newGridTypes, newGrid, portal, item,
                itemBg, wallH, wallV, taskBgItem, wayDownOut, attach, diff, taskType, trackBelt, movingFloor, flipBlocker)
            print query % params
            for i in range(5):
                try:
                    gid = yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
        else:
            query = """UPDATE core_gate SET vers=%s, rs=%s, "itemTypes"=%s, props=%s, "taskStep"=%s, tasks=%s,
                    scores=%s, gird=%s, "newGridTypes"=%s, "newGrid"=%s, portal=%s, item=%s,
                     "itemBg"=%s, "wallH"=%s, "wallV"=%s, "taskBgItem"=%s, "wayDownOut"=%s, attach=%s, diff=%s,
                      "taskType"=%s, "trackBelt"=%s, "movingFloor"=%s, "flipBlocker"=%s WHERE gate_id=%s"""
            params = (
                vers, rs, itemTypes, props, taskStep, tasks, scores, gird, newGridTypes, newGrid, portal, item, itemBg,
                wallH, wallV, taskBgItem, wayDownOut, attach, diff, taskType, trackBelt, movingFloor, flipBlocker, gate_id)
            print query % params
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
