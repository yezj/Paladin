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
    @api('Formation get', '/formation/get/', [
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
    ], filters=[ps_filter], description="Formation get")
    def get(self):
        try:
            idcard = self.get_argument("idcard")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        out = []
        if idcard:
            ahex, aid = idcard.split('h', 1)
            query = """SELECT formations FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
            params = (ahex, aid)
            res = yield self.sql.runQuery(query, params)
            if res:
                formations, = res[0]
                formations = escape.json_decode(formations)
                for index, one in enumerate(formations):
                    if len(one["formation"]) != 0:
                        out.append(one)
            else:
                self.write(dict(err=E.ERR_USER_NOTFOUND, msg=E.errmsg(E.ERR_USER_NOTFOUND)))
                return
            users = dict(formations=out)
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
    @api('Formation set', '/formation/set/', [
        Param('idcard', True, str, '864c04bf73a445fd84da86a206060c48h20', '864c04bf73a445fd84da86a206060c48h20',
              'idcard'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, '55526fcb39ad4e0323d32837021655300f957edc',
              '55526fcb39ad4e0323d32837021655300f957edc', 'access_token'),
        Param('slotId', True, int, 0, 0, 'slotId'),
        Param('formation', True, str, '[]', '[]', 'formation'),
    ], filters=[ps_filter], description="Formation set")
    def get(self):

        try:
            idcard = self.get_argument("idcard")
            slotId = self.get_argument("slotId")
            formation = self.get_argument("formation")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        formation = escape.json_decode(formation)
        ahex, aid = idcard.split('h', 1)
        query = """SELECT formations FROM core_user WHERE hex=%s and id=%s LIMIT 1"""
        params = (ahex, aid)
        res = yield self.sql.runQuery(query, params)
        IS_EXISTED = True
        if res:
            formations, = res[0]
            formations = escape.json_decode(formations)
            for index, one in enumerate(formations):
                print index, one
                print one["slotId"] == int(slotId)
                if int(one["slotId"]) == int(slotId):
                    if len(formation) == 0:
                        del formations[index]
                    else:
                        formations[index] = dict(slotId=int(slotId), formation=formation)
                    IS_EXISTED = False
            print formations
            if IS_EXISTED and len(formation) != 0:
                formations.append(dict(slotId=int(slotId), formation=formation))

            print 'formations', formations
            # print escape.json_encode(dict(slotId=slotId, formation=formation))

            query = "UPDATE core_user SET formations=%s WHERE hex=%s and id=%s"
            params = (escape.json_encode(formations), ahex, aid)
            for i in range(5):
                try:
                    yield self.sql.runOperation(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
            # ret = dict(timestamp=int(time.time()))
            # reb = zlib.compress(escape.json_encode(ret))
            self.write(dict(formations=formations))
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
