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


@handler
class AddHP(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Gm add hp', '/gm/add/hp', [
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('hp', True, str, '1', '1', 'hp'),
    ], filters=[ps_filter], description="Gm add hp")
    def get(self):
        try:
            user_id = self.get_argument("user_id")
            hp = int(self.get_argument("hp", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        # print 'hp:',hp
        user = yield self.get_player(user_id)
        currhp, tick = yield self.get_hp(user)
        currhp, tick = yield self.add_hp(user, hp)

        msg = "SUCCESS! curr hp: " + str(currhp)
        self.write(msg)


@handler
class EditProdHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Gm add prod', '/gm/add/prod/', [
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('pid', True, str, '01014', '01014', 'pid'),
        Param('num', False, str, '1', '1', 'num'),
        Param('seconds', False, int, '1', '1', 'seconds'),
    ], filters=[ps_filter], description="Gm add prod")
    def get(self):
        try:
            user_id = self.get_argument("user_id")
            pid = self.get_argument("pid")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        user = yield self.get_player(user_id)
        if 'l' in pid.split('_'):
            seconds = self.get_argument("seconds")
            expire_time = int(time.time()) + int(seconds)
            user['props'][pid] = expire_time
            msg = "SUCCESS! pid: " + pid + " expire_time:" + str(expire_time)
        else:
            num = int(self.get_argument("num", 1))
            if num > 0:
                if pid in user['props']:
                    user['props'][pid] += num
                else:
                    user['props'][pid] = num
            else:
                if pid in user['props']:
                    del user['props'][pid]
            msg = "SUCCESS! pid: " + pid + " curr num:" + str(num)
        cuser = dict(props=user['props'])
        yield self.set_player(user_id, **cuser)
        self.write(msg)
