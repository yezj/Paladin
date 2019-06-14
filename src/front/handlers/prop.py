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
class SetHandler(ApiHandler):
    @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Prop use', '/prop/use/', [
        Param('battle_id', True, str, '010208_0', '010208_0', 'battle_id'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, 'bb6ab3286a923c66088f790c395c0d11019c075b',
              'bb6ab3286a923c66088f790c395c0d11019c075b', 'access_token'),
        Param('pid', True, str, 'b_1', 'b_1', 'pid'),
        Param('num', True, str, '1', '1', 'num'),
    ], filters=[ps_filter], description="Prop use")
    def get(self):
        try:
            battle_id = self.get_argument("battle_id")
            pid = self.get_argument("pid")
            num = int(self.get_argument("num"))
        except Exception as e:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        user_id = self.user_id
        if user_id:
            user = yield self.get_player(user_id)
            if user:
                props = user['props']
                if pid in props:
                    if props[pid] >= num:
                        props[pid] = props[pid] - num
                    else:
                        self.write(dict(err=E.ERR_NOTENOUGH_PROD, msg=E.errmsg(E.ERR_NOTENOUGH_PROD)))
                        return
                else:
                    self.write(dict(err=E.ERR_NOTENOUGH_PROD, msg=E.errmsg(E.ERR_NOTENOUGH_PROD)))
                    return
                #yield self.set_player(user_id, props=props)
                cuser = dict(props=props)
                yield self.set_player(user_id, **cuser)
            else:
                self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
                return
            user = yield self.get_player(user_id)
            now_hp, tick = yield self.get_hp(user)
            user.update(dict(hp=now_hp, tick=tick, timestamp=int(time.time())))
            # ret = dict(timestamp=int(time.time()), data=data)
            # reb = zlib.compress(escape.json_encode(ret))
            self.write(user)
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
