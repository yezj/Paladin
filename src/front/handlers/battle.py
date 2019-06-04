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
class GetHandler(ApiHandler):
    # @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Battle get', '/battle/get/', [
        Param('gate_id', True, str, '010208_0', '010208_0', 'gate_id'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, 'bb6ab3286a923c66088f790c395c0d11019c075b',
              'bb6ab3286a923c66088f790c395c0d11019c075b', 'access_token'),
    ], filters=[ps_filter], description="Battle get")
    def get(self):
        try:
            gate_id = self.get_argument("gate_id")
            access_token = self.get_argument("access_token")
            user_id = self.get_argument("user_id")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        query = "SELECT id, username, password_hash, access_token, refresh_token FROM core_user WHERE id=%s AND" \
                " access_token=%s LIMIT 1"
        res = yield self.sql.runQuery(query, (user_id, access_token))
        if res:
            battle_id = uuid.uuid4().hex
            user = yield self.get_player(user_id)
            print user
            user.update(dict(gate_id=gate_id))
            if not user:
                self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
                return
            now_hp, tick = yield self.get_hp(user)
            if now_hp >= E.hplimit:
                yield self.set_flush(battle_id, user)
                ret = dict(user=user)
                reb = zlib.compress(escape.json_encode(ret))
                self.write(dict(battle_id=battle_id, timestamp=int(time.time())))
            else:
                self.write(dict(err=E.ERR_NOTENOUGH_HP, msg=E.errmsg(E.ERR_NOTENOUGH_HP)))
                return
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

@handler
class SetHandler(ApiHandler):
    # @utils.token
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('Battle set', '/battle/set/', [
        Param('battle_id', True, str, '010208_0', '010208_0', 'battle_id'),
        Param('user_id', True, str, '1', '1', 'user_id'),
        Param('access_token', True, str, 'bb6ab3286a923c66088f790c395c0d11019c075b',
              'bb6ab3286a923c66088f790c395c0d11019c075b', 'access_token'),
        Param('star', True, str, '1', '1', 'star'),
        Param('point', True, str, '1', '1', 'point'),
    ], filters=[ps_filter], description="Battle set")
    def get(self):
        try:
            battle_id = self.get_argument("battle_id")
            access_token = self.get_argument("access_token")
            user_id = self.get_argument("user_id")
            star = self.get_argument("star")
            point = self.get_argument("point")
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return

        query = "SELECT id, username, password_hash, access_token, refresh_token FROM core_user WHERE id=%s AND" \
                " access_token=%s LIMIT 1"
        res = yield self.sql.runQuery(query, (user_id, access_token))
        if res:
            battle = yield self.get_flush(battle_id)
            if battle:
                hp, tick = yield self.add_hp(battle, -E.hplimit)
                gates = battle['gates']
                gate_id = battle['gate_id']
                gates[gate_id] = [star, point]
                yield self.set_player(user_id, gates=gates)
            else:
                self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
                return
            user = yield self.get_player(user_id)
            user.update(dict(hp=hp, tick=tick, timestamp=int(time.time())))
            # ret = dict(timestamp=int(time.time()), data=data)
            # reb = zlib.compress(escape.json_encode(ret))
            self.write(user)
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return