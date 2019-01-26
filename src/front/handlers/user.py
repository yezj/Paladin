# -*- coding: utf-8 -*-

import random
import time
import zlib
import uuid
import json
import datetime
import pickle
from twisted.internet import defer
from twisted.python import log
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front.D import USERINIT, PREFIX, POSTFIX
from itertools import *
# from front.handlers.base import BaseHandler
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder
from front.utils import E
from front.wiapi import *
from front import D
from cyclone import web, escape

@handler
class GetHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('User get', '/user/get/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="User get")
    def post(self):
        nickname = yield self.get_nickname()
        self.write(dict(nickname=nickname))

@handler
class SetHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('User set', '/user/set/', [
        Param('channel', False, str, 'test1', 'test1', 'channel'),
        Param('nickname', False, str, '帅哥', '帅哥', 'nickname'),
        Param('avat', False, str, '1', '1', 'avat'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Task set")
    def post(self):
        nickname = self.get_argument("nickname", None)
        avat = self.get_argument("avat", None)

        uid = self.uid
        user = yield self.get_user(uid)
        nuser = {}
        if nickname:
            cuser = dict(nickname=user['nickname'])
            nuser = dict(nickname=nickname)
            yield self.redis.lpush("nickname", nickname)
        if avat:
            cuser = dict(avat=user['avat'])
            nuser = dict(avat=avat)
        if nickname and avat:
            cuser = dict(nickname=user['nickname'], avat=user['avat'])
            nuser = dict(nickname=nickname, avat=avat)
            yield self.redis.lpush("nickname", nickname)

        ctask = E.pushtasks(user)
        if ctask:
            cuser['tasks'] = user['tasks']
            nuser['tasks'] = user['tasks']
        cmail = E.checkmails(user)
        if cmail:
            cuser['mails'] = user['mails']
            nuser['mails'] = user['mails']
        if nuser:
            yield self.set_user(uid, **nuser)
            ret = dict(out=dict(user=nuser), timestamp=int(time.time()))
            reb = zlib.compress(escape.json_encode(ret))
            self.write(ret)
        else:
            self.set_status(400)
            return 

class RegisterHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get(self):
        try:
            name = self.get_argument("name")
            secret = self.get_argument("secret")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        res = yield self.sql.runQuery("SELECT id, name, secret FROM core_user WHERE name=%s LIMIT 1", (name, ))
        if not res:
            res = yield self.sql.runQuery("SELECT id, name, secret FROM core_user WHERE name=%s AND secret=%s LIMIT 1", (name, secret))
            if not res:        
                params = USERINIT
                params['name'] = name
                params['secret'] = secret
                params['timestamp'] = int(time.time())
                query = """INSERT INTO core_user(secret, name, avat, xp, gold, rock, feat, book, jheros, jprods, jbatts, jseals,\
                 jtasks, jworks, jmails, timestamp) VALUES (%(secret)s, %(name)s, %(avat)s, %(xp)s, %(gold)s, %(rock)s, %(feat)s,\
                  %(book)s, %(jheros)s, %(jprods)s, %(jbatts)s, %(jseals)s, %(jtasks)s, %(jworks)s, %(jmails)s, %(timestamp)s) RETURNING id"""
                for i in range(5):
                    try:
                        sql = yield self.sql.runQuery(query, params)
                        break
                    except storage.IntegrityError:
                        log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                        sql = None
                        continue
                if sql:
                    uid = sql[0][0]
                else:
                    raise web.HTTPError(500, 'Create user failed')
            else:
                uid =res[0][0]
        else:
            raise web.HTTPError(400, 'user exist, please try another name')

        self.write(dict(uid=uid))

class LoginHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    def get(self):
        try:
            name = self.get_argument("name")
            secret = self.get_argument("secret")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        res = yield self.sql.runQuery("SELECT id, name, secret FROM core_user WHERE name=%s LIMIT 1", (name, ))
        if not res:
            raise web.HTTPError(401, "User does not exist")
        else:
            res = yield self.sql.runQuery("SELECT id, name, secret FROM core_user WHERE name=%s AND secret=%s LIMIT 1", (name, secret))
            if not res:
                raise web.HTTPError(401, "User does not exist or password error, please register")

        self.write(dict(uid=res[0][0]))

@handler
class CreateArenaHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('User set', '/create/arena/', [
    ], filters=[ps_filter], description="Task set")
    def get(self):
        res = yield self.sql.runQuery("SELECT id, jheros FROM core_user WHERE xp>=900000 ORDER BY id")
        j = 1
        for r in res:

            iid, heros = r
            arena = yield self.sql.runQuery("select * from core_arena where user_id=%s", (iid, ))
            heros = escape.json_decode(heros)
            guards = {'01001':heros['01001'], '01002':heros['01002']}
            guards = escape.json_encode(guards)
            query = "INSERT INTO core_arena(user_id, arena_coin, before_rank, now_rank, last_rank, jguards, jpositions, formation, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
            params = (iid, 0, j, j, j, guards, '{"01001": "-1","01002": "-1"}', E.default_formation, int(time.time()))
            print query % params
            for i in range(5):
                try:
                    yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
            j += 1




