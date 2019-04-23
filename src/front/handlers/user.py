# -*- coding: utf-8 -*-

import os
import random
import time
import zlib
import uuid
import json
import datetime
import pickle
import binascii
import base64
from twisted.internet import defer
from twisted.python import log
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from passlib.apps import custom_app_context as pwd_context
from front.D import USERINIT, PREFIX, POSTFIX
from itertools import *
# from front.handlers.base import BaseHandler
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder
from itsdangerous import (TimedJSONWebSignatureSerializer
                          as Serializer, BadSignature, SignatureExpired)
from front.utils import E
from front.wiapi import *
from front import D
from cyclone import web, escape


@handler
class RegisterHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('User register', '/user/register/', [
        Param('username', True, str, 'test1', 'test1', 'username'),
        Param('password', True, str, 'test1', 'test1', 'password'),
    ], filters=[ps_filter], description="User register")
    def get(self):
        try:
            username = self.get_argument("username")
            password = self.get_argument("password")
        except Exception:
            raise web.HTTPError(400, "Argument error")
        res = yield self.sql.runQuery(
            "SELECT id, username, password_hash FROM core_user WHERE username=%s AND password_hash=%s LIMIT 1",
            (username, password))
        if not res:
            username = username
            password_hash = password  # pwd_context.encrypt(password)
            access_token = str(binascii.hexlify(os.urandom(20)).decode())
            refresh_token = str(binascii.hexlify(os.urandom(20)).decode())
            created = int(time.time())
            modified = int(time.time())
            query = "INSERT INTO core_user(username, password_hash, access_token, refresh_token, created, modified)" \
                    " VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
            params = (username, password_hash, access_token, refresh_token, created, modified)
            print query % params
            for i in range(5):
                try:
                    user = yield self.sql.runQuery(query, params)
                    break
                except storage.IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue
            print user
            if user:
                user_id = user[0][0]
                self.redis.set('access_token:%s' % access_token, user_id, D.EXPIRATION)
                self.write(dict(user_id=user_id, access_token=access_token, refresh_token=refresh_token))
                return
            else:
                self.write(dict(err=E.ERR_USER_CREATED, msg=E.errmsg(E.ERR_USER_CREATED)))
                return
        else:
            self.write(dict(err=E.ERR_USER_REPEAT, msg=E.errmsg(E.ERR_USER_REPEAT)))
            return


@handler
class LoginHandler(ApiHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @api('User login', '/user/login/', [
        Param('username', False, str, 'test1', 'test1', 'username'),
        Param('password', False, str, 'test1', 'test1', 'password'),
        Param('user_id', False, str, '1', '1', 'user_id'),
        Param('access_token', False, str, 'bb6ab3286a923c66088f790c395c0d11019c075b', 'bb6ab3286a923c66088f790c395c0d11019c075b', 'access_token'),
        Param('refresh_token', False, str, 'bb6ab3286a923c66088f790c395c0d11019c075b', 'bb6ab3286a923c66088f790c395c0d11019c075b', 'refresh_token'),
    ], filters=[ps_filter], description="User login")
    def get(self):
        try:
            username = self.get_argument("username", None)
            password = self.get_argument("password", None)
            user_id = self.get_argument("user_id", None)
            access_token = self.get_argument("access_token", None)
            refresh_token = self.get_argument("refresh_token", None)
        except Exception:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
        print self.has_arg("access_token"), self.has_arg("user_id")
        if username and password:
            query = "SELECT id, username, password_hash, access_token, refresh_token FROM core_user WHERE username=%s AND" \
                    " password_hash=%s LIMIT 1"
            r = yield self.sql.runQuery(query, (username, password))
            if r:
                user_id, username, password_hash, _access_token, _refresh_token = r[0]
                access_token_redis = self.redis.get('access_token:%s' % access_token)
                if not access_token_redis:
                    _access_token = binascii.hexlify(os.urandom(20)).decode()
                    _refresh_token = binascii.hexlify(os.urandom(20)).decode()
                    query = "UPDATE core_user SET access_token=%s, refresh_token=%s, modified=%s WHERE id=%s"
                    params = (_access_token, _refresh_token, int(time.time()), user_id)
                    for i in range(5):
                        try:
                            yield self.sql.runOperation(query, params)
                            break
                        except storage.IntegrityError:
                            log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                            continue
                self.redis.set('access_token:%s' % _access_token, user_id, D.EXPIRATION)
                self.write(dict(user_id=user_id, access_token=_access_token, refresh_token=_refresh_token))
                return
            else:
                self.write(dict(err=E.ERR_USER_PASSWORD, msg=E.errmsg(E.ERR_USER_PASSWORD)))
                return
        elif self.has_arg("access_token") and self.has_arg("user_id"):
            query = "SELECT id, username, password_hash, access_token, refresh_token FROM core_user WHERE id=%s AND" \
                    " access_token=%s LIMIT 1"
            r = yield self.sql.runQuery(query, (user_id, access_token))
            if r:
                user_id, username, password_hash, _access_token, _refresh_token = r[0]
                access_token_redis = yield self.redis.get('access_token:%s' % _access_token)
                #print 'access_token_redis', access_token_redis
                if not access_token_redis:
                    if self.has_arg("refresh_token"):
                        if self.arg("refresh_token") == _refresh_token:
                            _access_token = binascii.hexlify(os.urandom(20)).decode()
                            _refresh_token = binascii.hexlify(os.urandom(20)).decode()
                            query = "UPDATE core_user SET access_token=%s, refresh_token=%s, modified=%s WHERE id=%s"
                            params = (_access_token, _refresh_token, int(time.time()), user_id)
                            for i in range(5):
                                try:
                                    yield self.sql.runOperation(query, params)
                                    break
                                except storage.IntegrityError:
                                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                                    continue
                            self.redis.set('access_token:%s' % _access_token, user_id, D.EXPIRATION)
                        else:
                            self.write(dict(err=E.ERR_USER_REFRESH_TOKEN, msg=E.errmsg(E.ERR_USER_REFRESH_TOKEN)))
                            return
                    else:
                        self.write(dict(err=E.ERR_USER_TOKEN_EXPIRE, msg=E.errmsg(E.ERR_USER_TOKEN_EXPIRE)))
                        return
                else:
                    if self.has_arg("refresh_token"):
                        if self.arg("refresh_token") == _refresh_token:
                            _access_token = binascii.hexlify(os.urandom(20)).decode()
                            _refresh_token = binascii.hexlify(os.urandom(20)).decode()
                            query = "UPDATE core_user SET access_token=%s, refresh_token=%s, modified=%s WHERE id=%s"
                            params = (_access_token, _refresh_token, int(time.time()), user_id)
                            for i in range(5):
                                try:
                                    yield self.sql.runOperation(query, params)
                                    break
                                except storage.IntegrityError:
                                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                                    continue
                            self.redis.set('access_token:%s' % _access_token, user_id, D.EXPIRATION)
                        else:
                            self.write(dict(err=E.ERR_USER_REFRESH_TOKEN, msg=E.errmsg(E.ERR_USER_REFRESH_TOKEN)))
                            return
                    else:
                        pass

                self.write(dict(user_id=user_id, access_token=_access_token, refresh_token=_refresh_token))
                return

            else:
                self.write(dict(err=E.ERR_USER_TOKEN, msg=E.errmsg(E.ERR_USER_TOKEN)))
                return
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
