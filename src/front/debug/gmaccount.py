import time
import zlib
from twisted.internet import defer
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
#from front.handlers.base import BaseHandler
from front.wiapi import *
from front.handlers.base import ApiHandler, ApiJSONEncoder

@handler
class EditHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Gm account edit', '/gm/account/edit/', [
        Param('gold', False, int, 1, 1, 'gold'),
        Param('xp', False, int, 1, 1, 'xp'),
        Param('feat', False, int, 1, 1, 'feat'),
        Param('rock', False, int, 1, 1, 'rock'),
        Param('hp', False, int, 1, 1, 'hp'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Gm account gold")
    def get(self):
        uid = self.uid
        user = yield self.get_user(uid)
        res = yield self.sql.runQuery("SELECT b.accountid FROM core_user as a, core_account as b WHERE a.id=b.user_id and a.id=%s "
                                      "LIMIT 1", (uid,))
        cuser = {}
        if self.get_argument('gold', None):
            user['gold'] = int(self.get_argument('gold'))
            cuser['gold']= user['gold']

        if self.get_argument('xp', None):
            user['xp'] = int(self.get_argument('xp'))
            cuser['xp']= user['xp']

        if self.get_argument('feat', None):
            user['feat'] = int(self.get_argument('feat'))
            cuser['feat']= user['feat']

        if self.get_argument('rock', None):
            user['rock'] = int(self.get_argument('rock'))
            cuser['rock']= user['rock']

        if self.get_argument('hp', None):
            user['hp'] = int(self.get_argument('hp'))
            cuser['hp']= user['hp']
        # yield self.redis.flushdb()
        yield self.set_user(uid, **cuser)

        if res:
            cuser['accountid'] = res[0][0]

        self.write(cuser)


class EditGold(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            gold = int(self.get_argument("gold", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        user['gold'] = gold
        cuser = dict(gold=user['gold'])
        yield self.set_user(uid, **cuser)
        msg = "SUCCESS! curr gold: "+str(gold)
        self.write(msg)

class EditXP(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            xp = int(self.get_argument("xp", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        user['xp'] = xp
        cuser = dict(xp=user['xp'])
        yield self.set_user(uid, **cuser)
        msg = "SUCCESS! curr xp: "+str(xp)
        self.write(msg)

class EditFeat(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            feat = int(self.get_argument("feat", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        user['feat'] = feat
        cuser = dict(feat=user['feat'])
        yield self.set_user(uid, **cuser)
        msg = "SUCCESS! curr feat: "+str(feat)
        self.write(msg)

class EditRock(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            rock = int(self.get_argument("rock", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        user['rock'] = rock
        cuser = dict(rock=user['rock'])
        yield self.set_user(uid, **cuser)
        msg = "SUCCESS! curr rock: "+str(rock)
        self.write(msg)

class AddHP(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            hp = int(self.get_argument("hp", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        #print 'hp:',hp
        uid = self.uid
        user = yield self.get_user(uid)
        currhp, tick = yield self.get_hp(user)
        currhp, tick = yield self.add_hp(user, hp)

        msg = "SUCCESS! curr hp: "+str(currhp)
        self.write(msg)

