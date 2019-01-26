# -*- coding: utf-8 -*-

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
class EditProdHandler(ApiHandler):

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Gm prod edit', '/gm/prod/edit/', [
        Param('pid', True, str, '01014', '01014', 'pid'),
        Param('nu', True, str, '1', '1', 'nu'),
        Param('_sign', True, str, '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '4GRwMApTJ3VpZCcKcDEKUycxMScKcDIKcyK', '_sign'),
        ], filters=[ps_filter], description="Prod equipcom")
    def get(self):
        try:
            pid = self.get_argument("pid")
            nu = int(self.get_argument("nu", 1))
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        curnu = user['prods'].get('pid', 0)
        if nu > 0:
            if pid in user['prods']:
                user['prods'][pid] = nu
            else:
                user['prods'][pid] = nu
        else:
            if pid in user['prods']:
                del user['prods'][pid]

        cuser = dict(prods=user['prods'])
        yield self.set_user(uid, **cuser)
        msg = "SUCCESS! pid: "+pid+" curr num:"+str(nu)
        self.write(msg)
        #self.redirect("/sync/?_sign=" + self.sign)

    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    @api('Hp add', '/hp/add/', [
        Param('channel', False, str, 'putaogame', 'putaogame', 'channel'),
        Param('hp', False, int, 1, 1, 'hp'),
        Param('_sign', True, str, 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', 'gGRwMApTJ3VpZCcKcDEKUyc5JwpwMgpzLK', '_sign'),
        ], filters=[ps_filter], description="Hp add")
    def post(self):
        uid = self.uid
        user = yield self.get_user(uid)
        currhp, tick = yield self.get_hp(user)
        currhp, tick = yield self.add_hp(user, int(self.arg('hp')))
        hp = currhp
        ret = dict(hp=hp, timestamp=int(time.time()))
        reb = zlib.compress(escape.json_encode(ret))
        self.write(ret)

