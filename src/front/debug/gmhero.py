import time
import zlib
from twisted.internet import defer
from cyclone import escape, web
from front import storage
from front import utils
from front.utils import E
from front.handlers.base import BaseHandler

class InitHandler(BaseHandler):
    @storage.databaseSafe
    @defer.inlineCallbacks
    @utils.signed
    def get(self):
        try:
            hid = self.get_argument("hid")
        except Exception:
            raise web.HTTPError(400, "Argument error")

        uid = self.uid
        user = yield self.get_user(uid)
        try:
            hero = user['heros'][hid]
        except Exception:
            self.write(dict(err=E.ERR_INVALID, msg=E.errmsg(E.ERR_INVALID)))
            return

        hero['color'] = 0;
        hero['skills'] = [0, 0, 0, 0];
        hero['xp'] = 100000;
        hero['star'] = 0;
        hero['equips'] = [0, 0, 0, 0, 0, 0];

        cuser = dict(heros=user['heros'])
        yield self.set_user(uid, **cuser)

        msg = "SUCCESS! inited hid: "+hid
        self.write(msg)
