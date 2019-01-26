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
class TestHandler(ApiHandler):

    @storage.databaseSafe
    #@defer.inlineCallbacks
    @api('Operate test', '/operate/test/', [
        Param('formation', True, str, '[]', '[]', 'formation'),
        ], filters=[ps_filter], description="Operate test")
    def get(self):
        formation = self.get_argument('formation', None)
        if formation:
            formation = escape.json_decode(formation)
            self.write(formation)
        else:
            self.write(dict(err=E.ERR_ARGUMENT, msg=E.errmsg(E.ERR_ARGUMENT)))
            return
