# coding: utf-8

import sys
import cyclone.web

from front.urls import apiurls
# from front.urls import url_patterns
from front.settings import settings
from front.storage import DatabaseMixin
from twisted.python import log


handlers = [(u[0], u[1]) for u in apiurls]
class Application(cyclone.web.Application):
    def __init__(self):

        # Set up database connections
        DatabaseMixin.setup(settings)

        cyclone.web.Application.__init__(self, handlers, **settings)

def main():
    from twisted.internet import reactor

    log.startLogging(sys.stdout)
    reactor.listenTCP(8888, Application())
    reactor.run()

if __name__ == "__main__":
    main()

