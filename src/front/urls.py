import cyclone.web
import api_doc
from twisted.internet.tcp import _AbortingMixin
from wiapi import api_manager
from front.handlers import user
from front.handlers import gate
from front.debug import gm_tool
from front.handlers import battle
from front.handlers import prop
# from front.handlers import stage

# from front.handlers import formation
# from front.handlers import hero
# from front.handlers import arena
# # from front.debug import gmprod
# # from front.debug import gmaccount
# # from front.debug import gmhero
# from operate import operate
from local_settings import DEBUG

url_patterns = [
    # (r'/', home.HomeHandler),
    # (r'/active/', home.ActiveHandler),
    # (r'/startup/', home.StartupHandler),
    # (r'/sync/', home.SyncHandler),
    # (r'/syncdb/', home.SyncdbHandler),
    # (r'/flushdb/', home.FlushdbHandler),

    # (r'/notify/', pay.NotifyHandler),
    # (r'/verify/', pay.VerifyHandler),
    # gm tools
    # (r'/gm/account/edit/', gmaccount.EditHandler),
    # (r'/gm/prod/edit/', gmprod.EditProdHandler),

    # (r'/crossdomain\.xml', home.CrossdomainHandler),
    (r'/crossdomain\.xml', cyclone.web.RedirectHandler,
     {'url': '/static/crossdmain.xml'}),
]
if DEBUG == True:
    apiurls = api_manager.get_urls() + url_patterns + [(r"/doc$", api_doc.ApiDocHandler),
                                                       (r"/map$", api_doc.ApiMapHandler), ]
else:
    apiurls = url_patterns
print apiurls
