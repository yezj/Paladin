import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from local_settings import *  # @UnusedWildImport


settings = {}

settings['debug'] = DEBUG
settings['gzip'] = True
settings['url'] = FRONT_URL
settings["xheaders"] = True
settings['template_path'] = os.path.join(
    os.path.dirname(__file__), "templates")
settings['static_path'] = os.path.join(os.path.dirname(__file__), "static")
settings['static_url'] = settings['url'] + '/static/'
settings['configure_path'] = os.path.join(os.path.dirname(__file__), "../media")
settings['cookie_secret'] = "40)v#7)-5nlj7)ml5hdtel)h(nh_7lzc@dg*p364&amp;enrn^56_u7"
settings['cookie_domain'] = TOPDOMAIN
settings['login_url'] = '/login'
settings['xsrf_cookies'] = False

settings['sql'] = dict(
    host=DATA_HOST,
    username=DATA_USER,
    password=DATA_PASSWORD,
    database=DATA_NAME
)

settings['psql'] = dict(
    phost=PDATA_HOST,
    pusername=PDATA_USER,
    ppassword=PDATA_PASSWORD,
    pdatabase=PDATA_NAME
)

settings['redis'] = dict(
    host=DATA_HOST,
    dbid=DATA_DBID
)

settings['predis'] = dict(
    host=PUB_DATA_HOST,
    dbid=PUB_DATA_DBID
)

settings['email'] = dict(
    host=EMAIL_HOST,
    port=EMAIL_PORT,
    tls=EMAIL_TLS,
    username=EMAIL_USER,
    password=EMAIL_PASSWORD
)

settings['zoneid'] = ZONE_ID
settings['domain'] = DOMAIN
settings['timepoch'] = 1420041600  # 2015/1/1
