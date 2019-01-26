import requests
from local_settings import *

def cron_job(url, data):
    requests.post("/".join([FRONT_URL, url]), data)
