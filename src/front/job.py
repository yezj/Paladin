# coding: utf-8

import logging
logging.basicConfig()
import requests
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.twisted import TwistedScheduler
from twisted.python import log


class Jobs(object):

    @classmethod
    def setup(cls, conf):
        jobstores = {
            'default': SQLAlchemyJobStore(url=conf['job'])
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }

        scheduler = TwistedScheduler()
        scheduler.configure(jobstores=jobstores, job_defaults=job_defaults)

        scheduler.add_job(cls.syncdb, 'cron', args=[conf],  id="syncdb", replace_existing=True,
                          hour="16", minute="47")

        scheduler.start()

    @classmethod
    def syncdb(cls, conf):
        r = requests.post("/".join([conf['url'], "syncdb/"]))
        if r.status_code == requests.codes.ok:
            log.msg("Job syncdb done")
        else:
            log.msg("Job syncdb error")

    @classmethod
    @defer.inlineCallbacks
    def worship_job(cls):
        worshipers = []
        res = yield cls.sql.runQuery("SELECT a.id, a.nickname, a.avat, a.xp, b.now_rank FROM core_user AS a,\
         core_arena AS b WHERE a.id=b.user_id AND b.now_rank<%s" % D.WORSHIPRANK)
        if res:
            for r in res:
                worshipers.append(dict(uid=r[0], nickname=r[1], avat=r[2], xp=r[3], now_rank=r[4]))

        yield cls.predis.set('worshipers:%s' % ZONE_ID, pickle.dumps(worshipers))

