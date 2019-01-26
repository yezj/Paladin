# coding: utf-8

import collections
import functools
import pickle
import cyclone.redis
from cyclone import web, escape
from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.python import log
from apscheduler.schedulers.twisted import TwistedScheduler
import psycopg2
import datetime
import time
import D
import django_rq
import requests
from local_settings import *
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

IntegrityError = psycopg2.IntegrityError


def databaseSafe(method):
    """This decorator function makes all database calls safe from connection
     errors. It returns an HTTP 503 when either redis or sql are temporarily
     disconnected.

     @databaseSafe
     def get(self):
        now = yield self.sql.runQuery("select now()")
        print now
    """
    @defer.inlineCallbacks
    @functools.wraps(method)
    def run(self, *args, **kwargs):
        try:
            r = yield defer.maybeDeferred(method, self, *args, **kwargs)
        except cyclone.redis.ConnectionError, e:
            m = "redis.Error: %s" % e
            log.msg(m)
            raise cyclone.web.HTTPError(503, m)  # Service Unavailable
        except (psycopg2.InterfaceError, psycopg2.OperationalError), e:
            m = "sql.Error: %s" % e
            log.msg(m)
            raise cyclone.web.HTTPError(503, m)  # Service Unavailable
        else:
            defer.returnValue(r)

    return run


class DatabaseMixin(object):
    sql = None
    redis = None
    predis = None
    pubsub = None
    sched = None
    channels = collections.defaultdict(lambda: [])

    @classmethod
    def setup(cls, conf):
        if "sql" in conf:
            DatabaseMixin.sql = \
            adbapi.ConnectionPool("psycopg2",
                                  host=conf["sql"]['host'],
                                  database=conf["sql"]['database'],
                                  user=conf["sql"]['username'],
                                  password=conf["sql"]['password'],
                                  cp_min=1,
                                  cp_max=10,
                                  cp_reconnect=True,
                                  cp_noisy=conf["debug"])

        if "sql" in conf:
            DatabaseMixin.psql = \
                adbapi.ConnectionPool("psycopg2",
                                      host=conf["psql"]['phost'],
                                      database=conf["psql"]['pdatabase'],
                                      user=conf["psql"]['pusername'],
                                      password=conf["psql"]['ppassword'],
                                      cp_min=1,
                                      cp_max=10,
                                      cp_reconnect=True,
                                      cp_noisy=conf["debug"])

        if "redis" in conf:
            DatabaseMixin.redis = \
            cyclone.redis.lazyConnectionPool(
                          host=conf["redis"]['host'],
                          dbid=conf["redis"]['dbid'],
                          poolsize=10,
                          reconnect=True)

            if conf["redis"].get("pubsub", False):
                pubsub = cyclone.redis.SubscriberFactory()
                pubsub.maxDelay = 20
                pubsub.continueTrying = True
                pubsub.protocol = PubSubProtocol
                reactor.connectTCP(conf["redis"]['host'], 6379, pubsub)

        if "predis" in conf:
            DatabaseMixin.predis = \
            cyclone.redis.lazyConnectionPool(
                          host=conf["predis"]['host'],
                          dbid=conf["predis"]['dbid'],
                          poolsize=10,
                          reconnect=True)
            
            if conf["predis"].get("pubsub", False):
                pubsub = cyclone.redis.SubscriberFactory()
                pubsub.maxDelay = 20
                pubsub.continueTrying = True
                pubsub.protocol = PubSubProtocol
                reactor.connectTCP(conf["predis"]['host'], 6379, pubsub)

        DatabaseMixin.sched = TwistedScheduler(conf)
        DatabaseMixin.build(conf)
        DatabaseMixin.sched.start()


    @classmethod
    def build(cls, conf):
        sched = cls.sched
        sched.add_job(cls.day_job, 'cron', hour='5', minute='0', second='0')
        sched.add_job(cls.clean_job, 'cron', hour='0', minute='0', second='0')
        sched.add_job(cls.worship_job, 'cron', hour='21', minute='0', second='0')
        #sched.add_job(cls.arena_job_mutex, 'cron', hour='21', minute='30', second='0')
        #sched.add_job(cls.zone_job, 'cron', hour='0-23', minute='0', second='0', kwargs={'zoneid':ZONE_ID})
        sched.add_job(cls.interval_job, 'interval', minutes=1, kwargs={'zoneid':conf['zoneid']})
        sched.add_job(cls.monday_job, 'cron', month='1-12', day='1', hour='4', minute='50', second='0')
        #sched.add_job(cls.monday_job, 'cron', month='1-12', day='10', hour='16', minute='50', second='0')

    @classmethod
    @defer.inlineCallbacks
    def worship_job(cls):
        worshipers = []
        res = yield cls.sql.runQuery("SELECT a.id, a.nickname, a.avat, a.xp, b.now_rank FROM core_user AS a,\
         core_arena AS b WHERE a.id=b.user_id AND b.now_rank<%s" % D.WORSHIPRANK)
        if res:
            for r in res:
                worshipers.append(dict(uid=r[0],
                                       nickname=r[1],
                                       avat=r[2],
                                       xp=r[3],
                                       now_rank=r[4])
                                  )

        yield cls.predis.set('worshipers:%s' % ZONE_ID, pickle.dumps(worshipers))

    @classmethod
    def syncdb(cls, url):
        requests.post("/".join([FRONT_URL, url]))

    @classmethod
    @defer.inlineCallbacks
    def clean_job(cls):
        timestamp = int(time.mktime(datetime.datetime.now().timetuple())) - 24*3600*3
        yield cls.sql.runQuery("DELETE FROM core_arenaresult WHERE timestamp<=%s RETURNING id" % timestamp)

    @classmethod
    @defer.inlineCallbacks
    def day_job(cls):
        queue = django_rq.get_queue('high')
        queue.enqueue(cls.syncdb, "syncdb/")

    @classmethod
    @defer.inlineCallbacks
    def arena_job(cls):
        mutex = yield cls.redis.get('mutex')
        if not mutex:
            yield cls.redis.set('mutex', 1)
            res = yield cls.sql.runQuery("SELECT b.user_id, b.now_rank FROM core_user AS a,\
              core_arena AS b WHERE a.id=b.user_id AND b.now_rank<%s AND a.xp/100000>=%s" % D.ARENA_MAIL_LIMIT)
            for r in res:
                awards = {}
                for i in xrange(0, len(D.ARENADAY)/6):
                    if r[1] >= D.ARENADAY[i*6] and r[1] <= D.ARENADAY[i*6+1]:
                        awards = dict(rock=D.ARENADAY[i*6+2],
                                      gold=D.ARENADAY[i*6+3],
                                      arena_coin=D.ARENADAY[i*6+4],
                                      feat=D.ARENADAY[i*6+5])
                        break

                if awards:
                    mail = D.TOPMAIL
                    content = mail['content'] % r[1]
                    mail = dict(sender=mail['sender'],
                                title=mail['title'],
                                content=content,
                                jawards=awards)
                    cls.send_mail([r[0]], mail)

    @classmethod
    @defer.inlineCallbacks
    def arena_job_mutex(cls):
        yield cls.redis.set('mutex', 0)

    @classmethod
    @defer.inlineCallbacks
    def zone_job(cls, zoneid):
        res = yield cls.sql.runQuery("SELECT count(*) FROM core_user")
        if res:
            num, = res[0]
            yield cls.predis.set('zone:%s:%s' % (str(zoneid), datetime.datetime.now().strftime('%Y%m%d')), num)
        else:
            yield cls.predis.set('zone:%s:%s' % (str(zoneid), datetime.datetime.now().strftime('%Y%m%d')), 0)

    @classmethod
    @defer.inlineCallbacks
    def interval_job(cls, zoneid):
        maill = yield cls.predis.llen('mail:%s' % zoneid)
        for i in xrange(0, maill):
            mail = yield cls.predis.rpop('mail:%s' % zoneid)
            if mail:
                mail = pickle.loads(mail)
                if mail.get('to', None):
                    res = yield cls.sql.runQuery("SELECT a.id FROM core_user as a,\
                      core_account as b WHERE a.id=b.user_id and b.accountid=%s LIMIT 1", (mail.get('to', None), ))
                else:
                    res = yield cls.sql.runQuery("SELECT id FROM core_user")
                cls.send_mail([r[0] for r in res], mail)
            
    @classmethod
    @defer.inlineCallbacks
    def monday_job(cls):
        query = "UPDATE core_user SET jseals=%s RETURNING id"
        params = (escape.json_encode([0, 1]), )
        for i in range(5):
            try:
                yield cls.sql.runQuery(query, params)
                break
            except IntegrityError:
                log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                continue

    @classmethod
    @defer.inlineCallbacks
    def send_mail(cls, iids, mail):
        for iid in iids:
            query = "INSERT INTO core_mail(sender, to_id, title, content, jawards, comment, created_at,\
              type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
            sender = mail.get('sender', u'管理员')
            title = mail.get('title', '')
            content = mail.get('content', '') 
            awards = mail.get('jawards', {})
            mtype = mail.get('type', 0)
            params = (sender, iid, title, content, escape.json_encode(awards), '', datetime.datetime.now(), mtype)
            #print query % params
            for i in range(5):
                try:
                    yield cls.sql.runQuery(query, params)
                    break
                except IntegrityError:
                    log.msg("SQL integrity error, retry(%i): %s" % (i, (query % params)))
                    continue

    def subscribe(self, channel):
        if not DatabaseMixin.pubsub:
            raise cyclone.web.HTTPError(503, "Pubsub not available")
        if channel not in DatabaseMixin.channels:
            log.msg("Subscribing entire server to %s" % channel)
            if "*" in channel:
                DatabaseMixin.pubsub.psubscribe(channel)
            else:
                DatabaseMixin.pubsub.subscribe(channel)
        DatabaseMixin.channels[channel].append(self)
        log.msg("Client %s subscribed to %s" %
                (hasattr(self, 'request') and self.request.remote_ip or '*', channel))

    def unsubscribe(self, channel):
        peers = DatabaseMixin.channels.get(channel, [])
        if peers:
            try:
                peers.pop(peers.index(self))
                log.msg("Client %s unsubscribed from %s" %
                        (hasattr(self, 'request') and self.request.remote_ip or '*', channel))
            except Exception:
                return
        if not len(peers) and DatabaseMixin.pubsub:
            log.msg("Unsubscribing entire server from %s" % channel)
            if "*" in channel:
                DatabaseMixin.pubsub.punsubscribe(channel)
            else:
                DatabaseMixin.pubsub.unsubscribe(channel)
            try:
                del DatabaseMixin.channels[channel]
            except Exception:
                pass

    def unsubscribe_all(self):
        # Unsubscribe peer from all channels
        for channel, peers in DatabaseMixin.channels.items():
            try:
                peers.pop(peers.index(self))
                log.msg("Client %s unsubscribed from %s" %
                        (hasattr(self, 'request') and self.request.remote_ip or '*', channel))
            except Exception:
                continue
            # Unsubscribe from channel if no peers are listening
            if not len(peers) and DatabaseMixin.pubsub:
                log.msg("Unsubscribing entire server from %s" % channel)
                if "*" in channel:
                    DatabaseMixin.pubsub.punsubscribe(channel)
                else:
                    DatabaseMixin.pubsub.unsubscribe(channel)
                try:
                    del DatabaseMixin.channels[channel]
                except Exception:
                    pass

    def broadcast(self, pattern, channel, message):
        peers = DatabaseMixin.channels.get(pattern or channel)
        if not peers:
            return
        # Broadcast the message to all peers in channel
        for peer in peers:
            peer.pubsubReceived(pattern or channel, message)


class PubSubProtocol(cyclone.redis.SubscriberProtocol, DatabaseMixin):
    def messageReceived(self, pattern, channel, message):
        # When new messages are published to Redis channels or patterns,
        # they are broadcasted to all HTTP clients subscribed to those
        # channels.
        DatabaseMixin.broadcast(self, pattern, channel, message)

    def connectionMade(self):
        DatabaseMixin.pubsub = self
        # If we lost connection with Redis during operation, we
        # re-subscribe to all channels once the connection is re-established.
        for channel in DatabaseMixin.channels:
            if "*" in channel:
                self.psubscribe(channel)
            else:
                self.subscribe(channel)

    def connectionLost(self, why):
        DatabaseMixin.pubsub = None
