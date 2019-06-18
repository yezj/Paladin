# coding: utf-8

import base64
import datetime
import functools
import pickle
import time
import uuid
from cyclone import web
import D
import random
# import requests
from local_settings import BASE_URL


def token(method):
    @functools.wraps(method)
    def wraps(self, *args, **kwargs):
        access_token = str(self.get_argument("access_token", None))
        user_id = self.get_argument("user_id", None)
        if not access_token:
            raise web.HTTPError(400, "Access_token required")
        else:
            try:
                self.token = pickle.loads(base64.urlsafe_b64decode(access_token))
                self.user_id = self.token['user_id']
                if self.user_id != user_id:
                    raise web.HTTPError(400, "Access_token error")
            except Exception as e:
                print e.message
                raise web.HTTPError(400, "Access_token invalid")
        return method(self, *args, **kwargs)

    return wraps


class E(object):
    class UNKNOWNERROR(Exception):
        pass

    class INVALIDERROR(Exception):
        pass

    class SERVERERROR(Exception):
        pass

    class CLIENTERROR(Exception):
        pass

    class USERNOTFOUND(Exception):
        pass

    class USERABNORMAL(Exception):
        pass

    class USERBEBANKED(Exception):
        pass

    class VALUEINVALID(Exception):
        pass

    class HPNOTENOUGH(Exception):
        pass

    class GOLDNOTENOUGH(Exception):
        pass

    class ROCKNOTENOUGH(Exception):
        pass

    class FEATNOTENOUGH(Exception):
        pass

    class BOOkNOTENOUGH(Exception):
        pass

    class XPNOTENOUGH(Exception):
        pass

    class HXPNOTENOUGH(Exception):
        pass

    class PRODNOTENOUGH(Exception):
        pass

    class MAXSTARREADYGOT(Exception):
        pass

    class MAXLEVELREADYGOT(Exception):
        pass

    class MAXSKILLREADYGOT(Exception):
        pass

    class OPENTERMDISSATISFY(Exception):
        pass

    class MAXENTRYDISSATISFY(Exception):
        pass

    class MAXBUYTIMESDISSATISFY(Exception):
        pass

    class COLDDOWNDISSATISFY(Exception):
        pass

    class SEALALREADYGOT(Exception):
        pass

    class TASKNOTFOUND(Exception):
        pass

    class TASKDISSATISFY(Exception):
        pass

    class TASKALREADYGOT(Exception):
        pass

    class WORKNOTFOUND(Exception):
        pass

    class WORKDISSATISFY(Exception):
        pass

    class WORKALREADYGOT(Exception):
        pass

    class ARENACOINNOTENOUGH(Exception):
        pass

    class HEROBUSY(Exception):
        pass

    ERR_UNKNOWN = 0
    ERR_INVALID = 1
    ERR_SERVER = 2
    ERR_CLIENT = 3
    ERR_SYNC = 4
    ERR_REPEAT = 5
    ERR_ARGUMENT = 6
    ERR_CHANNEL = 7
    ERR_USER_NOTFOUND = 101
    ERR_USER_ABNORMAL = 102
    ERR_USER_BEBANKED = 103
    ERR_USER_REPEAT = 104
    ERR_USER_CREATED = 105
    ERR_USER_PASSWORD = 106
    ERR_USER_TOKEN_EXPIRE = 107
    ERR_USER_TOKEN = 108
    ERR_USER_REFRESH_TOKEN = 109
    # ERR_USER_CREATED = 105
    ERR_NOTENOUGH_HP = 201
    ERR_NOTENOUGH_GOLD = 202
    ERR_NOTENOUGH_ROCK = 203
    ERR_NOTENOUGH_FEAT = 204
    ERR_NOTENOUGH_BOOK = 205
    ERR_NOTENOUGH_XP = 206
    ERR_NOTENOUGH_HXP = 207
    ERR_NOTENOUGH_PROD = 208
    ERR_NOTENOUGH_SP = 209
    ERR_DISSATISFY_PLAYTERM = 301
    ERR_DISSATISFY_MAXENTRY = 302
    ERR_DISSATISFY_COLDDOWN = 303
    ERR_DISSATISFY_MAXBUYTIMES = 304
    ERR_DISSATISFY_ENTRYTIME = 305

    ERR_SEAL_ALREADYGOT = 401
    ERR_MAXSTAR_ALREADYGOT = 402
    ERR_MAXLEVEL_ALREADYGOT = 402
    ERR_MAXSKILL_ALREADYGOT = 403
    ERR_WORK_NOTFOUND = 601
    ERR_WORK_ALREADYGOT = 602
    ERR_WORK_DISSATISFY = 603
    ERR_MAIL_NOTFOUND = 701
    ERR_MAIL_ALREADYGOT = 702
    ERR_NOTENOUGH_ARENACOIN = 801
    ERR_DISSATISFY_MAXREFRESHES = 802
    ERR_DISSATISFY_MAXRESETS = 803
    ERR_DUPLICATE_BUY = 804
    ERR_PRISION_FULL = 901

    ERR_TASK_NOTFOUND = 1001
    ERR_TASK_ALREADYGOT = 1002
    ERR_TASK_DISSATISFY = 1003
    ERR_HERO_BUSY = 1101
    ERR_MAXMINE_ALREADYGOT = 1102
    ERR_DISSATISFY_HARVEST = 1103
    ERR_MINE_ALREADYGOT = 1104
    ERR_HARVEST_ALREADYGOT = 1105

    ERR_DISSATISFY_MAXWORSHIPS = 1201
    ERR_BEAUTY_ALREADYGOT = 1301
    ERR_SIGN = 1401

    ERR_ACTIVITY_NOTFOUND = 1501
    ERR_ACTIVITY_ALREADYGOT = 1502
    ERR_ACTIVITY_DISSATISFY = 1503
    ERR_ACTIVITY_END = 1504

    ERR_CODE_NOTFOUND = 1601
    ERR_CODE_TIMEOUT = 1602
    ERR_CODE_ALREADYGOT = 1603
    ERR_CODE_REPEAT = 1604

    ERR_BLACKMARET_NOTFOUND = 1701
    ERR_MARET_NOTFOUND = 1702

    ERR_SECKILL_NOTFOUND = 1801
    ERR_SECKILL_OVER = 1802
    ERR_SECKILL_END = 1803
    ERR_SECKILL_REPEAT = 1804

    ERR_VIPPACKAGE_NOTFOUND = 1901
    ERR_VIPPACKAGE_ALREADYGOT = 1902
    ERR_VIPPACKAGE_DISSATISFY = 1903
    ERR_VIPPACKAGE_CLOSED = 1904

    ERR_NOTENOUGH_EXPEDCOIN = 2001
    ERR_MAXEXCHANGETIMES_ALREADYGOT = 3001

    ERR_NOTENOUGH_VERSUSCOIN = 4001
    ERR_VERSUSSTATE_ALREADY = 4002
    ERR_VERSUSSTATE_NOTFOUND = 4003

    ERR_CREATE_GUILD_REPEAT = 5000
    ERR_JOIN_GUILD_REPEAT = 5001
    ERR_QUIT_GUILD_REPEAT = 5002
    ERR_NOTEXIST_GUILD_USER = 5003

    ERR_NOTENOUGH_PROP = 10001
    ERR_NOTENOUGH_PROP_EXPIRED = 10002
    _errmsg = {
        ERR_UNKNOWN: u'未知错误',
        ERR_INVALID: u'非法请求',
        ERR_SERVER: u'服务器正在维护',
        ERR_CLIENT: u'客户端发生异常',
        ERR_SYNC: u'数据同步错误',
        ERR_REPEAT: u'重复操作',
        ERR_ARGUMENT: u'参数错误',
        ERR_CHANNEL: u'渠道错误',
        ERR_USER_CREATED: u'创建用户失败',
        ERR_USER_NOTFOUND: u'用户不存在',
        ERR_USER_ABNORMAL: u'账户发生异常',
        ERR_USER_BEBANKED: u'账户已被封',
        ERR_USER_REPEAT: u'用户名重复',
        ERR_USER_PASSWORD: u'用户名或密码错误',
        ERR_USER_TOKEN_EXPIRE: u'Access_token过期,请重新登录',
        ERR_USER_TOKEN: u'Access_token错误',
        ERR_USER_REFRESH_TOKEN: u'Refresh_token错误',
        ERR_NOTENOUGH_PROP: u'道具不足',
        ERR_NOTENOUGH_PROP_EXPIRED: u'道具已过期',
        ERR_NOTENOUGH_HP: u'体力不足',
        ERR_NOTENOUGH_GOLD: u'金币不足',
        ERR_NOTENOUGH_ROCK: u'钻石不足',
        ERR_NOTENOUGH_FEAT: u'功勋不足',
        ERR_NOTENOUGH_BOOK: u'计谋不足',
        ERR_NOTENOUGH_PROD: u'物品不足',
        ERR_NOTENOUGH_XP: u'战队等级不足',
        ERR_NOTENOUGH_HXP: u'英雄等级不足',
        ERR_NOTENOUGH_SP: u'技能点不足',
        ERR_DISSATISFY_PLAYTERM: u'进入条件不满足',
        ERR_DISSATISFY_MAXENTRY: u'最大进入次数已达到',
        ERR_DISSATISFY_COLDDOWN: u'进入冷却时间未达到',
        ERR_DISSATISFY_MAXBUYTIMES: u'购买次数已经达到上限',
        ERR_DISSATISFY_ENTRYTIME: u'进入时间未满足',
        ERR_SEAL_ALREADYGOT: u'签到已领取',
        ERR_MAXSTAR_ALREADYGOT: u'您已达到最大星级',
        ERR_MAXLEVEL_ALREADYGOT: u'您已达到最大等级',
        ERR_MAXSKILL_ALREADYGOT: u'您的技能已达到最大等级',
        ERR_TASK_NOTFOUND: u'任务不存在',
        ERR_TASK_ALREADYGOT: u'任务奖励已领取',
        ERR_TASK_DISSATISFY: u'任务奖励领取条件不满足',
        ERR_WORK_NOTFOUND: u'当前活动不存在',
        ERR_WORK_ALREADYGOT: u'每日奖励已领取',
        ERR_WORK_DISSATISFY: u'每日奖励领取条件不满足',
        ERR_MAIL_NOTFOUND: u'邮件不存在',
        ERR_MAIL_ALREADYGOT: u'邮件已领取',
        ERR_NOTENOUGH_ARENACOIN: u'巅峰币不足',
        ERR_DISSATISFY_MAXREFRESHES: u'最大刷新次数已达到',
        ERR_DISSATISFY_MAXRESETS: u'最大重置次数已达到',

        ERR_DUPLICATE_BUY: u'重复购买物品',
        ERR_PRISION_FULL: u'监狱已满',
        ERR_HERO_BUSY: u'英雄正在开采中',
        ERR_MAXMINE_ALREADYGOT: u'最大开采数量已达到',
        ERR_DISSATISFY_HARVEST: u'收获时间未达到',
        ERR_MINE_ALREADYGOT: u'已收获或不存在',
        ERR_HARVEST_ALREADYGOT: u'开采已结束',

        ERR_DISSATISFY_MAXWORSHIPS: u'最大膜拜次数已达到',

        ERR_BEAUTY_ALREADYGOT: u'美人已在帐中',
        ERR_SIGN: u'签名错误',

        ERR_ACTIVITY_NOTFOUND: u'活动不存在',
        ERR_ACTIVITY_ALREADYGOT: u'活动奖励已领取',
        ERR_ACTIVITY_DISSATISFY: u'活动奖励领取条件不满足',
        ERR_ACTIVITY_END: u'活动截止',
        ERR_CODE_NOTFOUND: u'兑换码不存在',
        ERR_CODE_TIMEOUT: u'兑换码已经失效',
        ERR_CODE_ALREADYGOT: u'兑换码已兑换',
        ERR_CODE_REPEAT: u'兑换码不能重复领取',

        ERR_BLACKMARET_NOTFOUND: u'黑市已关闭',
        ERR_MARET_NOTFOUND: u'集市已关闭',

        ERR_SECKILL_REPEAT: u'仅能抢购一次哦',
        ERR_SECKILL_NOTFOUND: u'物品不存在或活动时间已结束',
        ERR_SECKILL_OVER: u'已秒杀',
        ERR_SECKILL_END: u'秒杀结束',

        ERR_VIPPACKAGE_NOTFOUND: u'VIP礼包不存在',
        ERR_VIPPACKAGE_ALREADYGOT: u'VIP礼包已领取',
        ERR_VIPPACKAGE_DISSATISFY: u'VIP礼包领取等级不满足',
        ERR_VIPPACKAGE_CLOSED: u'VIP礼包暂未开放',

        ERR_NOTENOUGH_EXPEDCOIN: u' 五关币不足',
        ERR_MAXEXCHANGETIMES_ALREADYGOT: u'最大兑换次数已达到',
        ERR_NOTENOUGH_VERSUSCOIN: u'群雄争霸币不足',
        ERR_VERSUSSTATE_ALREADY: u'您已经在一个州了，不能重复加入',
        ERR_VERSUSSTATE_NOTFOUND: u'您不在当前州',

        ERR_CREATE_GUILD_REPEAT: u'每个账号仅能创建一个公会',
        ERR_JOIN_GUILD_REPEAT: u'不能重复加入工会',
        ERR_QUIT_GUILD_REPEAT: u'不能重复退出工会',
        ERR_NOTEXIST_GUILD_USER: u'该工会不存在此玩家',
    }

    @staticmethod
    def errmsg(code):
        try:
            return E._errmsg[code]
        except KeyError:
            return E._errmsg[0]

    @staticmethod
    def initdata4user():
        user = D.USERINIT
        user['secret'] = ''
        user['name'] = uuid.uuid4().hex[:8]
        user['nickname'] = u'刘健'
        user['timestamp'] = int(time.time())
        return user

    @staticmethod
    def cost4skill(hero, skill, pt):
        try:
            cost = D.HEROSKILL[skill][pt]
        except LookupError:
            cost = {
                "xp": 10000000,
                "gold": 10000000,
                "prods": {}
            }
        return cost

    @staticmethod
    def cost4level(hero, level):
        return {
            "feat": D.LEVELFEAT[level * 2],
            "hxp": D.LEVELFEAT[level * 2 + 1],
        }

    @staticmethod
    def cost4color(hero, color):
        try:
            cost = D.HEROCOLOR[color]
        except LookupError:
            cost = {
                "gold": 10000000
            }
        return cost

    @staticmethod
    def cost4star(hero, star):
        try:
            cost = D.HEROSTAR[hero['hid']][hero['star']]
            # cost = D.HEROSTAR[hero['star']]

        except LookupError:
            cost = {
                "gold": 10000000,
                "prods": {}
            }
        return cost

    @staticmethod
    def cost4recruit(hid):
        try:
            cost = D.HERORECRUIT[hid][0]
        except LookupError:
            cost = {
                "gold": 10000000,
                "num": 999
            }
        return cost

    @staticmethod
    def equips4hero(hero):
        try:
            equips = D.HEROEQUIP[hero['hid']][hero['color']]
        except LookupError:
            equips = ('01001', '01002', '01003', '01004', '01005', '01006')
        return equips

    @staticmethod
    def cost4equipcom(prod):
        try:
            cost = D.PRODEQUIPCOM[prod]
        except LookupError:
            cost = {
                "gold": 10000000,
                "prods": {}
            }
        return cost

    @staticmethod
    def cost4sellout(prod):
        try:
            cost = D.PRODSELLOUT[prod]
        except LookupError:
            cost = {
                "gold": 1,
            }
        return cost

    @staticmethod
    def drawseal(user):
        userseals = user['seals']
        if userseals:
            sealnu, sealday = userseals
        else:
            sealnu, sealday = 0, 0
        weekday = datetime.date.today().weekday()
        # today = datetime.date.today().day
        if sealnu != 0 and sealday - 1 == weekday:
            raise E.SEALALREADYGOT
        seal = D.SEALS[sealnu]
        awards = seal.get('awards')
        if awards:
            user['gold'] += awards.get('gold', 0)
            user['rock'] += awards.get('rock', 0)
            user['feat'] += awards.get('feat', 0)
            for prod, n in awards.get('prods', {}).items():
                if prod in user['prods']:
                    user['prods'][prod] += n
                else:
                    user['prods'][prod] = n
                if user['prods'][prod] > 9999:
                    user['prods'][prod] = 9999
                elif user['prods'][prod] == 0:
                    del user['prods'][prod]
                else:
                    pass
        userseals[0] += 1
        userseals[1] = weekday + 1

    @staticmethod
    def drawtask(user, tid):
        usertasks = user['tasks']
        if tid not in usertasks:
            raise E.TASKNOTFOUND
        if usertasks[tid]['_'] == 0:
            raise E.TASKDISSATISFY
        elif usertasks[tid]['_'] == 2:
            raise E.TASKALREADYGOT
        task = D.TASK[tid]
        usertask = usertasks[tid]
        awards = task.get('awards')
        if awards:
            user['gold'] += awards.get('gold', 0)
            user['rock'] += awards.get('rock', 0)
            user['feat'] += awards.get('feat', 0)
            for prod, n in awards.get('prods', {}).items():
                if prod in user['prods']:
                    user['prods'][prod] += n
                else:
                    user['prods'][prod] = n
                if user['prods'][prod] > 9999:
                    user['prods'][prod] = 9999
                elif user['prods'][prod] == 0:
                    del user['prods'][prod]
                else:
                    pass
        usertask['_'] = 2
        nexttids = task.get('revdep', [])
        nexttids.extend(task.get('open', []))
        try:
            cattids = D.TASKCAT[task['cat']]
            cattid = cattids[cattids.index(tid) + 1]
            cattask = D.TASK[cattid]
            cattaskdeptid = cattask.get('dep', None)
            if not cattaskdeptid:
                nexttids.append(cattid)
            else:
                if cattaskdeptid in usertasks and usertasks[cattaskdeptid]['_'] != 2:
                    nexttids.append(cattid)
                    # print 'nexttids', nexttids

        except Exception:
            pass
        # print 'nexttids', nexttids
        for t in nexttids:
            tt = D.TASK[t]
            progress = tt['progress'](user)
            # print 'progress', progress
            if usertasks.has_key(t):
                if usertasks[t]['_'] == 2:
                    continue
            if progress >= tt['tags']['*']:
                progress = tt['tags']['*']
                usertasks[t] = {'_': 1, 'tags': {'*': (progress, progress)}}
            else:
                usertasks[t] = {'_': 0, 'tags': {'*': (tt['tags']['*'], progress)}}
                # print usertasks

    @staticmethod
    def pushtasks(user):
        changed = False
        usertasks = user['tasks']
        for tid, usertask in usertasks.iteritems():
            if usertask['_'] == 0:
                task = D.TASK[tid]
                progress = task['progress'](user)
                if progress != usertask['tags']['*'][1]:
                    changed = True
                    if progress >= task['tags']['*']:
                        progress = task['tags']['*']
                        usertask['_'] = 1
                    usertask['tags']['*'][1] = progress
        return changed

    @staticmethod
    def tagworks(user, tags):
        changed = False
        userworks = user['works']
        tagkeys = set(tags.keys())
        for wid, work in D.WORKS.iteritems():
            for tag, target in work['tags'].items():
                if tag in tagkeys:
                    if wid not in userworks:
                        _tags = {_tag: (_target, 0) for _tag, _target in work['tags'].items()}
                        userworks[wid] = {'_': 0, 'tags': _tags}
                    if userworks[wid]['_'] == 0:
                        lastprogress = userworks[wid]['tags'][tag][1]
                        progress = lastprogress + tags[tag]
                        if progress > target:
                            progress = target
                        if lastprogress != progress:
                            changed = True
                        userworks[wid]['tags'][tag] = (target, progress)
        for userwork in userworks.itervalues():
            if userwork['_'] == 0:
                if all([y >= x for x, y in userwork['tags'].values()]):
                    userwork['_'] = 1
        return changed

    @staticmethod
    def drawwork(user, wid):
        userworks = user['works']
        if wid not in userworks:
            raise E.WORKNOTFOUND
        if userworks[wid]['_'] == 0:
            raise E.WORKDISSATISFY
        elif userworks[wid]['_'] == 2:
            raise E.WORKALREADYGOT
        work = D.WORKS[wid]
        userwork = userworks[wid]
        awards = work.get('awards')
        if awards:
            user['gold'] += awards.get('gold', 0)
            user['rock'] += awards.get('rock', 0)
            user['feat'] += awards.get('feat', 0)
            for prod, n in awards.get('prods', {}).items():
                if prod in user['prods']:
                    user['prods'][prod] += n
                else:
                    user['prods'][prod] = n
                if user['prods'][prod] > 9999:
                    user['prods'][prod] = 9999
                elif user['prods'][prod] == 0:
                    del user['prods'][prod]
                else:
                    pass
        userwork['_'] = 2

    @staticmethod
    def checkworks(user):
        userworks = user['works']
        for wid, work in D.WORKS.iteritems():
            day = datetime.datetime.now().weekday() + 1
            if day not in work['opendate']:
                userworks[wid]['_'] = 2
        return userworks

    @staticmethod
    def checkmails(user):
        changed = False
        usermails = user['mails']
        for mid in usermails:
            if usermails[mid] == -1:
                usermails[mid] = 0
                changed = True
        return changed

    _entryids = [
        '9101', '9102',  # 英雄试炼，每场景再细分三关：010101, 010102, 010103
    ]

    @staticmethod
    def entryopens(user):
        opens = []
        for eid in E._entryids:
            match = E.match4entry(eid)
            label = match.get('label')
            term = match.get('playterm')
            if term:
                if term(user, label):
                    opens.append(eid)
            else:
                opens.append(eid)
        return opens

    @staticmethod
    def match4entry(eid):
        try:
            MATCH = dict(D.MATCH, **D.TRIAL)
            match = MATCH[eid]
        except KeyError:
            match = {
                'label': eid,
                'playterm': lambda user, label: True,
                'colddown': 1,
                'maxentry': 99,
                'hp': 6,
                'gold': 1,
                'rock': 0,
                'feat': 0,
                'xp': 1,
                'hxp': 1,
                'prods': [('01001', 0.01)],
            }
        return match

    @staticmethod
    def awards4firstbatt(bid):
        try:
            return D.FIRSTBATT[bid]
        except Exception:
            return {
                "feat": 0,
                "gold": 0,
                "rock": 0
            }

    @staticmethod
    def bornhero(user, hid):
        if hid not in user['heros']:
            hero = D.HERO[hid].copy()
            user['heros'][hid] = hero
        else:
            hero, = D.HERORECRUIT[hid]
            prods = hero['prods']
            for prod, n in prods.items():
                if prod in user['prods']:
                    user['prods'][prod] += n
                else:
                    user['prods'][prod] = n
                if user['prods'][prod] > 9999:
                    user['prods'][prod] = 9999
                elif user['prods'][prod] == 0:
                    del user['prods'][prod]
                else:
                    pass
            return None

    @staticmethod
    def addskills(user, hero, skills):
        heroskills = hero['skills']
        costs = {'gold': 0, 'prods': {}}
        for skill, nu in enumerate(skills):
            if nu:
                pt = heroskills[skill]
                if pt >= 99:
                    raise E.MAXSKILLREADYGOT
                for i in range(1, nu + 1):
                    cost = E.cost4skill(hero, skill, pt + i)
                    xp = cost.get('xp', 0)
                    # if user['xp'] < xp:
                    if hero['xp'] < xp:
                        raise E.XPNOTENOUGH
                    costs['gold'] += cost['gold']
                    for k, v in cost['prods'].items():
                        costs['prods'][k] = costs['prods'].get(k, 0) + v
        # print 'cost', costs['gold'], costs['prods']
        if user['gold'] < costs['gold']:
            raise E.GOLDNOTENOUGH
        for k, v in costs['prods'].items():
            if k not in user['prods'] or user['prods'][k] < v:
                raise E.PRODNOTENOUGH
        user['gold'] -= costs['gold']
        for k, v in costs['prods'].items():
            user['prods'][k] -= v
        for skill, nu in enumerate(skills):
            heroskills[skill] += nu

    @staticmethod
    def setequips(user, hero, equips):
        heroequips = hero['equips']
        userprods = user['prods']
        prodids = E.equips4hero(hero)
        for equip, has in enumerate(equips):
            if has > 0 and heroequips[equip] == 0:
                prodid = prodids[equip]
                if prodid in userprods and userprods[prodid] > 0:
                    heroequips[equip] = 100000
                    userprods[prodid] -= 1
                    if userprods[prodid] == 0:
                        del userprods[prodid]
                else:
                    raise E.PRODNOTENOUGH

    @staticmethod
    def steplevel(user, hero, feat):
        hxp = hero['xp']
        lv, lxp = divmod(hxp, 100000)
        if feat:
            cost = dict(hxp=feat, feat=feat)
        else:
            cost = E.cost4level(hero, lv + 1)
        if user['feat'] < cost['feat']:
            raise E.FEATNOTENOUGH
        hlvmit, hero['xp'] = E.normhxp(user, hxp, cost['hxp'])
        # print hlvmit, hero['xp']
        if not hlvmit:
            user['feat'] -= cost['feat']
            # hero['xp'] = E.normxp(user, hxp + cost['hxp'])

    @staticmethod
    def stepcolor(user, hero):
        if not all(hero['equips']):
            raise E.PRODNOTENOUGH
        cost = E.cost4color(hero, hero['color'] + 1)
        gold = cost['gold']
        if user['gold'] < gold:
            raise E.GOLDNOTENOUGH
        user['gold'] -= gold
        hero['color'] += 1
        hero['equips'] = [0, 0, 0, 0, 0, 0]

    max_star = 4

    @staticmethod
    def stepstar(user, hero):
        if hero['star'] + 1 > E.max_star:
            raise E.MAXSTARREADYGOT

        cost = E.cost4star(hero, hero['star'] + 1)
        gold = cost['gold']
        prods = cost['prods']
        if user['gold'] < gold:
            raise E.GOLDNOTENOUGH
        for k, v in prods.items():
            if k not in user['prods'] or user['prods'][k] < v:
                raise E.PRODNOTENOUGH
        user['gold'] -= gold
        for k, v in prods.items():
            user['prods'][k] -= v
        hero['star'] += 1

    @staticmethod
    def recruit(user, hid):
        cost = E.cost4recruit(hid)
        gold = cost['gold']
        prods = cost['prods']
        if user['gold'] < gold:
            raise E.GOLDNOTENOUGH
        for k, v in prods.items():
            if k not in user['prods'] or user['prods'][k] < v:
                raise E.PRODNOTENOUGH
        user['gold'] -= gold
        for k, v in prods.items():
            user['prods'][k] -= v
        user['heros'][hid] = D.HERO[hid]

    max_level = 99

    @staticmethod
    def normxp(user, xp, ahp=0):
        lv, lxp = divmod(xp, 100000)
        if lv >= E.max_level:
            raise E.MAXLEVELREADYGOT
        while lxp > D.LEVELXP[(lv + 1) * 2 + 1] and lv < 99:
            lv += 1
            lxp = lxp - D.LEVELXP[lv * 2 + 1]

            ahp = D.AWARDHP[lv * 2 + 1]
            if lv == E.max_level:
                break
        if lv >= 99:
            lv = 99
            lxp = 0
        xp = lv * 100000 + lxp
        # print 'ahp', ahp
        return xp, ahp

    @staticmethod
    def hpmax(xp):
        lv = int(xp) / 100000
        return D.LEVELHP[lv]

    hpmax = 30
    hpup = 1
    hptick = 360
    hplimit = 5

    @staticmethod
    def cost4lott(lotttype, times):
        try:
            cost = D.LOTT[lotttype][times]
        except LookupError:
            cost = None
        return cost

    lott_by_gold = 1
    lott_by_rock = 2
    limit_by_gold = 5
    limit_by_arena = 5

    timer_by_gold = 600  # 600
    timer_by_rock = 172800  # 172800
    timer_by_arena = 600
    default_formation = 1
    limit_by_refresh = 10
    limit_by_reset = 5
    cost_for_resetcd = 50

    @staticmethod
    def random_prod(lott, daylotts, times, prodproba, prodreward):
        lott = int(lott)
        start = daylotts
        end = daylotts + times
        prod_list = []
        if end < 31:
            try:
                for one in xrange(start, end):
                    prods = {}
                    lottypes = prodproba[lott][one]
                    lottype = random.choice(lottypes)
                    pid, pmin, pmax = random.choice(prodreward[lottype])
                    prods[pid] = random.randint(pmin, pmax)
                    prod_list.append(prods)
            except LookupError:
                prod_list = None

        else:
            end = 21
            try:
                for one in xrange(0, times):
                    prods = {}
                    lottypes = prodproba[lott][end + one]
                    lottype = random.choice(lottypes)
                    pid, pmin, pmax = random.choice(prodreward[lottype])
                    prods[pid] = random.randint(pmin, pmax)
                    prod_list.append(prods)
            except LookupError:
                prod_list = None
        return prod_list

    @staticmethod
    def cost4arena(pid):
        try:
            cost = D.ARENAPROD[pid]['arena_coin']
        except LookupError:
            cost = None
        return cost

    @staticmethod
    def arenamatch(now_rank, before_rank):
        history = D.ARENAHISTORY
        b = 0
        n = 0
        j = 0
        for i in xrange(0, len(history) / 4):
            if before_rank > history[i * 4] and before_rank <= history[i * 4 + 1]:
                b = i
            if 11 > history[i * 4] and 11 <= history[i * 4 + 1]:
                j = i
            if now_rank >= history[i * 4] and now_rank < history[i * 4 + 1]:
                n = i
        rock = 0
        if now_rank < before_rank <= 11:
            if n == b:
                rock += int(history[n * 4 + 2] * history[n * 4 + 3])
            if n < b:
                rock += int(sum([history[one * 4 + 2] * history[one * 4 + 3] for one in xrange(n, b + 1)]))

        if now_rank < 11 <= before_rank:
            # rock += int(sum([history[one*4+2]*history[one*4+3] for one in xrange(n, j+1)]))
            if j + 1 == b:
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(n, j + 1)]))
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])
            if j + 1 < b:
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(n, j + 1)]))
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(j + 1, b)]))
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])

        if 11 <= now_rank < before_rank:
            if n == b:
                rock += int((before_rank - now_rank) * history[n * 4 + 2] * history[n * 4 + 3])
            else:
                rock += int((history[n * 4 + 1] - now_rank) * history[n * 4 + 2] * history[n * 4 + 3])
                rock += sum(
                    [int((history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3]) for
                     one in xrange(n + 1, b)])
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])

        return rock

    @staticmethod
    def cost4search(xp, times):
        lv = xp / 100000
        try:
            huntbase = [D.HUNTBASE[i * 2 + 1] for i in xrange(0, len(D.HUNTBASE) / 2) if lv == D.HUNTBASE[i * 2]]
            huntratio = [D.HUNTRATIO[i * 2 + 1] for i in xrange(0, len(D.HUNTRATIO) / 2) if lv == D.HUNTRATIO[i * 2]]
            hunttimes = [D.HUNTTIMES[i * 3 + 2] for i in xrange(0, len(D.HUNTTIMES) / 3) if
                         times <= D.HUNTTIMES[i * 3 + 1] and times >= D.HUNTTIMES[i * 3]]
            gold = huntbase[0] + huntratio[0] * hunttimes[0]
            cost = dict(gold=gold)
        except LookupError:
            cost = dict(gold=10000)
        return cost

    @staticmethod
    def earn4hunt(csword):
        try:
            gold, feat = [(D.HUNTEARN[i * 4 + 2], D.HUNTEARN[i * 4 + 3]) for i in xrange(0, len(D.HUNTEARN) / 4) if
                          csword <= D.HUNTEARN[i * 4 + 1] and csword >= D.HUNTEARN[i * 4]][0]
            earn = dict(gold=gold, feat=feat)
        except LookupError:
            earn = {'gold': 0, 'feat': 0}
        return earn

    true = 1
    false = 0
    idle = 0  # 闲置
    reclaim = 1  # 开荒
    assart = 2  # 挖矿
    timer_by_reclaim = 10000
    resist = 0  # 反抗
    expire = 1  # 期满
    release = 2  # 释放

    @staticmethod
    def earn4against(user, guards, heros):
        try:
            sword = 0
            for one in guards:
                # hero = heros[one]
                # sword += (hero['star']*5+hero['color']*3)*(hero['xp']/100000+1)*100
                sword += 1300 + heros[one]['xp'] / 100000 * 30 + (70 + heros[one]['xp'] / 100000 + heros[one][
                    'star'] * 2) * heros[one]['star'] + (heros[one]['xp'] / 100000 + heros[one]['color']) * 80
            gold, feat = \
                [(D.HUNTAGAINST[i * 4 + 2], D.HUNTAGAINST[i * 4 + 3]) for i in xrange(0, len(D.HUNTAGAINST) / 4) if
                 csword <= D.HUNTAGAINST[i * 4 + 1] and csword >= D.HUNTAGAINST[i * 4]][0]
            earn = dict(gold=gold, feat=feat)
        except LookupError:
            earn = {'gold': 0, 'feat': 0}
        return earn

    @staticmethod
    def cost4instant(instanttimes):
        try:
            rock = [D.HUNTINSTANTCOST[i * 2 + 1] for i in xrange(0, len(D.HUNTINSTANTCOST) / 2) if
                    instanttimes == D.HUNTINSTANTCOST[i * 2]][0]
            cost = dict(rock=rock)
        except Exception:
            cost = dict(rock=100)
        return cost

    limit_by_instant = 30  # 立即完成次数
    limit_by_gainst = 30  # 反抗次数
    limit_by_search = 20  # 搜索次数
    limit_by_hunt = 20  # 掠夺次数

    @staticmethod
    def count4sword(guards, heros):
        csword = 0
        for one in guards:
            # csword += (heros[one]['star']*5+heros[one]['color']*3)*(heros[one]['xp']/100000+1)*100
            # csword += 1300+heros[one]['xp']*30+((60+heros[one]['star']*20)/2)*(20+heros[one]['color']*4)*(heros[one]['xp']/100000+1)
            csword += 1300 + heros[one]['xp'] / 100000 * 30 + (
                                                                  70 + heros[one]['xp'] / 100000 + heros[one][
                                                                      'star'] * 2) * \
                                                              heros[one]['star'] + (heros[one]['xp'] / 100000 +
                                                                                    heros[one]['color']) * 80
            # print 'csword', csword
        return csword

    @staticmethod
    def buy4hp(times):
        if times > 20:
            times = 20
        try:
            rock, hp = \
                [(D.HPBUY[i * 3 + 1], D.HPBUY[i * 3 + 2]) for i in xrange(0, len(D.HPBUY) / 3) if
                 times == D.HPBUY[i * 3]][
                    0]
            cost = dict(rock=rock, hp=hp)
        except Exception:
            cost = dict(rock=50, hp=120)
        return cost

    rate = 0.3

    @staticmethod
    def buy4gold(start, times, xp):
        buy = []
        end = start + times
        if end <= 90:
            for time in xrange(start, end):
                rock, gold = [(D.GOLDBUY[i * 3 + 1], D.GOLDBUY[i * 3 + 2]) for i in xrange(0, len(D.GOLDBUY) / 3) if
                              time == D.GOLDBUY[i * 3]][0]
                gold += xp / 100000 * 50
                extra = 0
                if random.random() < E.rate:
                    extra = gold
                buy.append(dict(rock=rock, gold=gold, extra=extra))

        elif end <= 100 and end > 90:
            for time in xrange(start, 100):
                rock, gold = [(D.GOLDBUY[i * 3 + 1], D.GOLDBUY[i * 3 + 2]) for i in xrange(0, len(D.GOLDBUY) / 3) if
                              time == D.GOLDBUY[i * 3]][0]
                gold += xp / 100000 * 50
                extra = 0
                if random.random() < E.rate:
                    extra = gold
                buy.append(dict(rock=rock, gold=gold, extra=extra))

            for time in xrange(0, end - 100):
                rock, gold = [(D.GOLDBUY[i * 3 + 1], D.GOLDBUY[i * 3 + 2]) for i in xrange(0, len(D.GOLDBUY) / 3) if
                              100 == D.GOLDBUY[i * 3]][0]
                gold += xp / 100000 * 50
                extra = 0
                if random.random() < E.rate:
                    extra = gold
                buy.append(dict(rock=rock, gold=gold, extra=extra))
        else:
            for time in xrange(0, times):
                rock, gold = [(D.GOLDBUY[i * 3 + 1], D.GOLDBUY[i * 3 + 2]) for i in xrange(0, len(D.GOLDBUY) / 3) if
                              100 == D.GOLDBUY[i * 3]][0]
                gold += xp / 100000 * 50
                extra = 0
                if random.random() < E.rate:
                    extra = gold
                buy.append(dict(rock=rock, gold=gold, extra=extra))
        return buy

    @staticmethod
    def vip(vrock):
        vip = 0
        for i in xrange(0, len(D.VIP) / 2):
            if i < len(D.VIP) / 2 - 1:
                if vrock >= D.VIP[i * 2 + 1] and vrock < D.VIP[i * 2 + 3] and i < len(D.VIP) / 2 - 1:
                    vip = D.VIP[i * 2]
                    break
            else:
                if vrock >= D.VIP[i * 2 + 1]:
                    vip = D.VIP[i * 2]
        return vip

    @staticmethod
    def hpmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.HPBUYTIMES[i * 2 + 1] for i in xrange(0, len(D.HPBUYTIMES) / 2) if vip == D.HPBUYTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def goldmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.GOLDBUYTIMES[i * 2 + 1] for i in xrange(0, len(D.GOLDBUYTIMES) / 2) if
                     vip == D.GOLDBUYTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def arenamaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.ARENARESETTIMES[i * 2 + 1] for i in xrange(0, len(D.ARENARESETTIMES) / 2) if
                     vip == D.ARENARESETTIMES[i * 2]]
        return maxtimes

    # hunt reset maxtimes
    @staticmethod
    def huntmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.HUNTRESETTIMES[i * 2 + 1] for i in xrange(0, len(D.HUNTRESETTIMES) / 2) if
                     vip == D.HUNTRESETTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def gainstmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.GAINSTESETTIMES[i * 2 + 1] for i in xrange(0, len(D.GAINSTESETTIMES) / 2) if
                     vip == D.GAINSTESETTIMES[i * 2]]
        return maxtimes

    win = 1
    lose = 0

    @staticmethod
    def check4prisonid(user):
        pids = []
        vip = E.vip(user['vrock'])
        for i in xrange(0, len(D.PRISONID) / 3):
            if D.PRISONID[i * 3 + 1] == -1:
                if user['xp'] / 100000 >= D.PRISONID[i * 3 + 2]:
                    pids.append(D.PRISONID[i * 3])
            else:
                if vip >= D.PRISONID[i * 3 + 1]:
                    pids.append(D.PRISONID[i * 3])
        return pids

    @staticmethod
    def bookmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.BOOKBUYTIMES[i * 2 + 1] for i in xrange(0, len(D.BOOKBUYTIMES) / 2) if
                     vip == D.BOOKBUYTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def buy4book(times):
        if times > 20:
            times = 20
        try:
            rock, book = [(D.BOOKBUY[i * 3 + 1], D.BOOKBUY[i * 3 + 2]) for i in xrange(0, len(D.BOOKBUY) / 3) if
                          times == D.BOOKBUY[i * 3]][0]
            cost = dict(rock=rock, prods={'04001': book})
        except Exception:
            cost = dict(rock=999999, prods={})
        return cost

    @staticmethod
    def buy4sp(times):
        if times > 16:
            times = 16
        try:
            rock, sp = \
                [(D.SPBUY[i * 3 + 1], D.SPBUY[i * 3 + 2]) for i in xrange(0, len(D.SPBUY) / 3) if
                 times == D.SPBUY[i * 3]][
                    0]
            cost = dict(rock=rock, sp=sp)
        except Exception:
            cost = dict(rock=999999, sp=0)
        return cost

    @staticmethod
    def spmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes = 10
        return maxtimes

    @staticmethod
    def earn4tax(batt):
        feat = gold = 0
        try:
            pos, = [i for i in xrange(0, len(D.TAX) / 3) if batt == D.TAX[i * 3]]
            feat += D.TAX[pos * 3 + 1]
            gold += D.TAX[pos * 3 + 2]
        except Exception:
            feat = feat
            gold = gold
        awards = dict(feat=feat, gold=gold)
        return awards

    busy = 0
    done = 1
    assart = 0
    mine = 1

    @staticmethod
    def minemaxnum(vrock):
        vip = E.vip(vrock)
        minemaxnum, = [D.MINELIMIT[i * 2 + 1] for i in xrange(0, len(D.MINELIMIT) / 2) if vip == D.MINELIMIT[i * 2]]
        return minemaxnum

    @staticmethod
    def duration4mine(mtype, size):
        duration, = [D.MINE[i * 3 + 2] for i in xrange(0, len(D.MINE) / 3) if
                     mtype == D.MINE[i * 3] and size == D.MINE[i * 3 + 1]]
        return duration

    @staticmethod
    def earn4mine(guards, heros, mtype):
        gold = rock = feat = 0
        csword = E.count4sword(guards, heros)
        try:
            if mtype:
                gold, rock = \
                    [(D.GOLDOUTPUT[i * 4 + 2], D.GOLDOUTPUT[i * 4 + 3]) for i in xrange(0, len(D.GOLDOUTPUT) / 4) if
                     csword >= D.GOLDOUTPUT[i * 4] and csword <= D.GOLDOUTPUT[i * 4 + 1]][0]
                rate = dict(gold=gold, rock=rock)
            else:
                feat, rock = \
                    [(D.FEATOUTPUT[i * 4 + 2], D.FEATOUTPUT[i * 4 + 3]) for i in xrange(0, len(D.FEATOUTPUT) / 4) if
                     csword >= D.FEATOUTPUT[i * 4] and csword <= D.FEATOUTPUT[i * 4 + 1]][0]
                rate = dict(feat=feat, rock=rock)
        except Exception:
            rate = dict(gold=0, rock=0, )
        return rate

    @staticmethod
    def earn4rape(user, guards, heros, mtype):
        gold = rock = feat = 0
        try:
            csword = E.count4sword(guards, heros)
            if mtype:
                gold, rock = [(D.HUNTRAPE[i * 5 + 2], D.HUNTRAPE[i * 5 + 4]) for i in xrange(0, len(D.HUNTRAPE) / 5) if
                              csword <= D.HUNTRAPE[i * 5 + 1] and csword >= D.HUNTRAPE[i * 5]][0]
            else:
                feat, rock = [(D.HUNTRAPE[i * 5 + 3], D.HUNTRAPE[i * 5 + 4]) for i in xrange(0, len(D.HUNTRAPE) / 5) if
                              csword <= D.HUNTRAPE[i * 5 + 1] and csword >= D.HUNTRAPE[i * 5]][0]
            earn = dict(gold=gold, rock=rock, feat=feat)
        except Exception:
            earn = {'gold': 0, 'rock': 0, 'feat': 0}
        return earn

    spup = 1
    sptick = 300

    @staticmethod
    def spmax(vrock):
        vip = E.vip(vrock)
        spmax, = [D.SPLIMIT[i * 2 + 1] for i in xrange(0, len(D.SPLIMIT) / 2) if vip == D.SPLIMIT[i * 2]]
        return spmax

    dispear = 3600

    @staticmethod
    def blackmarket(times):
        times = times % 12
        weekday = datetime.date.today().weekday()
        prods = {}
        for i in xrange(0, len(D.BLACKMARKET) / 9):
            if weekday == D.BLACKMARKET[i * 9] and times == D.BLACKMARKET[i * 9 + 4]:
                prods[D.BLACKMARKET[i * 9 + 3]] = dict(id=str(D.BLACKMARKET[i * 9 + 5]).zfill(5), is_buy=0,
                                                       num=D.BLACKMARKET[i * 9 + 6], gold=D.BLACKMARKET[i * 9 + 7],
                                                       rock=D.BLACKMARKET[i * 9 + 8])
        return prods

    @staticmethod
    def market(times):
        times = times % 12
        weekday = datetime.date.today().weekday()
        prods = {}
        for i in xrange(0, len(D.MARKET) / 9):
            if weekday == D.MARKET[i * 9] and times == D.MARKET[i * 9 + 4]:
                prods[D.MARKET[i * 9 + 3]] = dict(id=str(D.MARKET[i * 9 + 5]).zfill(5), is_buy=0,
                                                  num=D.MARKET[i * 9 + 6], gold=D.MARKET[i * 9 + 7],
                                                  rock=D.MARKET[i * 9 + 8])
        return prods

    @staticmethod
    def store(times):
        times = times % 12
        weekday = datetime.date.today().weekday()
        prods = {}
        for i in xrange(0, len(D.STORE) / 9):
            if weekday == D.STORE[i * 9] and times == D.STORE[i * 9 + 4]:
                prods[D.STORE[i * 9 + 3]] = dict(id=str(D.STORE[i * 9 + 5]).zfill(5), is_buy=0, num=D.STORE[i * 9 + 6],
                                                 gold=D.STORE[i * 9 + 7], rock=D.STORE[i * 9 + 8])
        return prods

    @staticmethod
    def warshiptimes(vrock):
        vip = E.vip(vrock)
        maxwartimes, = [D.WORSHIPTIMES[i * 2 + 1] for i in xrange(0, len(D.WORSHIPTIMES) / 2) if
                        vip == D.WORSHIPTIMES[i * 2]]
        return maxwartimes

    @staticmethod
    def type4worship(wtype):
        try:
            gold, rock, hp = [(D.WORSHIPTYPE[i * 4 + 1], D.WORSHIPTYPE[i * 4 + 2], D.WORSHIPTYPE[i * 4 + 3]) for i in
                              xrange(0, len(D.WORSHIPTYPE) / 4) if wtype == D.WORSHIPTYPE[i * 4]][0]
        except Exception:
            gold = rock = hp = 0
        cost = dict(gold=gold, rock=rock)
        awards = dict(hp=hp)
        return (cost, awards)

    @staticmethod
    def award4worship(wtype, times):
        if times > 30:
            times = 30
        gold, = [D.WORSHIPAWRAD[i * 3 + 2] for i in xrange(0, len(D.WORSHIPAWRAD) / 3) if
                 wtype == D.WORSHIPAWRAD[i * 3] and times == D.WORSHIPAWRAD[i * 3 + 1]]
        return gold

    @staticmethod
    def cost4beauty(btype, level):
        prods = {}
        gold = rock = 0
        try:
            if not btype:
                gold, prod, num = \
                    [(D.BEAUTY[i * 7 + 1], D.BEAUTY[i * 7 + 2], D.BEAUTY[i * 7 + 3]) for i in
                     xrange(0, len(D.BEAUTY) / 7)
                     if level == D.BEAUTY[i * 7]][0]
            else:
                rock, prod, num = \
                    [(D.BEAUTY[i * 7 + 4], D.BEAUTY[i * 7 + 5], D.BEAUTY[i * 7 + 6]) for i in
                     xrange(0, len(D.BEAUTY) / 7)
                     if level == D.BEAUTY[i * 7]][0]
            prods[prod] = int(num)
            cost = dict(gold=gold, rock=rock, prods=prods)
        except Exception:
            cost = None
        return cost

    @staticmethod
    def pearlmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.PEARLBUYTIMES[i * 2 + 1] for i in xrange(0, len(D.PEARLBUYTIMES) / 2) if
                     vip == D.PEARLBUYTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def buy4pearl(times):
        if times > 20:
            times = 20
        try:
            rock, book = [(D.PEARLBUY[i * 3 + 1], D.PEARLBUY[i * 3 + 2]) for i in xrange(0, len(D.PEARLBUY) / 3) if
                          times == D.PEARLBUY[i * 3]][0]
            cost = dict(rock=rock, prods={'04002': book})
        except Exception:
            cost = dict(rock=999999, prods={})
        return cost

    @staticmethod
    def battmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.HARDBATTTIMES[i * 2 + 1] for i in xrange(0, len(D.HARDBATTTIMES) / 2) if
                     vip == D.HARDBATTTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def buy4batt(times):
        if times > 20:
            times = 20
        try:
            rock, = [D.BATTBUY[i * 3 + 1] for i in xrange(0, len(D.BATTBUY) / 3) if times == D.BATTBUY[i * 3]]
        except Exception:
            rock = 99999
        cost = dict(rock=rock)
        return cost

    @staticmethod
    def incr4hp(vrock):
        vip = E.vip(vrock)
        hp, = [D.HPINCR[i * 2 + 1] for i in xrange(0, len(D.HPINCR) / 2) if vip == D.HPINCR[i * 2]]
        return hp

    @staticmethod
    def random_prods(group, start, end, mprods, times):
        # print 333, group, start, end, times
        if group > 20:
            group = 0
        prods = {}
        start = start + group * 100
        end = end + group * 100
        # print start, end, group
        if start >= group * 100 and end <= (group + 1) * 100:
            if end == (group + 1) * 100:
                group = random.randint(0, len(mprods) / 100 - 1)
                battentrytimes = 0
            else:
                battentrytimes = end - group * 100
            prods_list = mprods[start:end]

        elif start < (group + 1) * 100 and end > (group + 1) * 100:
            new_group = random.randint(0, len(mprods) / 100 - 1)
            prods_list = mprods[start:(group + 1) * 100] + mprods[
                                                           new_group * 100:new_group * 100 + (end - (group + 1) * 100)]
            last_group = group
            group = new_group
            battentrytimes = end - (last_group + 1) * 100

        elif start == (group + 1) * 100:
            group = random.randint(0, len(mprods) / 100 - 1)
            prods_list = mprods[group * 100:group * 100 + (end - start)]
            battentrytimes = end - start

        elif start > (group + 1) * 100:
            group = random.randint(0, len(mprods) / 100 - 1)
            mod, step = divmod(start, 100)
            prods_list = mprods[group * 100 + step:group * 100 + (end - start)]
            mod, battentrytimes = divmod(end, 100)

        if battentrytimes == 100:
            battentrytimes = 0

        for one in prods_list:
            if one in prods:
                prods[one] += times
            else:
                prods[one] = times
        return (prods, group, battentrytimes)

    @staticmethod
    def random_exped_prods(start, end, mprods, times):
        # print start, end, mprods, times
        prods = {}
        if times == 1:
            prod = mprods[(start + times) % len(mprods)]
            if prod in prods:
                prods[prod] += 1
            else:
                prods[prod] = 1
        elif times > 1 and times < len(mprods):
            if (start + times) <= len(mprods):
                for one in xrange(start, start + times):
                    prod = mprods[one]
                    if prod in prods:
                        prods[prod] += 1
                    else:
                        prods[prod] = 1
            elif (start + times) % len(mprods) < start % len(mprods):
                for one in xrange(start % len(mprods), len(mprods)):
                    prod = mprods[one]
                    if prod in prods:
                        prods[prod] += 1
                    else:
                        prods[prod] = 1
                for one in xrange(0, (start + times) % len(mprods)):
                    prod = mprods[one]
                    if prod in prods:
                        prods[prod] += 1
                    else:
                        prods[prod] = 1

            elif (start + times) % len(mprods) > start % len(mprods):
                for one in xrange(start % len(mprods), (start + times) % len(mprods)):
                    prod = mprods[one]
                    if prod in prods:
                        prods[prod] += 1
                    else:
                        prods[prod] = 1
        else:
            if times >= len(mprods):
                for one in xrange(0, len(mprods)):
                    prod = mprods[one]
                    if prod in prods:
                        prods[prod] += 1
                    else:
                        prods[prod] = 1
        return prods

    unused = 0
    used = 1

    @staticmethod
    def bmmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.BMRESETTIMES[i * 2 + 1] for i in xrange(0, len(D.BMRESETTIMES) / 2) if
                     vip == D.BMRESETTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def marketmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.MARKETRESETTIMES[i * 2 + 1] for i in xrange(0, len(D.MARKETRESETTIMES) / 2) if
                     vip == D.MARKETRESETTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def storemaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.STORERESETTIMES[i * 2 + 1] for i in xrange(0, len(D.STORERESETTIMES) / 2) if
                     vip == D.STORERESETTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def calc_sword(heros):
        sword = 0
        for key in heros.keys():
            hero = heros[key]
            sword += int(
                1300 + hero['xp'] / 100000 * 71.6 + (70 + hero['xp'] / 100000 + hero['star'] * 2) * hero['star'] +
                hero['color'] * 80 + hero['skills'][0] * 96 * 0.2 + hero['skills'][1] * 96 * 0.2)
        return sword

    @staticmethod
    def calc_topsword(user):

        def calc_sword(hero):
            return int(
                1300 + hero['xp'] / 100000 * 71.6 + (70 + hero['xp'] / 100000 + hero['star'] * 2) * hero['star'] +
                hero['color'] * 80 + hero['skills'][0] * 96 * 0.2 + hero['skills'][1] * 96 * 0.2)

        def calc_beauty(beautys):
            return sum([v * 13 for v in beautys.values()])

        heros = user['heros']
        beautys = user['beautys']
        heross = sorted([(hid, calc_sword(heros[hid]), heros[hid]) for hid in heros],
                        key=lambda x: x[1], reverse=True)
        topheros = heross[:5]
        tophids = [h[0] for h in topheros]
        topsword = reduce(lambda x, y: x + y[1], topheros, 0) + calc_beauty(beautys)
        return topsword

    @staticmethod
    def double4hunt(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.HUNTDOUBEL[i * 2 + 1] for i in xrange(0, len(D.HUNTDOUBEL) / 2) if vip == D.HUNTDOUBEL[i * 2]]
        return maxtimes

    @staticmethod
    def cost4exped(pid):
        try:
            cost = D.EXPEDPROD[pid]['exped_coin']
        except LookupError:
            cost = None
        return cost

    limit_by_versus = 5
    timer_by_versus = 600

    @staticmethod
    def expedmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.EXPEDRESETTIMES[i * 2 + 1] for i in xrange(0, len(D.EXPEDRESETTIMES) / 2) if
                     vip == D.EXPEDRESETTIMES[i * 2]]
        return maxtimes

    @staticmethod
    def earn4versus(now_rank):
        try:
            versus_coin, = [D.VERSUSOUTPUT[i * 3 + 2] for i in xrange(0, len(D.VERSUSOUTPUT) / 3) if
                            now_rank >= D.VERSUSOUTPUT[i * 3] and now_rank <= D.VERSUSOUTPUT[i * 3 + 1]]
            rate = dict(versus_coin=versus_coin)
        except Exception:
            rate = dict(versus_coin=0)
        return rate

    @staticmethod
    def cost4versus(pid):
        try:
            cost = D.VERSUSPROD[pid]['versus_coin']
        except LookupError:
            cost = None
        return cost

    @staticmethod
    def versusmatch(now_rank, before_rank):
        history = D.VERSUSHISTORY
        b = 0
        n = 0
        j = 0
        for i in xrange(0, len(history) / 4):
            if before_rank > history[i * 4] and before_rank <= history[i * 4 + 1]:
                b = i
            if 11 > history[i * 4] and 11 <= history[i * 4 + 1]:
                j = i
            if now_rank >= history[i * 4] and now_rank < history[i * 4 + 1]:
                n = i
        rock = 0
        if now_rank < before_rank <= 11:
            if n == b:
                rock += int(history[n * 4 + 2] * history[n * 4 + 3])
            if n < b:
                rock += int(sum([history[one * 4 + 2] * history[one * 4 + 3] for one in xrange(n, b + 1)]))

        if now_rank < 11 <= before_rank:
            # rock += int(sum([history[one*4+2]*history[one*4+3] for one in xrange(n, j+1)]))
            if j + 1 == b:
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(n, j + 1)]))
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])
            if j + 1 < b:
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(n, j + 1)]))
                rock += int(sum(
                    [(history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3] for one in
                     xrange(j + 1, b)]))
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])

        if 11 <= now_rank < before_rank:
            if n == b:
                rock += int((before_rank - now_rank) * history[n * 4 + 2] * history[n * 4 + 3])
            else:
                rock += int((history[n * 4 + 1] - now_rank) * history[n * 4 + 2] * history[n * 4 + 3])
                rock += sum(
                    [int((history[one * 4 + 1] - history[one * 4]) * history[one * 4 + 2] * history[one * 4 + 3]) for
                     one in xrange(n + 1, b)])
                rock += int((before_rank - history[b * 4]) * history[b * 4 + 2] * history[b * 4 + 3])

        return rock

    @staticmethod
    def versusmaxtimes(vrock):
        vip = E.vip(vrock)
        maxtimes, = [D.VERSUSRESETTIMES[i * 2 + 1] for i in xrange(0, len(D.VERSUSRESETTIMES) / 2) if
                     vip == D.VERSUSRESETTIMES[i * 2]]
        return maxtimes
