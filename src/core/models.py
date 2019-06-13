# -*- coding: utf-8 -*-

import datetime
import hashlib
from django.db import models
from django.db import models
from django_extensions.db.models import TimeStampedModel
from django.utils.text import ugettext_lazy as _
from cyclone import escape
from signals import *
from positions import PositionField
from django.db.models.signals import post_save, post_delete
from django.core.exceptions import ValidationError
from filebrowser.fields import FileBrowseField, FileObject
from django.contrib.auth.models import User as AdminUser
from passlib.apps import custom_app_context as pwd_context


# from django.contrib.postgres.fields import JSONField


class Channel(models.Model):
    title = models.CharField(_('Title'), max_length=20)
    slug = models.SlugField(_('Slug'))

    class Meta:
        verbose_name = _('Channel')
        verbose_name_plural = _('Channels')
        ordering = ('slug',)

    def __unicode__(self):
        return self.title


#
# 
# class User(models.Model):
#     id = models.AutoField(primary_key=True)
#     hex = models.CharField(_('Hex'), max_length=64, unique=True)
#     user_id = models.CharField(_('User_id'), max_length=20, blank=True)
# 
#     model = models.CharField(_('Model'), max_length=100, blank=True)
#     serial = models.CharField(_('Serial'), max_length=20, unique=True)
#     channel = models.ForeignKey(Channel, blank=True, null=True)
#     nickname = models.CharField(_('Nickname'), max_length=30, blank=True, db_index=True)
#     avat = models.CharField(_('Avatar'), max_length=20, blank=True)
#     playerLevel = models.PositiveIntegerField(_('PlayerLevel'), default=0, db_index=True)  # 玩家等级 uint from 1
#     playerXp = models.PositiveIntegerField(_('PlayerXp'), default=0,
#                                            db_index=True)  # 玩家经验，可能暂时不用，直接用所有武将和兵的等级总和来计算玩家等级
#     goldcoin = models.PositiveIntegerField(_('Goldcoin'), default=0)  # 元宝
#     gem = models.PositiveIntegerField(_('Gem'), default=0)  # 钻石
#     honorPoint = models.PositiveIntegerField(_('HonorPoint'), default=0)  # 荣誉点数
# 
#     arena5v5Rank = models.PositiveIntegerField(_('Arena5v5Rank'), default=0)  # 排名模式 5V5竞技场,名次等级 0 ~ 20 0级为最高级
#     arena5v5Place = models.PositiveIntegerField(_('Arena5v5Place'), default=0)  # 排名模式 5V5竞技场,等级为0时有详细名次，非0时无效
# 
#     arenaOtherRank = models.PositiveIntegerField(_('ArenaOtherRank'), default=0)  # 其他规则天梯
#     arenaOtherPlace = models.PositiveIntegerField(_('ArenaOtherPlace'), default=0)  # 其他规则天梯
# 
#     heroList = models.TextField(_('JSON Heros'), blank=True)  # 武将表,level == 0 是未解锁（获得）武将
#     soldierList = models.TextField(_('JSON Soldier'), blank=True)  # 士兵表,level == 0 是未获得士兵
#     formations = models.TextField(_('JSON Formations'),
#                                   blank=True)  # formation 保存的队伍信息，最多5个武将，可少于5个，没有保存队伍则 formation 是空
#     items = models.TextField(_('JSON Items'), blank=True)  # 拥有的物品列表
# 
#     headIconList = models.TextField(_('JSON HeadIconList'), blank=True)  # headIconList
#     titleList = models.TextField(_('JSON TitleList'), blank=True)  # 可用的玩家称号列表
#     achievement = models.TextField(_('JSON Achievement'), blank=True)  # 已完成的成就id列表,系列成就如何处理？
# 
#     playerConfig = models.TextField(_('JSON PlayerConfig'), blank=True)  # 玩家设置，今天内容再细化
#     buddyList = models.TextField(_('JSON BuddyList'), blank=True)  # 好友信息列表，玩家状态暂时不好实现的话，就分在线离线也行
# 
#     playerStatusInfo = models.TextField(_('JSON PlayerStatusInfo'), blank=True)
#     # 邮件
#     jmails = models.TextField(_('JSON Mails'), blank=True)
# 
#     # 下面是关卡进度
# 
#     # 命名规则：
#     # annal 编年史
#     # biography 帝王列传
#     # Normal 普通
#     # Hero 英雄（英杰）
#     # Epic 史诗
#     # dungeon 副本模式
#     annalNormal = models.TextField(_('JSON annalNormal'), blank=True)  # 普通大关卡章节进度
#     annelCurrentGateNormal = models.TextField(_('JSON annelCurrentGateNormal'), blank=True)  # 普通当前解锁章节 小关卡进度
# 
#     annalHero = models.TextField(_('JSON annalHero'), blank=True)  # 英雄大关卡章节进度
#     annelCurrentGateHero = models.TextField(_('JSON annelCurrentGateHero'), blank=True)  # 英雄小关卡章节进度
# 
#     annalEpic = models.TextField(_('JSON annalEpic'), blank=True)  # 史诗难度章节进度
#     # 史诗难度，上次说史诗难度第一版先不上，所以史诗难度的数据没列，这次先按大概方案列一下吧
#     # 史诗难度全是副本模式，默认大小关卡全部解锁，不需要返回小关卡进度数据，
#     # 但是服务器需要保存史诗难度的大关卡完成情况（同冒险模式），成就使用，例如： "成就：我是传奇！ 【完成全部史诗难度战役】"。
#     # 史诗难度的副本刷新进度大概是 7天，可能部分小副本 可以是 3天，暂时都按7天大副本做。
#     # 普通 和 英雄副本 完成之后会重置，可再次进入，史诗副本完成也不会刷新，只能等到服务器自动刷新周期刷新
#     # （例如：每周四2:00刷新7天的大副本，每周一和周五2:00刷新3天的小副本）。
#     dungeonAnnelHero = models.TextField(_('JSON dungeonAnnelHero'), blank=True)  # 当天英雄副本奖励
#     # 当周期内（有的7天，有的3天） 史诗副本奖励，
#     # 由于副本是服务器刷新，所以获得奖励的状态，与副本状态统一，即：没获得奖励的关卡一定未完成，获得奖励的关卡一定已完成
#     dungeonAnnelEpic = models.TextField(_('JSON dungeonAnnelEpic'), blank=True)  # 当天史诗副本奖励
# 
#     dungeonAnnelGatesNormal = models.TextField(_('JSON dungeonAnnelGatesNormal'), blank=True)  # 普通难度副本进度
#     dungeonAnnelGatesHero = models.TextField(_('JSON dungeonAnnelGatesHero'), blank=True)  # 英雄难度副本进度
#     # 史诗难度副本进度
#     # 此处只返回部分完成的副本进度关卡进度 数据，全部完成的和完全没打过的，在dungeonAnnelEpic表里可以提现
#     dungeonAnnelGatesEpic = models.TextField(_('JSON dungeonAnnelGatesEpic'), blank=True)  # 史诗难度副本进度
# 
#     created = models.PositiveIntegerField(_('Created'), default=0, blank=True)
#     modified = models.PositiveIntegerField(_('Modified'), default=0, blank=True)
# 
#     class Meta:
#         verbose_name = _('User')
#         verbose_name_plural = _('Users')
# 
#     def __unicode__(self):
#         return self.nickname
# 
# 
# post_save.connect(syncdb_callback_function, sender=User)
# post_delete.connect(syncdb_callback_function, sender=User)
# 
# 
# class Arena(models.Model):
#     user = models.OneToOneField(User)
#     rank = models.PositiveIntegerField(_('Rank'), default=25, blank=True)
#     star = models.PositiveIntegerField(_('Star'), default=0, blank=True)
#     timestamp = models.PositiveIntegerField(_('Timestamp'), default=0, blank=True)
# 
#     class Meta:
#         verbose_name = _('Arena')
#         verbose_name_plural = _('Arenas')
# 
#     def __unicode__(self):
#         return self.user.nickname
# 
# 
# post_save.connect(syncdb_callback_function, sender=Arena)
# post_delete.connect(syncdb_callback_function, sender=Arena)


# class Zone(models.Model):
#     zoneid = models.PositiveIntegerField(_('Zoneid'), default=0)
#     name = models.CharField(_('Zone Name'), max_length=20, unique=True)
#
#     class Meta:
#         verbose_name = _('Zone')
#         verbose_name_plural = _('Zones')
#
#     def __unicode__(self):
#         return ':'.join([self.name, str(self.zoneid)])


# class Notice(models.Model):
#     title = models.CharField(_('Title'), max_length=20, unique=True)
#     content = models.TextField(blank=True)
#     screenshot = FileBrowseField(_('Screenshot'), max_length=200, directory='img/screenshot', format='image',
#                                  extensions=[".jpg"], blank=True)
#     url = models.CharField(_('Url'), max_length=20, blank=True)
#     sign = models.CharField(_('Sign'), max_length=20, blank=True)
#     created_at = models.DateTimeField(_('Created_at'), default=datetime.datetime.now)
#     ended_at = models.DateTimeField(_('Ended_at'), default=datetime.datetime.now)
#
#     class Meta:
#         verbose_name = _('Notice')
#         verbose_name_plural = _('Notices')
#
#     def __unicode__(self):
#         return self.title


# class Noticeship(models.Model):
#     zone = models.ForeignKey(Zone)
#     notice = models.ForeignKey(Notice)
#     position = PositionField(_('Position'), collection='zone')
#
#     class Meta:
#         verbose_name = 'Noticeship'
#         verbose_name_plural = _('Noticeships')
#         ordering = ('position',)
#
#     def __unicode__(self):
#         return self.notice.title


# class Prop(models.Model):
#     user = models.ForeignKey(User)
#     label = models.CharField(_('Label'), max_length=20, db_index=True)
#     num = models.PositiveIntegerField(_('Num'), blank=True, null=True)
#     txt = models.TextField(_('Txt'), blank=True, null=True)
#     timestamp = models.PositiveIntegerField(_('Timestamp'), default=0, blank=True)
#
#     class Meta:
#         verbose_name = _('Prop')
#         verbose_name_plural = _('Props')
#         unique_together = ('user', 'label')
#
#     def __unicode__(self):
#         return self.user.name





# class Product(models.Model):
#     FIRST = 0
#     SECOND = 1
#     ORDER = (
#         (FIRST, _('First')),
#         (SECOND, _('Second')),
#     )
#     CARD = 1
#     UNLIMIT = 2
#     LIMIT = 3
#     TYPE = (
#         (CARD, _('Card')),
#         (UNLIMIT, _('Unlimit')),
#         (LIMIT, _('Limit')),
#     )
#     UNRESTRICT = 0
#     RESTRICT = 1
#     TIMES = (
#         (UNRESTRICT, _('Unrestrict')),
#         (RESTRICT, _('Restrict')),
#     )
#     pid = models.CharField(_('Pid'), max_length=64)
#     code = models.CharField(_('Code'), max_length=64)
#     sequence = models.PositiveSmallIntegerField(
#         _('Sequence'), choices=ORDER, default=FIRST)
#     type = models.PositiveSmallIntegerField(
#         _('Type'), choices=TYPE, default=LIMIT)
#     value = models.PositiveIntegerField(_('Value'), default=0)
#     extra = models.PositiveIntegerField(_('Extra'), default=0)
#     times = models.PositiveSmallIntegerField(
#         _('Times'), choices=TIMES, default=UNRESTRICT)
#     price = models.PositiveIntegerField(_('Price'), default=0)
#     created_at = models.PositiveIntegerField(_('Created_at'), default=0)
#     channels = models.ManyToManyField('Channel', blank=True)
#
#     class Meta:
#         verbose_name = _('Product')
#         verbose_name_plural = _('Products')
#
#     def __unicode__(self):
#         return u'%s' % self.pid


# class GmLog(models.Model):
#     user = models.ForeignKey(AdminUser)
#     content = models.TextField(blank=True)
#     created_at = models.DateTimeField(default=datetime.datetime.now)
#
#     class Meta:
#         verbose_name = _('GmLog')
#         verbose_name_plural = _('GmLogs')
#
#     def __unicode__(self):
#         return self.user


# class Gate(models.Model):
#     gate_id = models.CharField(_('Gate Id'), max_length=100, blank=True)
#     jgates = models.TextField(_('JSON Gates'), blank=True)
#     timestamp = models.PositiveIntegerField(_('Timestamp'), default=0, blank=True)
#
#     class Meta:
#         verbose_name = _('Gate')
#         verbose_name_plural = _('Gate')
#
#     def __unicode__(self):
#         return self.gate_id


class Gate(TimeStampedModel):
    gate_id = models.CharField(_('gate id'), max_length=64)
    vers = models.FloatField(_('vers'), blank=True, null=True, default=0.0)  # 版本号
    rs = models.TextField(_('rs'), blank=True)  # 随机种子 整数组
    itemTypes = models.TextField(_('itemTypes'), blank=True)  # 颜色类型 整数组
    props = models.TextField(_('props'), blank=True)  # 道具种类 整数组
    taskStep = models.IntegerField(_('taskStep'), default=0)
    tasks = models.TextField(_('tasks'), blank=True)  # 任务	二维数组
    scores = models.TextField(_('scores'), blank=True)  # 分数 整数组(3或4元素)
    gird = models.TextField(_('gird'), blank=True)  # 地形 0-1数组(81)
    newGridTypes = models.TextField(_('newGridTypes'), blank=True)  # 出生点元素类型 二维数组（字符串）
    newGrid = models.TextField(_('newGrid'), blank=True)  # 出生点 整数组（81）
    portal = models.TextField(_('portal'), blank=True)  # 传送门			二维数组（整数）
    item = models.TextField(_('item'), blank=True)  # 元素			数组（81，字符串）
    itemBg = models.TextField(_('itemBg'), blank=True)  # 元素背景		数组（81，字符串）
    wallH = models.TextField(_('wallH'), blank=True)  # 水平墙		0-1数组（90）
    wallV = models.TextField(_('wallV'), blank=True)  # 竖直墙		0-1数组（90）
    taskBgItem = models.TextField(_('taskBgItem'), blank=True)
    wayDownOut = models.TextField(_('wayDownOut'), blank=True)
    attach = models.TextField(_('attach'), blank=True)
    diff = models.CharField(_('diff'), max_length=64)
    taskType = models.CharField(_('taskType'), max_length=64)
    trackBelt = models.TextField(_('trackBelt'), blank=True)
    iceWall = models.TextField(_('iceWall'), blank=True)
    flipBlocker = models.TextField(_('flipBlocker'), blank=True)
    movingFloor = models.TextField(_('movingFloor'), blank=True)

    class Meta:
        verbose_name = _('Gate')
        verbose_name_plural = _('Gates')

    def __unicode__(self):
        return u'%s' % self.vers


class Prop(TimeStampedModel):
    FRONT = 1
    MIDDLE = 2
    BACK = 3
    HP = 4
    TYPE = (
        (FRONT, _('FRONT')),
        (MIDDLE, _('MIDDLE')),
        (BACK, _('BACK')),
        (HP, _('HP')),
    )
    pid = models.CharField(_('Pid'), max_length=64)
    name = models.CharField(_('Name'), max_length=20, db_index=True)
    num = models.PositiveIntegerField(_('Num'), blank=True, null=True)  # 每局使用次数
    type = models.PositiveSmallIntegerField(
        _('Type'), choices=TYPE, default=FRONT)  # 类型
    expired = models.DateTimeField(default=datetime.datetime.now())

    class Meta:
        verbose_name = _('Prop')
        verbose_name_plural = _('Props')

    def __unicode__(self):
        return self.name


class User(TimeStampedModel):
    username = models.CharField(_('Username'), max_length=32, unique=True)
    password_hash = models.CharField(_('Password_hash'), max_length=128, blank=True)
    access_token = models.CharField(_('Access_token'), max_length=128, unique=True)
    refresh_token = models.CharField(_('Refresh_token'), max_length=128, unique=True)

    def hash_password(self, password):
        self.password_hash = pwd_context.encrypt(password)
        return self.password_hash

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)

    class Meta:
        verbose_name = _('User')
        verbose_name_plural = _('Users')

    def __unicode__(self):
        return self.username


class Player(TimeStampedModel):
    id = models.AutoField(primary_key=True)  # idcard, 1851100000
    user = models.OneToOneField(User)
    channel = models.ForeignKey(Channel, blank=True, null=True)  # 渠道
    model = models.CharField(_('Model'), max_length=50, blank=True)  # 设备号
    serial = models.CharField(_('Serial'), max_length=100, blank=True)  # 物理地址
    phone = models.CharField(_('Phone'), max_length=100, blank=True)  # 手机号
    nickname = models.CharField(_('Nickname'), max_length=30, blank=True, db_index=True)  # 昵称
    avatar = models.CharField(_('Avatar'), max_length=20, blank=True)  # 头像
    gold = models.PositiveIntegerField(_('Gold'), default=0)  # 金币
    rock = models.PositiveIntegerField(_('Rock'), default=0)  # 钻石
    star = models.PositiveIntegerField(_('Star'), default=0)  # 总星数
    point = models.PositiveIntegerField(_('Point'), default=0)  # 积分
    prods = models.TextField(_('Prods'), blank=True)  # 道具
    gates = models.TextField(_('Gates'), blank=True)  # 关卡
    mails = models.TextField(_('Mails'), blank=True)  # 邮件
    ips = models.TextField(_('Ips'), blank=True)  # 常用IP

    class Meta:
        verbose_name = _('Player')
        verbose_name_plural = _('Players')

    def __unicode__(self):
        return self.nickname


class Hp(TimeStampedModel):
    user = models.OneToOneField(User)
    hp = models.PositiveSmallIntegerField(_('Hp'), default=0)  # 体力
    timestamp = models.PositiveIntegerField(_('Timestamp'), default=0, blank=True)

    class Meta:
        verbose_name = _('Hp')
        verbose_name_plural = _('Hps')

    def __unicode__(self):
        return self.user


class UserGate(TimeStampedModel):
    user = models.OneToOneField(User)
    gate = models.OneToOneField(Gate)
    star = models.PositiveSmallIntegerField(_('Star'), default=0)  # 体力
    step = models.PositiveSmallIntegerField(_('Step'), default=0)  # 剩余步数

    class Meta:
        verbose_name = _('UserGate')
        verbose_name_plural = _('UserGates')

    def __unicode__(self):
        return '{}@{}'.format(self.user.id, self.gate.id)


class Mail(models.Model):
    MAIL = 0
    NOTICE = 1
    TYPES = (
        (MAIL, _('Mail')),
        (NOTICE, _('Notice')),
    )
    sender = models.CharField(_('Sender'), max_length=20, db_index=True)
    to = models.ForeignKey(User, blank=True, null=True)
    title = models.CharField(_('Title'), max_length=50)
    content = models.TextField(_('Content'), blank=True)
    awards = models.TextField(_('Json Awards'), blank=True)
    comment = models.TextField(_('Comment'), blank=True)
    type = models.PositiveSmallIntegerField(
        _('types'), choices=TYPES, default=MAIL)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _('Mail')
        verbose_name_plural = _('Mails')

    def save(self, *args, **kwargs):
        if self.jawards:
            try:
                escape.json_decode(self.jawards)
            except Exception:
                raise ValueError("JSON jawards format error")
        super(Mail, self).save(*args, **kwargs)

    def __unicode__(self):
        return self.title
