import cyclone.web
from cyclone.web import HTTPError
# from django.conf import settings
from back import settings
import os
import simplejson

os.environ['DJANGO_SETTINGS_MODULE'] = 'back.settings'
from django.core.serializers.json import DjangoJSONEncoder
from django.utils.encoding import smart_unicode

import hashlib, urllib

import tempfile
import datetime
import decimal
import functools
import logging
from UserDict import UserDict
# from checkword import checkword

from django.db.models.query import QuerySet
from django.core.exceptions import ObjectDoesNotExist as DoesNotExist, MultipleObjectsReturned
from django.core.exceptions import ValidationError

from django.core.cache import cache as user_status_cache
from django.core.files.uploadedfile import TemporaryUploadedFile, InMemoryUploadedFile


class objid(str):
    pass


class dbid(str):
    pass


class ExampleParam(dict):
    def __init__(self, parent, name):
        self['_parent'] = parent
        self['_name'] = name

    def __getattr__(self, name):
        if not self.has_key(name):
            self[name] = ExampleParam(self, name)
        return self[name]

    def __str__(self):
        if self['_parent']:
            return '%s.%s' % (self['_parent'], self['_name'])
        else:
            return self['_name']

    def print_tree(self, pre=''):
        ps = ['%s%s%s' % (pre, k, (lambda vp: vp and ':\r\n%s' % vp or '')(v.print_tree(pre + '\t'))) for k, v in
              self.items() if k not in ('_parent', '_name')]
        if ps:
            return "%s" % '\r\n'.join(ps)
        else:
            return ""


ex = ExampleParam({}, 'ex')


class ApiDefined(UserDict):
    def __init__(self, name, method, uri, params=[], result=None, need_login=False, need_appkey=False, handler=None,
                 module=None, filters=[], description=''):
        UserDict.__init__(self)
        self['name'] = name
        self['method'] = method
        self['module'] = module
        self['uri'] = uri
        self['handler'] = handler
        self['params'] = params
        self['result'] = result
        self['need_login'] = need_login
        self['need_appkey'] = need_appkey
        self['filters'] = filters
        self['description'] = description

    def get_handler_name(self):
        return self['handler'].__name__

    def doc(self):
        d = '%s\n%s %s' % (self['name'], self['method'], self['uri'])
        d = d + '\nname\trequired\ttype\tdefault\texample\t\tdesc'
        d = d + '\n------------------------------------------------'
        for p in self['params']:
            d = d + '\n%s\t%s\t%s\t%s\t%s\t%s' % (
            p.name, p.required, p.param_type.__name__, p.default, p.display_example(), p.description)
        if self['result']:
            d = d + '\nResult:\n%s' % self['result']
        return d

    def __getattr__(self, name):
        try:
            return self[name]
        except Exception:
            return ""


class Param(UserDict):
    def __init__(self, name, required=False, param_type=str, default=None, example=None, description="", hidden=False):
        UserDict.__init__(self)
        self['name'] = name
        self['required'] = required
        self['param_type'] = param_type
        self['default'] = default
        self['example'] = example
        self['description'] = description
        self['hidden'] = hidden

    def display_type(self, _t=None):
        _t = _t or self['param_type']
        if type(_t) in (list, tuple) and _t:
            return '[%s,..]' % self.display_type(_t[0])
        return _t.__name__

    def display_example(self):
        if self['hidden']: return ''
        if self['param_type'] is bool:
            return self['example'] and 'true' or 'false'
        else:
            return str(self['example'])

    def html_example(self):
        if self['hidden']: return ''

        if type(self['example']) is ExampleParam:
            return '<input type="text" class="example_input" name="%s" value=""><a class="example_value" val="%s">E</a>' \
                   % (self['name'], str(self['example']))
        if self['param_type'] is file:
            return '<input name="%s" type="file"/>' % self['name']
        if self['param_type'] is bool:
            return '<select name="%s"><option value="true"%s>True</option><option value="false"%s>False</option></select>' % \
                   (self['name'], self['example'] and ' selected' or '', (not self['example']) and ' selected' or '')
        elif self['param_type'] in (str, int, float):
            if type(self['example']) in (list, tuple):
                return '<select name="%s">%s</select>' % (
                self['name'], ''.join(['<option value="%s">%s</option>' % (v, v) for v in self['example']]))
        return self['name'], str(self['example'])

    def __getattr__(self, name):
        try:
            return self[name]
        except Exception:
            return ""


class ApiHolder(object):
    apis = []

    def __init__(self):
        pass

    def addapi(self, api):
        api['id'] = len(self.apis) + 1
        self.apis.append(api)

    def get_apis(self, name=None, module=None, handler=None):
        all_apis = self.apis
        if name:
            name = name.replace(' ', '_').lower()
            all_apis = filter(lambda api: api.name.lower().replace(' ', '_') == name, all_apis)
        if module:
            all_apis = filter(lambda api: api['module'] == module, all_apis)
        if handler:
            handler = handler.lower()
            all_apis = filter(lambda api: api['handler'].__name__.lower() == handler or api[
                                                                                            'handler'].__name__.lower() == '%shandler' % handler,
                              all_apis)
        return all_apis

    def get_urls(self):
        urls = {}
        for api in self.apis:
            if not urls.has_key(api['uri']):
                urls[api['uri']] = api['handler']
        return [(r'%s$' % uri, handler) for uri, handler in urls.items()]


api_manager = ApiHolder()


def api(name, uri, params=[], result=None, filters=[], description=''):
    def wrap(method):
        if not hasattr(method, 'apis'):
            setattr(method, 'apis', [])
        getattr(method, 'apis').append(
            ApiDefined(name, method.__name__.upper(), uri, params, result, module=method.__module__, filters=filters,
                       description=description))
        return method

    return wrap


def handler(cls):
    for m in [getattr(cls, i) for i in dir(cls) if callable(getattr(cls, i)) and hasattr(getattr(cls, i), 'apis')]:
        method_filters = getattr(m, 'api_filters', None)
        for api in m.apis:
            api['handler'] = cls
            if method_filters:
                for f in method_filters:
                    f(api)
            if api['filters']:
                for f in api['filters']:
                    f(api)
            api_manager.addapi(api)
    return cls


def ps_filter(api):
    api.params.extend(
        [Param('start', False, int, 0, 0, 'Data Start'), Param('count', False, int, 25, 25, 'Data Count')])


class ApiJSONEncoder(DjangoJSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return str(o)
        # return dt2ut(o)
        elif isinstance(o, decimal.Decimal):
            return str(o)
        else:
            try:
                return super(ApiJSONEncoder, self).default(o)
            except Exception:
                return smart_unicode(o)
