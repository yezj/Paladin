from front.wiapi import *
from front.handlers.base import BaseHandler
class ApiDocHandler(BaseHandler):
	
	def get(self):
		all_apis = api_manager.get_apis(name=self.get_argument('name', None),module=self.get_argument('module', None),handler=self.get_argument('handler', None))
		apis = {}
		for api in all_apis:
			if not apis.has_key(api.module):
				apis[api.module] = []
			apis[api.module].append(api)
		self.render('api_docs.html', **{'apis': apis, 'api_base':self.settings.get("api_base", ''), \
			'test_user_name': self.settings.get("test_user_name", '')})


class ApiMapHandler(BaseHandler):
	
	def get(self):
		all_apis = api_manager.get_apis(name=self.get_argument('name', None),module=self.get_argument('module', None),handler=self.get_argument('handler', None))
		apis = {}
		for api in all_apis:
			if not apis.has_key(api.module):
				apis[api.module] = []
			apis[api.module].append(api)
		self.render('api_map.html', **{'apis': apis, 'api_base':self.settings.get("api_base", ''),})

