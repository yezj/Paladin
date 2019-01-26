import requests
try:
	requests.post('http://front.kingdom.putaogame.com/syncdb/')
except Exception, e:
	print 'error>>', e