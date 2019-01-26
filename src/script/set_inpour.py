
# -*- coding: utf-8 -*-
#!python -m json.tool
import psycopg2
DATA_HOST = '127.0.0.1'
DATA_NAME = 'ptkingdom'
DATA_USER = 'deploy'
DATA_PASSWORD = '4wxto.kNAE.cujML97'
__author__ = 'yezijian'
ZID = '12'
conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port="5432")
cur = conn.cursor()

with open('productpay.txt') as f:
	for one in f.readlines():
		#print one.strip().split('\t')
		zid, uid = one.strip().split('\t')
		#print zid, uid
		if ZID == zid:
			#print uid
			sql = "UPDATE core_user SET rock=rock+2400, vrock=vrock+1200 WHERE id=%s RETURNING id" % (uid, )
			cur.execute(sql)
			res = "UPDATE core_userinpour SET rock=rock+1200 where user_id=%s RETURNING id" % (uid, )
			cur.execute(res)
conn.commit()
conn.close()

	

