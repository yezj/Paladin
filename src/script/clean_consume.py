# -*- coding: utf-8 -*-
#!python -m json.tool

import psycopg2
DATA_HOST = '127.0.0.1'
DATA_NAME = 'ptkingdom'
DATA_USER = 'deploy'
DATA_PASSWORD = '4wxto.kNAE.cujML97'
DATA_PORT = '5432'
__author__ = 'yezijian'

conn = psycopg2.connect(database=DATA_NAME, user=DATA_USER, password=DATA_PASSWORD, host=DATA_HOST, port=DATA_PORT)

query = "DELETE FROM core_userconsume"
cur = conn.cursor()
cur.execute(query)
conn.commit()

conn.close()