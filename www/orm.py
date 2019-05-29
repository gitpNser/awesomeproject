import aysncio, logging

import aiomysql

def log(sql, args=()):
	logging.info('SQL: %s' % sql)
	
