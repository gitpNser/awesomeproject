import asyncio, logging

import aiomysql

def log(sql, args=()):
	logging.info('SQL: %s' % sql)
	
async def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host', 'localhost'),	# MYSQL 服务器地址
		port=kw.get('port', 3306),	# MYSQL 服务器端口
		user=kw['user'],	# 数据库用户名
		password=kw['password'],	# 数据库用户密码
		db=kw['db'],				# 数据库名称 
		charset=kw.get('charset', 'utf8mb4'),	# 数据库Coding方式
		autocommit=kw.get('autocommit', True),	# 自动提交？190530
		maxsize=kw.get('maxsize', 10),	# 最大连接数 ？190530
		minsize=kw.get('minsize', 1),	# 最小连接数 ？190530
		loop=loop
	)

async def select(sql, args, size=None):	# 创建Select函数
	log(sql, args)
	global __pool
	async with __pool.get() as conn:	#链接数据库
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.replace('?', '%s'), args or ())	#MYSQL 命令行参数？ 和 %s 自由切换
			if size:	#如果函数调用时带入size参数就获取指定数量记录
				rs = await cur.fetchmany(size)
			else:
				rs = await cur.fetchall()	# 否则获取所有记录
		logging.info('rows returned: %s' % len(rs))
		return rs
		
async def execute(sql, args, autocommit=True):	# 定义一个执行参数，可以执行Insert, Update和Delete语句
	log(sql)						# 自己添加了args参数 190530, 删除args参数190618
	async with __pool.get() as conn:	#  链接数据库
		if not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?', '%s'), args)
				affected = cur.rowcount		# 获取需求的行数
			if not autocommit:
				await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()		# 如果出错就回滚？ 190530
			raise
		return affected
		
def create_args_string(num):	# 生成SQL 语句参数？ 190530
	L = []
	for n in range(num):
		L.append('?')			# 增加几个问号 ？ 190530
	return ', '.join(L)
	
class Field(object):		# 定义一个“列”对象 ？ 由名称、列类型， 是否主键，defaut形参？ 190530 

	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default
		
	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):	# 定义String类型 列

	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)
		
class BooleanField(Field):	# 定义布尔类型 列

	def __init__(self, name=None, default=False):
		super().__init__(name, 'boolean', False, default)

class IntegerField(Field):	# 定义一个整数类型 列

	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):	# 定义一个浮点类型 列
	
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)

class TextField(Field):		# 定义文本类类型 列
	
	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)
		
class ModelMetaclass(type):

	def __new__(cls, name, bases, attrs):
		if name == 'Model':		# 排除 Model 类
			return type.__new__(cls, name, bases, attrs)
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		mappings = dict()
		fields = []
		primaryKey = None
		for k, v in attrs.items():
			if isinstance(v, Field):
				logging.info('  found mapping: %s ==> %s' % (k, v))
				mappings[k] = v
				if v.primary_key:
					if primaryKey:
						raise StandardError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					fields.append(k)
		if not primaryKey:
			raise StandardError('Primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings	#  保存属性和列的映射关系
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primaryKey	# 主键属性名
		attrs['__fields__'] = fields	# 除主键外的属性名
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		#	attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)
	
class Model(dict, metaclass=ModelMetaclass):
 
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)
		
	def __getattr__(self, key):
		try:
			return self[key]	# [] 错用成（），190618修正
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)
		
	def __setattr__(self, key, value):
		self[key] = value
	
	def getValue(self, key):
		return getattr(self, key, None)
		
	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value
		
	@classmethod
	async def findAll(cls, where=None, args=None, **kw):		# find objects by where clause:
		sql = [cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args = []
		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				sql.append('?, ?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limie value: %s' % str(limit))
		rs = await select(' '.join(sql), args)
		return [cls(**r) for r in rs]	# 输出查询结果， 结果是个dict? 190530
		
	@classmethod
	async def findNumber(cls, selectField, where=None, args=None):		# find number by select and where
		sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs = await select(' '.join(sql), args, 1)
		if len(rs) == 0:
			return None
		return rs[0]['_num_']	#输出结果， 结果是个dict? 19530
		
	@classmethod
	async def find(cls, pk):	# find by primary key
		rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])
		
	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__insert__, args)
		if rows != 1:
			logging.warn('fail to insert record: affected rows: %s' % rows)
		
	async def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = await execute(self.__update__, args)
		if rows != 1:
			logging.warn('fail to update by primary key : affcted rows: %s' % rows)
			
	async def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = await execute(self.__delete__, args)
		if rows != 1:
			logging.warn('fail to remove by primary key: affected rows: %s' % rows)

		
	
	