import asyncio
import os
import time

import aiomysql
import tornado
import uvloop
from tornado import httpserver
from tornado import web
from tornado.options import options
from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.web import StaticFileHandler

from apps.mysql.views import TransactionView, selectOneView, selectOnlyView, selectAllView, insertOneView, \
    ConditionInsertOneView, updateManyView, IndexView
from setting.setting import DEBUG, DATABASES
from utils.logClient import logClient

tornado.options.define('port', type=int, default=8002, help='服务器端口号')


class HttpService():
    def __init__(self,ioloop = None,aioloop = None):
        self.ioloop = ioloop
        self.aioloop = aioloop
        self.transactionDict = {}       #事务保持链接对象
        self.mysql_pool_dict = {}       #数据库链接池对象
        self.aioloop.run_until_complete(self.create_pool(self.aioloop))
        self.urlpatterns = [
            (r'/transaction', TransactionView, {'server': self}),
            (r'/selectOne/(.*)', selectOneView, {'server': self}),
            (r'/selectOnly/(.*)', selectOnlyView, {'server': self}),
            (r'/selectAll/(.*)', selectAllView, {'server': self}),
            (r'/insertOne/(.*)', insertOneView, {'server': self}),
            (r'/conditionInsertOne/(.*)', ConditionInsertOneView, {'server': self}),
            (r'/updateMany/(.*)', updateManyView, {'server': self}),
            # (r'/', IndexView, {'server': self}),
        ]

        app = web.Application(self.urlpatterns,
                              debug=DEBUG,
                              # autoreload=True,
                              # compiled_template_cache=False,
                              # static_hash_cache=False,
                              # serve_traceback=True,
                              static_path = os.path.join(os.path.dirname(__file__),'static'),
                              template_path = os.path.join(os.path.dirname(__file__),'template'),
                              autoescape=None,  # 全局关闭模板转义功能
                                      )
        http_setver = httpserver.HTTPServer(app)
        http_setver.listen(options.port)
        self.aioloop.call_later(1, self.timeout)

    async def timeoutRollbackTransaction(self,point):
        cur = self.transactionDict[point]['cur']
        conn = self.transactionDict[point]['coon']
        await cur.execute("ROLLBACK")
        await cur.close()
        conn.close()
        del self.transactionDict[point]
        await logClient.asyncioDebugLog('提回滚事务:({})'.format(point))

    def timeout(self):
        now_time = time.time()
        for point, info in self.transactionDict.items():
            if now_time - info['create_time'] >= 30:
                self.aioloop.create_task(self.timeoutRollbackTransaction(point))
        self.aioloop.call_later(1, self.timeout)

    # 创建连接池对象
    async def create_pool(self,loop, **kw):
        """定义mysql全局连接池"""
        for DATABASE in DATABASES:
            host = DATABASE['host']
            port = int(DATABASE['port'])
            user = DATABASE['user']
            password = DATABASE['password']
            db = DATABASE['name']
            _mysql_pool = await aiomysql.create_pool(host=host,
                                                     port=port,
                                                     user=user,
                                                     password=password,
                                                     db=db,
                                                     loop=loop,
                                                     charset=kw.get('charset', 'utf8'),
                                                     autocommit=kw.get('autocommit', True),
                                                     maxsize=kw.get('maxsize', 24),
                                                     minsize=kw.get('minsize', 1),
                                                     connect_timeout=10)
            self.mysql_pool_dict[db] = _mysql_pool