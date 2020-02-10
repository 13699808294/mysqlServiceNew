import asyncio
import json
import time

import aiomysql
from tornado import web, gen

from setting.setting import DATABASES
from utils.agreement import RET
from utils.logClient import logClient
from utils.my_json import json_dumps

class BaseHanderView(web.RequestHandler):
    def set_default_headers(self) -> None:
        self.set_header('Content-Type','application/json;charset=UTF-8')

    def write_error(self, status_code: int, **kwargs) -> None:
        self.write(u"<h1>出错了</h1>")
        self.write(u'<p>{}</p>'.format(kwargs.get('error_title','')))
        self.write(u'<p>{}</p>'.format(kwargs.get('error_message','')))

    def initialize(self,**kwargs) -> None:
        self.server = kwargs.get('server')
        self.transactionDict = self.server.transactionDict
        self.ioloop = self.server.ioloop
        self.aioloop = self.server.aioloop
        self.mysql_pool_dict = self.server.mysql_pool_dict

    def prepare(self):
        if self.request.headers.get('Content-Type','').startswith('application/json'):
            # try:
            if self.request.body:
                self.json_dict = json.loads(self.request.body)
            # except:
            #     self.json_dict = None
        else:
            self.json_dict = None

    def on_finish(self) -> None:
        pass


    async def openTransaction(self,database):
        for DATABASE in DATABASES:
            if DATABASE['name'] == database:
                host = DATABASE.get('host')
                port = int(DATABASE.get('port'))
                user = DATABASE.get('user')
                db = DATABASE.get('name')
                password = DATABASE.get('password')
                break
        else:
            content = {
                'create_time': time.time(),
                'status': 1
            }
            return content
        conn = await aiomysql.connect(host=host,
                                      port=port,
                                      user=user,
                                      password=password,
                                      db=db,
                                      loop=asyncio.get_event_loop())
        cur = await conn.cursor()
        await cur.execute("BEGIN")  # 开启事务
        await logClient.asyncioDebugLog('开启事务')

        # await cur.execute("COMMIT")                         #提交事务

        # await cur.execute("ROLLBACK")                       #回滚
        # await cur.execute("SAVEPOINT identifier1")          #创建保存点
        # await cur.execute("RELEASE SAVEPOINT identifier1")  #删除保存点
        # await cur.execute("ROLLBACK TO identifier")         #回滚到保存点
        transaction_info = {
            'coon': conn,
            'cur': cur,
            'create_time': time.time()
        }
        point = str(time.time())
        self.transactionDict[point] = transaction_info
        content = {
            'point': point,
            'create_time': time.time(),
            'status': 0
        }
        await logClient.asyncioDebugLog('开启事务:({})'.format(point))
        return content

    async def commitTransaction(self,transaction_point):
        cur = self.transactionDict[transaction_point]['cur']
        conn = self.transactionDict[transaction_point]['coon']
        await cur.execute("COMMIT")
        await cur.close()
        conn.close()
        del self.transactionDict[transaction_point]
        content = {
            'create_time': time.time(),
            'status': 0
        }
        await logClient.asyncioDebugLog('提交事务:({})'.format(transaction_point))
        return content


    async def rollbackTransaction(self,transaction_point):
        if transaction_point in self.transactionDict.keys():
            cur = self.transactionDict[transaction_point]['cur']
            conn = self.transactionDict[transaction_point]['coon']
            await cur.execute("ROLLBACK")
            await cur.close()
            conn.close()
            del self.transactionDict[transaction_point]
            await logClient.asyncioDebugLog('提回滚事务:({})'.format(transaction_point))
        # todo：唯一查询


    async def selectOnly(self,model, data):
        database = data.get('database')
        # todo:查询的字段
        fields = data.get('fields')
        # todo：条件
        eq = data.get('eq')
        neq = data.get('neq')
        gt = data.get('gt')
        gte = data.get('gte')
        lt = data.get('lt')
        lte = data.get('lte')
        # ----------------------------------------------todo：拼接sql语句----------------------------------------------------#
        sqlCommand = 'select '
        # todo：添加查询字段
        if fields:
            sqlCommand += ', '.join([str(x) for x in fields])
        else:
            sqlCommand += '*'
        sqlCommand += ' from ' + model
        # todo：添加查询条件
        if eq or neq or gt or gte or lt or lte:
            conditionFlag = True
            sqlCommand += ' where '
        else:
            conditionFlag = False
        if eq:
            for k, v in eq.items():
                if v == None:
                    sqlCommand += '{} is null'.format(k)
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += k + ' in ' + '('
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                elif type(v) == dict:
                    sqlCommand += '{} = {}'.format(k, v['key'])
                else:
                    sqlCommand += '{} = '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if neq:
            for k, v in neq.items():
                if v == None:
                    sqlCommand += k + ' is not '
                    sqlCommand += 'null'
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} not in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} <> '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if gt:
            for k, v in gt.items():
                sqlCommand += k + ' > '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if gte:
            for k, v in gte.items():
                sqlCommand += k + ' >= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lt:
            for k, v in lt.items():
                sqlCommand += k + ' < '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lte:
            for k, v in lte.items():
                sqlCommand += k + ' <= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if conditionFlag:
            sqlCommand = sqlCommand[:-4]

        sqlCommand += ';'
        await logClient.asyncioDebugLog(sqlCommand)
        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        try:
            async with self.mysql_pool_dict[database].acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute(sqlCommand, ())
                    except Exception as e:
                        await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                        await cur.close()
                        return {'ret': RET.DBERR, 'errmsg': str(e)}
                    result = await cur.fetchall()
                    lenght = len(result)
                    await cur.close()
                    return {'ret': RET.OK, 'msg': result, 'lenght': lenght}
        except Exception as e:
            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
            return {'ret': RET.DBERR, 'errmsg': 'database key error'}

    async def selectOne(self,model, data):
        database = data.get('database')
        # todo:查询的字段
        fields = data.get('fields')
        # todo：条件
        eq = data.get('eq')
        neq = data.get('neq')
        gt = data.get('gt')
        gte = data.get('gte')
        lt = data.get('lt')
        lte = data.get('lte')
        # todo：排序字段(默认是升序,降序==DESC)
        sortInfo = data.get('sortInfo')
        # ----------------------------------------------todo：拼接sql语句----------------------------------------------------#
        sqlCommand = 'select '
        # todo：添加查询字段
        if fields:
            sqlCommand += ', '.join([str(x) for x in fields])
        else:
            sqlCommand += '*'
        sqlCommand += ' from ' + model

        # todo：添加查询条件
        if eq or neq or gt or gte or lt or lte:
            conditionFlag = True
            sqlCommand += ' where '
        else:
            conditionFlag = False
        if eq:
            for k, v in eq.items():
                if v == None:
                    sqlCommand += '{} is null'.format(k)
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += k + ' in ' + '('
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                elif type(v) == dict:
                    sqlCommand += '{} = {}'.format(k, v['key'])
                else:
                    sqlCommand += '{} = '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if neq:
            for k, v in neq.items():
                if v == None:
                    sqlCommand += k + ' is not '
                    sqlCommand += 'null'
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} not in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} <> '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if gt:
            for k, v in gt.items():
                sqlCommand += k + ' > '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if gte:
            for k, v in gte.items():
                sqlCommand += k + ' >= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lt:
            for k, v in lt.items():
                sqlCommand += k + ' < '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lte:
            for k, v in lte.items():
                sqlCommand += k + ' <= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if conditionFlag:
            sqlCommand = sqlCommand[:-4]

        # todo : 添加排序条件
        if sortInfo:
            sqlCommand += ' order by '
            for s in sortInfo:
                for k, v in s.items():
                    sqlCommand += k + ' ' + v
                    sqlCommand += ','
            else:
                sqlCommand = sqlCommand[:-1]
        sqlCommand += ';'
        await logClient.asyncioDebugLog(sqlCommand)
        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        try:
            async with self.mysql_pool_dict[database].acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute(sqlCommand, ())
                    except Exception as e:
                        await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                        await cur.close()
                        return {'ret': RET.DBERR, 'errmsg': str(e)}
                    result = await cur.fetchone()
                    await cur.close()
                    return {'ret': RET.OK, 'msg': result}
        except Exception as e:
            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
            return {'ret': RET.DBERR, 'errmsg': 'database key error'}

    # todo：查询所有
    async def selectAll(self,model, data):
        database = data.get('database')
        # todo:查询的字段
        fields = data.get('fields')
        # todo：条件
        eq = data.get('eq')
        neq = data.get('neq')
        gt = data.get('gt')
        gte = data.get('gte')
        lt = data.get('lt')
        lte = data.get('lte')

        # todo：排序字段(默认是升序,降序==DESC)
        sortInfo = data.get('sortInfo')

        # todo:获取分页信息
        pageInfo = data.get('pageInfo')
        if pageInfo != None:
            try:
                pageCount = int(pageInfo.get('pageCount', 20))  # 分页数,一页
                pageIndex = int(pageInfo.get('pageIndex', 1))  # 页数
            except:
                pageCount = 20
                pageIndex = 1
                pageInfo = None
        else:
            pageCount = 20
            pageIndex = 1
            pageInfo = None
        # ----------------------------------------------todo：拼接sql语句----------------------------------------------------#
        sqlCommand = 'select '
        # todo：添加查询字段
        if fields:
            sqlCommand += ', '.join([str(x) for x in fields])
        else:
            sqlCommand += '*'
        sqlCommand += ' from ' + model

        # todo：添加查询条件
        if eq or neq or gt or gte or lt or lte:
            conditionFlag = True
            sqlCommand += ' where '
        else:
            conditionFlag = False
        if eq:
            for k, v in eq.items():
                if v == None:
                    sqlCommand += '{} is null'.format(k)
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += '{},'.format(a)
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                elif type(v) == dict:
                    sqlCommand += '{} = {}'.format(k, v['key'])
                else:
                    sqlCommand += '{} = '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if neq:
            for k, v in neq.items():
                if v == None:
                    sqlCommand += k + ' is not '
                    sqlCommand += 'null'
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} not in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} <> '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if gt:
            for k, v in gt.items():
                sqlCommand += k + ' > '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if gte:
            for k, v in gte.items():
                sqlCommand += k + ' >= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lt:
            for k, v in lt.items():
                sqlCommand += k + ' < '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lte:
            for k, v in lte.items():
                sqlCommand += k + ' <= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if conditionFlag:
            sqlCommand = sqlCommand[:-4]

        # todo : 添加排序条件
        if sortInfo:
            sqlCommand += ' order by '
            for s in sortInfo:
                for k, v in s.items():
                    sqlCommand += k + ' ' + v
                    sqlCommand += ','
            else:
                sqlCommand = sqlCommand[:-1]
        # todo:分页参数
        if pageInfo != None:
            sqlCommand += ' limit '
            sqlCommand += str(pageCount * (pageIndex - 1)) + ',' + str(pageCount)
        sqlCommand += ';'
        await logClient.asyncioDebugLog(sqlCommand)
        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        try:
            async with self.mysql_pool_dict[database].acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:  # aiomysql.DictCursor        字典方式返回结果
                    try:
                        await cur.execute(sqlCommand, ())
                    except Exception as e:
                        await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                        await cur.close()
                        return {'ret': RET.DBERR, 'errmsg': str(e)}
                    result = await cur.fetchall()
                    await cur.close()
                    return {'ret': RET.OK, 'msg': result}
        except Exception as e:
            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
            return {'ret': RET.DBERR, 'errmsg': 'database key error'}

        # todo:新增

    async def insertOne(self,model, data):
        database = data.get('database')
        # 获取事务连接
        transactionPoint = data.get('transactionPoint')
        msg = data.get('msg')
        if not msg:
            return {'ret': RET.PARAMERR, 'msg': '没有任何参数'}
        # ----------------------------------------------todo：拼接sql语句----------------------------------------------------#
        sqlCommand = 'insert into {}'.format(model)
        sqlCommand += ' ('
        sqlCommand += ', '.join([str(x) for x in msg.keys()])
        sqlCommand += ') values ('
        for x in msg.values():
            if x == None:
                sqlCommand += 'null'
            else:
                if type(x) == str:
                    sqlCommand += '"'
                    sqlCommand += str(x)
                    sqlCommand += '"'
                else:
                    sqlCommand += str(x)
            sqlCommand += ','
        else:
            sqlCommand = sqlCommand[:-1]
        sqlCommand += ')'
        sqlCommand += ';'
        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        await logClient.asyncioDebugLog(sqlCommand)
        if transactionPoint:
            cur = self.transactionDict[transactionPoint]['cur']
            try:
                await cur.execute(sqlCommand, ())
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                await self.rollbackTransaction(transactionPoint)
                return {'ret': RET.DBERR}
            else:
                affected = cur.rowcount
                if affected == 1:
                    return {'ret': RET.OK}
                else:
                    return {'ret': RET.DBERR}
        else:
            try:
                async with self.mysql_pool_dict[database].acquire() as conn:
                    async with conn.cursor() as cur:
                        try:
                            await cur.execute(sqlCommand, ())
                        except Exception as e:
                            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                            await conn.rollback()
                            await cur.close()
                            return {'ret': RET.DBERR}
                        else:
                            affected = cur.rowcount
                            if affected == 1:
                                await cur.close()
                                return {'ret': RET.OK}
                            else:
                                await cur.close()
                                return {'ret': RET.DBERR}
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                return {'ret': RET.DBERR, 'errmsg': 'database key error'}

        # todo：添加 新增

    async def conditionInsertOne(self,model, data):
        database = data.get('database')
        # 获取事务连接
        transactionPoint = data.get('transactionPoint')
        msg = data.get('msg')
        fields = data.get('fields')
        # todo：条件
        eq = data.get('eq')
        neq = data.get('neq')
        gt = data.get('gt')
        gte = data.get('gte')
        lt = data.get('lt')
        lte = data.get('lte')
        if not msg:
            return {'ret': RET.PARAMERR, 'msg': '没有任何参数'}
        sqlCommand = 'insert into {} '.format(model)
        sqlCommand += ' ('
        sqlCommand += ', '.join([str(x) for x in msg.keys()])
        sqlCommand += ') select '
        for x in msg.values():
            if x == None:
                sqlCommand += 'null'
            else:
                if type(x) == str:
                    sqlCommand += '"'
                    sqlCommand += str(x)
                    sqlCommand += '"'
                else:
                    sqlCommand += str(x)
            sqlCommand += ','
        else:
            sqlCommand = sqlCommand[:-1]
        sqlCommand += ' from dual where not exists ( select {} from {}'.format(fields[0], model)
        # todo：添加查询条件
        if eq or neq or gt or gte or lt or lte:
            conditionFlag = True
            sqlCommand += ' where '
        else:
            conditionFlag = False
        if eq:
            for k, v in eq.items():
                if v == None:
                    sqlCommand += '{} is null'.format(k)
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += '{},'.format(a)
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                elif type(v) == dict:
                    sqlCommand += '{} = {}'.format(k, v['key'])
                else:
                    sqlCommand += '{} = '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if neq:
            for k, v in neq.items():
                if v == None:
                    sqlCommand += k + ' is not '
                    sqlCommand += 'null'
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} not in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} <> '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if gt:
            for k, v in gt.items():
                sqlCommand += k + ' > '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if gte:
            for k, v in gte.items():
                sqlCommand += k + ' >= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lt:
            for k, v in lt.items():
                sqlCommand += k + ' < '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lte:
            for k, v in lte.items():
                sqlCommand += k + ' <= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if conditionFlag:
            sqlCommand = sqlCommand[:-4]
        sqlCommand += ')'

        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        await logClient.asyncioDebugLog(sqlCommand)
        if transactionPoint:
            cur = self.transactionDict[transactionPoint]['cur']
            try:
                await cur.execute(sqlCommand, ())
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                await self.rollbackTransaction(transactionPoint)
                return {'ret': RET.DBERR}
            else:
                affected = cur.rowcount
                if affected == 1:
                    return {'ret': RET.OK}
                else:
                    return {'ret': RET.DBERR}
        else:
            try:
                async with self.mysql_pool_dict[database].acquire() as conn:
                    async with conn.cursor() as cur:
                        try:
                            await cur.execute(sqlCommand, ())
                        except Exception as e:
                            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                            await conn.rollback()
                            await cur.close()
                            return {'ret': RET.DBERR}
                        else:
                            affected = cur.rowcount
                            if affected == 1:
                                await cur.close()
                                return {'ret': RET.OK}
                            else:
                                await cur.close()
                                return {'ret': RET.DBERR}
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                return {'ret': RET.DBERR, 'errmsg': 'database key error'}
        # todo:修改

    async def updateMany(self,model, data):
        database = data.get('database')
        # 获取事务连接
        transactionPoint = data.get('transactionPoint')
        msg = data.get('msg')
        # todo：条件
        eq = data.get('eq')
        neq = data.get('neq')
        gt = data.get('gt')
        gte = data.get('gte')
        lt = data.get('lt')
        lte = data.get('lte')
        if not msg:
            return {'ret': RET.PARAMERR, 'msg': '没有任何参数'}
        # ----------------------------------------------todo：拼接sql语句----------------------------------------------------#
        sqlCommand = 'update {} set '.format(model)
        for k, v in msg.items():
            sqlCommand += '{} = '.format(k)
            if v == None:
                sqlCommand += 'null'
            else:
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
            sqlCommand += ','
        else:
            sqlCommand = sqlCommand[:-1]
        # todo：条件
        if eq or neq or gt or gte or lt or lte:
            conditionFlag = True
            sqlCommand += ' where '
        else:
            conditionFlag = False
        if eq:
            for k, v in eq.items():
                if v == None:
                    sqlCommand += '{} is null'.format(k)
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += k + ' in ' + '('
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} = '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if neq:
            for k, v in neq.items():
                if v == None:
                    sqlCommand += k + ' is not '
                    sqlCommand += 'null'
                elif type(v) == list:
                    if len(v) == 0:
                        continue
                    sqlCommand += '{} not in ('.format(k)
                    for a in v:
                        if type(a) == str:
                            sqlCommand += '"{}",'.format(a)
                        else:
                            sqlCommand += a + ','
                    else:
                        sqlCommand = sqlCommand[:-1]
                        sqlCommand += ')'
                else:
                    sqlCommand += '{} <> '.format(k)
                    if type(v) == str:
                        sqlCommand += '"{}"'.format(v)
                    else:
                        sqlCommand += str(v)
                sqlCommand += ' and '
        if gt:
            for k, v in gt.items():
                sqlCommand += k + ' > '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if gte:
            for k, v in gte.items():
                sqlCommand += k + ' >= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lt:
            for k, v in lt.items():
                sqlCommand += k + ' < '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if lte:
            for k, v in lte.items():
                sqlCommand += k + ' <= '
                if type(v) == str:
                    sqlCommand += '"{}"'.format(v)
                else:
                    sqlCommand += str(v)
                sqlCommand += ' and '
        if conditionFlag:
            sqlCommand = sqlCommand[:-4]
        sqlCommand += ';'
        # ----------------------------------------------todo：查询sql语句----------------------------------------------------#
        await logClient.asyncioDebugLog(sqlCommand)
        if transactionPoint:
            cur = self.transactionDict[transactionPoint]['cur']
            try:
                await cur.execute(sqlCommand, ())
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                await self.rollbackTransaction(transactionPoint)
                return {'ret': RET.DBERR}
            else:
                affected = cur.rowcount
                return {'ret': RET.OK, 'count': affected}
        else:
            try:
                async with self.mysql_pool_dict[database].acquire() as conn:
                    async with conn.cursor() as cur:
                        try:
                            await cur.execute(sqlCommand, ())
                        except Exception as e:
                            await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                            await conn.rollback()
                            await cur.close()
                            return {'ret': RET.DBERR}
                        else:
                            affected = cur.rowcount
                            await cur.close()
                            return {'ret': RET.OK, 'count': affected}
            except Exception as e:
                await logClient.asyncioErrorLog(str(e)+' {} {}'.format(database,sqlCommand))
                return {'ret': RET.DBERR, 'errmsg': 'database key error'}

        # todo：sql语句查询一个

    async def sqlFetchone(self,db,sql, args=()):
        """封装select，查询单个，返回数据为字典"""
        await logClient.asyncioDebugLog(sql + args)
        async with self.mysql_pool_dict[db].acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                rs = await cur.fetchone()
                await cur.close()
                return rs

        # todo：sql语句查询所有

    async def sqlSelect(self,db,sql, args=(), size=None):
        """封装select，查询多个，返回数据为列表"""
        await logClient.asyncioDebugLog(sql + args)
        async with self.mysql_pool_dict[db].acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                if size:
                    rs = await cur.fetchmany(size)
                else:
                    rs = await cur.fetchall()
                await cur.close()
                return rs

        # todo：sql语句增,修改,删除


    async def sqlExecute(self,db,sql, args=()):
        """封装insert, delete, update"""
        await logClient.asyncioDebugLog(sql + args)
        
        async with self.mysql_pool_dict[db].acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(sql, args)
                except BaseException:
                    await conn.rollback()
                    await cur.close()
                    return
                else:
                    affected = cur.rowcount
                    await cur.close()
                    return affected



class TransactionView(BaseHanderView):

    async def get(self):
        database = self.get_query_argument('database')
        transactionInfo = await self.openTransaction(database)
        self.write(json_dumps(transactionInfo))

    async def put(self):
        try:
            data = json.loads(self.request.body)
            transaction_point = data.get('transaction_point')
            transactionInfo = await self.commitTransaction(transaction_point)
        except:
            transactionInfo = {'ret': RET.PARAMERR}
        self.write(json_dumps(transactionInfo))

    async def delete(self):
        try:
            data = json.loads(self.request.body)
            transaction_point = data.get('transaction_point')
            transactionInfo = await self.rollbackTransaction(transaction_point)
        except:
            transactionInfo = {'ret': RET.PARAMERR}
        self.write(json_dumps(transactionInfo))

class selectOneView(BaseHanderView):
    async def post(self,mode):
        #todo：获取参数
        try:
            data = json.loads(self.request.body)
            result = await self.selectOne(mode, data)
        except: result = {'ret':RET.PARAMERR}
        self.write(json_dumps(result))

class selectOnlyView(BaseHanderView):
    async def post(self,mode):
        #todo:获取参数
        try:
            data = json.loads(self.request.body)
            result = await self.selectOnly(mode, data)
        except: result = {'ret':RET.PARAMERR}
        self.write(json_dumps(result))

class selectAllView(BaseHanderView):
    async def post(self,mode):
        #todo:获取参数
        try:
            data = json.loads(self.request.body)
            result = await self.selectAll(mode, data)
        except: result = {'ret':RET.PARAMERR}
        self.write(json_dumps(result))

class insertOneView(BaseHanderView):
    async def post(self,mode):
        # todo:获取参数
        try:
            data = json.loads(self.request.body)
            result = await self.insertOne(mode, data)
        except: result = {'ret':RET.PARAMERR}
        self.write(json_dumps(result))

class ConditionInsertOneView(BaseHanderView):
    async def post(self,mode):
        try:
            data = json.loads(self.request.body)
            result = await self.conditionInsertOne(mode, data)
        except:
            result = {'ret': RET.PARAMERR, }
        self.write(json_dumps(result))

class updateManyView(BaseHanderView):
    async def post(self,mode):
        # todo:获取参数
        try:
            data = json.loads(self.request.body)
            result = await self.updateMany(mode, data)
        except:
            result = {'ret': RET.PARAMERR, }
        self.write(json_dumps(result))

class sqlFetchoneView(BaseHanderView):
    async def post(self):
        data = await self.request.json()
        sql = data.get('sql')
        args = data.get('args')
        result = await self.sqlFetchone(sql,args)
        self.write(json_dumps(result))
class sqlSelectView(BaseHanderView):
    async def post(self):
        data = await self.request.json()
        sql = data.get('sql')
        args = data.get('args')
        size = data.get('size')
        result = await self.sqlSelect(sql,args,size)
        self.write(json_dumps(result))

class sqlExecuteView(BaseHanderView):
    async def post(self):
        data = await self.request.json()
        sql = data.get('sql')
        args = data.get('args')
        result = await self.sqlSelect(sql,args)
        self.write(json_dumps(result))

class IndexView(BaseHanderView):
    @gen.coroutine
    def get(self,*args,**kwargs):
        self.write('error')
        return

    @gen.coroutine
    def post(self, *args, **kwargs):
        self.write('error')
        return

    @gen.coroutine
    def put(self, *args, **kwargs):
        self.write('error')
        return
    @gen.coroutine
    def delete(self, *args, **kwargs):
        self.write('error')
        return
        # self.ioloop.add_timeout(self.ioloop.time() + 5, self.test_func)
        # subject = self.get_query_argument('subject')
        # print(subject)
        # subject = self.get_query_arguments('subject')
        # print(subject)
        # subject = self.get_body_argument('subject')
        # print(subject)
        # subject = self.get_body_arguments('subject')title
        # print(subject)
        # subject = self.get_argument('subject')
        # print(subject)
        # subject = self.get_arguments('subject')
        # print(subject)
        # s = self.request.arguments
        # s = self.request.body
        # s1 = json.loads(s)
        # s = self.request.body_arguments
        # s = self.request.connection
        # s = self.request.files
        # for name, file_list in s.items():
        #     for file_object in file_list:
        #         filename = file_object['filename']
        #         body = file_object['body']
        #         content_type = file_object['content_type']
        #         with open(filename, 'ab') as f:
        #             f.write(body)
        # s = self.request.headers
        # s = self.request.cookies
        # s = self.request.full_url()
        # # s = self.request.get_ssl_certificate(binary_form=False)
        # s = self.request.host
        # s = self.request.host_name
        # s = self.request.method
        # s = self.request.path
        # s = self.request.protocol
        # s = self.request.query
        # s = self.request.query_arguments
        # s = self.request.request_time()
        # s = self.request.version
        # s = self.request.remote_ip
        # s = self.request.server_connection
        # s = self.request.uri
        # info = {
        #     'name': 'pancunli',
        #     'age': 18
        # }
        # self.write(info)
        # self.set_header('Content-Type','application/json;charset=UTF-8')
        # self.write(json.dumps(info))
        # self.redirect('https://www.runoob.com/http/http-status-codes.html')     #重定向
        # error_info = {
        #     'error_title':'服务器崩溃了',
        #     'error_message':'服务器累了,现在不能提供服务'
        # }
        # self.send_error(**error_info)
        # self.render('index.html')
        # self.write('hello world')
        self.write('error')
