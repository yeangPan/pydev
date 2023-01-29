#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/14 12:19:21
@version    :1.0
@Description:
'''


import sqlite3
import pandas as pd
from ..logger import logger


class Client:

    def __init__(self, sid=None):
        # 以下两种连接方式，二选一（sid是服务名）
        self.logger = logger('database')

    def Create(self, sid, sql):
        '''
        Description:
        \t执行需要执行的查询，但是带id为objectid的结果
        Params:
        \tsql: 需要执行的sql语句
        Return:
        \tdf: 查询的结果，为Dataframe类型
        '''
        try:
            conn = sqlite3.connect(sid)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            self.logger.error(sql)
            self.logger.error(str(e))
        finally:
            cursor.close()

    def Insert(self, sid, sql):
        '''
        Description:
        \t执行需要执行的查询，但是带id为objectid的结果
        Params:
        \tsql: 需要执行的sql语句
        Return:
        \tdf: 查询的结果，为Dataframe类型
        '''
        try:
            conn = sqlite3.connect(sid)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            row = cursor.fetchall()
            title = [x[0] for x in cursor.description]
            return row, title
        except Exception as e:
            self.logger.error(sql)
            self.logger.error(str(e))
        finally:
            cursor.close()

    def Query(self, sid, sql):
        '''
        Description:
        \t执行需要执行的查询，但是带id为objectid的结果
        Params:
        \tsql: 需要执行的sql语句
        Return:
        \tdf: 查询的结果，为Dataframe类型
        '''
        try:
            conn = sqlite3.connect(sid)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            row = cursor.fetchall()
            title = [x[0] for x in cursor.description]
            return row, title
        except Exception as e:
            self.logger.error(sql)
            self.logger.error(str(e))
        finally:
            cursor.close()

    def CreateTable(self, sid, tableName, colNames, colTypes, primaryKey='id'):
        
        if not primaryKey in colNames:
            colNames = [primaryKey]+[colNames]
            colTypes = ['BLOB']+[colTypes]
        cols = []
        for colName, colType in zip(colNames, colTypes):
            if colName == primaryKey:
                cols.append(
                    '{} {} PRIMARY KEY NOT NULL'.format(colName, colType))
            else:
                cols.append('{} {}'.format(colName, colType))
        sql = """create table {}(\n""".format(tableName) + ',\n'.join(cols) + '\n);'
        return self.Create(sid=sid, sql=sql)

    def insertTable(self,sid, sql, param):
        '''
        Description: 
        \t新纪录导入
        Param : 
        \tsql: 需要执行的sql语句
        \tparam: 对应的param是一个tuple或者list
        Return: 
        \ttitile: 返回列名
        \teffectRow: sql影响的行数
        '''

        try:
            conn = sqlite3.connect(sid)
            cursor = conn.cursor()
            cursor.executemany(sql, param)
            conn.commit()
            success = True
        except Exception as e:
            self.logger.error(sql)
            self.logger.error(str(e))
            self.conn.rollback()
            success = False
        finally:
            return success

    def Tables(self,sid):
        '''
        Description: 
        \t建立数据库
        Return: 
        \tNone:
        '''
        sql = r"""select name from sqlite_master where type='table' order by name"""
        tables,name = self.Query(sid=sid, sql=sql)
        df = pd.DataFrame(data=tables,columns=name)
        return list(df.name)

    def CreateDatabase(self, sid):
        '''
        Description: 
        \t建立数据库
        Return: 
        \tNone:
        '''
        try:
            sqlite3.connect(sid)
            success = True
        except Exception as e:
            success = False
            self.logger.error(str(e))
        finally:
            return success

    def QueryByTableName(self, tbName):
        '''
        Description: 
        \t表格全表查询
        Param : 
        \ttbName: 表格名称
        Return: 
        \trow: 数据集
          title: 字段
        '''
        sql = 'select * from {}'.format(tbName)
        return self.Query(sql)

    def BoolParser(self, key, trueVal, falseVal):
        if isinstance(key, list):
            if isinstance(trueVal, list):
                return [trueVal[i] if key[i] else falseVal[i] for i in range(len(key))]
            else:
                return [trueVal if item else falseVal for item in key]
        else:
            return trueVal if key else falseVal


    def QueryByCols(self, tbName: str, cols: list, vals: list, sortby=None, ascending=True):
        """查询where column = value的记录

        :param tbName: table name
        :type tbName: str
        :param cols: columns 
        :type cols: str
        :param vals: values
        :type vals: list
        :param sortby: sortby values, defaults to None
        :type sortby: list, optional
        :param ascending: orders, defaults to True
        :type ascending: bool, optional
        :return: query result
        :rtype: tuple
        """
        OrderCond = ''
        if isinstance(sortby, str):
            orders = self.BoolParser(ascending, 'ASC', 'DESC')
            OrderCond = 'ORDER BY {} {}'.format(sortby, orders)
        elif isinstance(sortby, list):
            if isinstance(ascending, list):
                orders = self.BoolParser(ascending, 'ASC', 'DESC')
            else:
                orders = self.BoolParser(
                    [ascending]*len(sortby), 'ASC', 'DESC')
            subString = ','.join([sortby[i]+' '+orders[i]
                                    for i in range(len(sortby))])
            OrderCond = 'ORDER BY {}'.format(subString)
        cond = "and".join([" {}='{}' ".format(*x) for x in zip(cols, vals)])
        if cols:
            sql = "select * from {} where {} {}".format(
                tbName, cond, OrderCond)
        else:
            sql = "select * from {} {}".format(tbName, OrderCond)
        return self.Query(sql)

    
    def QueryTableRows(self, tbName):
        """query rows of the table 

        :param tbName: table name
        :type tbName: str
        :return: query result
        :rtype: tuple
        """
        sql = "select count(*) from {}".format(tbName)
        return self.Query(sql)

    
    """执行多行插入操作

        Args:
            tbName (str): 表格名称
            cols (list): 列名
            values (tuple): 列值(多行)
        """
    def InsertTable(self,sid, tbName: str, cols: list, values: list):
        """insert records to table

        :param sid: database
        :type sid: _type_
        :param tbName: _description_
        :type tbName: str
        :param cols: _description_
        :type cols: list
        :param values: _description_
        :type values: list
        :return: _description_
        :rtype: _type_
        """
        ss = ','.join(["?"]*len(cols))
        colNames = ",".join(['{}'.format(x) for x in cols])
        sql = "insert into {}({}) values({})".format(tbName, colNames, ss)
        return self.insertTable(sid, sql, values)
