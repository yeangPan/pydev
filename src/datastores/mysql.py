#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 15:25:53
@version    :1.0
@Description:
'''

import pymysql
from .base import SqlClient
from ..constants import Comparator
from ..exceptions import DatabaseError
from ..exceptions import InvaliInputError
from ..exceptions import DatabaseInputError
from ..exceptions import DatabaseRuntimeError


class MySQL(SqlClient):
    def __init__(self, host: str, user: str, passwd: str, port: str, database: str):
        # 以下两种连接方式，二选一（sid是服务名）
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.database = database

    def __str__(self) -> str:
        return 'host:{},user:{},passwd:{},port:{},database:{}'.format(self.host, self.user, self.passwd, self.port, self.database)

    def connection(self):
        """build database connection

        :raises DatabaseError: database error
        :return: database connection
        :rtype: mysql connection
        """
        try:
            connect = pymysql.connect(
                host=self.host, user=self.user, password=self.passwd, database=self.database, port=self.port)
            return connect

        except Exception as e:
            raise DatabaseError(
                'connect database error msg:\n {}\n with database config:\n{}'.format(str(e), self.__str__()))

    def check_db(self):
        return super().check_db()

    def check_tb(self, tbname: str):
        return super().check_tb(tbname)

    def query(self, sql: str):
        """execute query sql and return result

        :param sql: sql query string
        :type sql: sql
        :raises str: exeception
        :return: query result
        :rtype: tuple
        """
        try:
            connect = self.connection()
            cursor = connect.cursor()
            cursor.execute(sql)
            connect.commit()
            row = cursor.fetchall()
            title = [x[0] for x in cursor.description]
            connect.close()
            return title, list(row)
        except Exception as e:
            connect.close()
            raise DatabaseRuntimeError(
                'database runtime error msg:{}\n with sql:{}\n'.format(str(e), sql))

    def insert(self, sql, values):
        """execute insert insert sql

        :param sql: sql insert string
        :type sql: sql
        :param values: insert values
        :type values: list
        :raises str: exeception
        """
        try:
            connect = self.connection()
            cursor = connect.cursor()
            cursor.execute(sql, values)
            connect.commit()
            connect.close()
        except Exception as e:
            connect.close()
            connect.rollback()
            raise DatabaseRuntimeError(
                'database runtime error msg:{}\n with sql:{}\n'.format(str(e), sql))

    def update(self, sql):
        """execute update sql

        :param sql: sql update string
        :type sql: sql
        :raises str: exeception
        """
        try:
            connect = self.connection()
            cursor = connect.cursor()
            cursor.execute(sql)
            connect.commit()
            connect.close()
        except Exception as e:
            connect.close()
            connect.rollback()
            raise DatabaseRuntimeError(
                'database runtime error msg:{}\n with sql:{}\n'.format(str(e), sql))

    def delete(self, sql):
        """execute delete sql

        :param sql: sql delete string
        :type sql: sql
        :raises str: exeception
        """
        try:
            connect = self.connection()
            cursor = connect.cursor()
            cursor.execute(sql)
            connect.commit()
            connect.close()
        except Exception as e:
            connect.close()
            connect.rollback()
            raise DatabaseRuntimeError(
                'database runtime error msg:{}\n with sql:{}\n'.format(str(e), sql))

    def insert_many(self, sql: str, params):
        """execute insert insert sql in batch insert mode

        :param sql: sql insert string
        :type sql: str
        :param params: insert values
        :type params: iterable
        :raises DatabaseRuntimeError: runtime exception
        """
        try:
            connect = self.connection()
            cursor = connect.cursor()
            cursor.executemany(sql, params)
            connect.commit()
            connect.close()
        except Exception as e:
            connect.close()
            connect.rollback()
            raise DatabaseRuntimeError(
                'database runtime error msg:{}\n with sql:{}\n'.format(str(e), sql))

    def query_records(self, tbname: str):
        sql = "select * from {};".format(tbname)
        return self.query(sql=sql)

    def bool_parser(self, key, true_val, false_val):
        """convertor for true and false value

        :param key: key
        :type key: str
        :param true_val: true values
        :type true_val: any
        :param false_val: false values
        :type false_val: any
        :return: values
        :rtype: any
        """
        if isinstance(key, list):
            if isinstance(true_val, list):
                return [true_val[i] if key[i] else false_val[i] for i in range(len(key))]
            else:
                return [true_val if item else false_val for item in key]
        else:
            return true_val if key else false_val

    def query_by_cols(self, tbname: str, cols, vals, sortby=None, ascending=True):
        """query by cols values

        :param tbname: table name
        :type tbname: str
        :param cols: columns names
        :type cols: list or str
        :param vals: condition values
        :type vals: list or any
        :param sortby: if sorted, defaults to None
        :type sortby: sorting, optional
        :param ascending: whether sorting by ascending, defaults to True
        :type ascending: bool, optional
        :return: records that satisfies with the condition
        :rtype: tuple (columns, data)
        """
        order_condition = ''
        if sortby:
            if isinstance(sortby, str):
                orders = self.bool_parser(ascending, 'ASC', 'DESC')
                order_condition = 'ORDER BY {} {}'.format(sortby, orders)
            elif isinstance(sortby, list):
                if isinstance(ascending, list):
                    orders = self.bool_parser(ascending, 'ASC', 'DESC')
                else:
                    orders = self.bool_parser(
                        [ascending]*len(sortby), 'ASC', 'DESC')
                subString = ','.join([sortby[i]+' '+orders[i]
                                     for i in range(len(sortby))])
                order_condition = 'ORDER BY {}'.format(subString)
            else:
                raise InvaliInputError(
                    'pls make sure the sortby is str | list, sortby is type {} with value {}', type(sortby), sortby)
        if (isinstance(cols, list) & isinstance(cols, list) & (len(cols) == len(vals))):
            zipped = zip(cols, vals)
        else:
            zipped = zip([cols], [vals])
        cond = "and".join([" {}='{}' ".format(x[0], x[1]) for x in zipped])
        if cols:
            sql = "select * from {} where {} {}".format(
                tbname, cond, order_condition)
        else:
            sql = "select * from {} {}".format(tbname, order_condition)
        return self.query(sql)

    def query_by_single_col_interval(self, tbname: str,
                                     colname: str,
                                     low_bound: any,
                                     up_bound: any,
                                     low_cmptor=Comparator.BIGGER_OR_EQUAL,
                                     up_cmptor=Comparator.SMALLER_OR_EQUAL,
                                     with_nan: bool = False):
        """query table by single interval values

        :param tbname: target table name
        :type tbname: str
        :param colname: column name
        :type colname: str
        :param low_bound: lower boundary for query
        :type low_bound: any
        :param up_bound: upper boundary for query
        :type up_bound: any
        :param with_nan: column value can be null, defaults to False
        :type with_nan: bool, optional
        """

        if (isinstance(up_bound, type(None))) & (not isinstance(low_bound, type(None))):
            sql = """
                    select * from {} where `{}`{}{};
                  """.format(tbname, colname, low_cmptor.value, low_bound)
        elif (not isinstance(up_bound, type(None))) & (isinstance(low_bound, type(None))):
            sql = """
                    select * from {} where `{}`{}{};
                  """.format(tbname, colname, up_cmptor.value, up_bound)
        elif (not isinstance(low_bound, type(None))) | (not isinstance(up_bound, type(None))):
            sql = """
                    select * from {} where `{}`{}{} and `{}`{}{};
                  """.format(tbname, colname, low_cmptor.value, low_bound, colname, up_cmptor.value, up_bound)
        else:
            raise DatabaseInputError(
                'at least set one boundary: low_bound or up_bound as input param')
        return self.query(sql)

    def delete_by_cols(self, tbname: str, cols: str, vals: list):
        """delete records by cols values

        :param tbname: table name
        :type tbname: str
        :param cols: columns
        :type cols: str
        :param vals: values corresponding to columns
        :type vals: list
        """
        if cols:
            zipped = zip(cols, vals)
            cond = "and".join([" {}='{}' ".format(x[0], x[1]) for x in zipped])
            sql = "delete from {} where {}".format(tbname, cond)
            self.delete(sql)
        else:
            raise DatabaseInputError('input param: cols is none')

    def insert_record(self, tbname: str, cols: list, values: list):
        """insert record into 

        :param tbname: target table name
        :type tbname: str
        :param cols: column names
        :type cols: list
        :param values: values corresponding to columns
        :type values: list
        """
        ss = ','.join(["%s"]*len(cols))
        colNames = ",".join(['`{}`'.format(x) for x in cols])
        sql = "insert into {}({}) values({})".format(tbname, colNames, ss)
        return self.insert(sql, values)

    def update_cols(self, tbname, scols, svals, ccols, cvals):
        """update table 

        :param tbname: table name
        :type tbname: str
        :param scols: set columns
        :type scols: list
        :param svals: set values
        :type svals: list
        :param ccols: condition columns
        :type ccols: list
        :param cvals: condition values
        :type cvals: list
        """
        zipped = zip(scols, svals)
        sval = ",".join([" `{}`='{}' ".format(x[0], x[1]) for x in zipped])
        zipped = zip(ccols, cvals)
        cond = "and".join([" `{}`='{}' ".format(x[0], x[1]) for x in zipped])
        sql = "update {} set {} where {}".format(tbname, sval, cond)
        return self.update(sql=sql)

    def insert_update(self, tbname: str, cols: list, values: list, idkey: str = 'id'):
        """insert or update record according to primary key column

        :param tbname: table name
        :type tbname: str
        :param cols: column names
        :type cols: list
        :param values: column values
        :type values: list
        :param idkey: primary key, defaults to 'id'
        :type idkey: str, optional
        """
        subcols = list(set(cols).difference([idkey]))
        ss = ','.join(["%s"]*len(cols))
        vv = ','.join(['`{}`=VALUES(`{}`)'.format(x, x) for x in subcols])
        colNames = ",".join(['`{}`'.format(x) for x in cols])
        sql = "insert into {}({}) values({}) ON DUPLICATE KEY UPDATE {}".format(
            tbname, colNames, ss, vv)
        self.insert(sql, values)

    def insert_table(self, tbname: str, cols: list, values: list):
        """insert table values into table

        :param tbname: table name
        :type tbname: str
        :param cols: column names
        :type cols: list
        :param values: insert values
        :type values: list
        """
        ss = ','.join(["%s"]*len(cols))
        colNames = ",".join(['`{}`'.format(x) for x in cols])
        sql = "insert into {}({}) values({})".format(tbname, colNames, ss)
        self.insert_many(sql, values)
