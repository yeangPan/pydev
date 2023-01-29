#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : dbapi.py
@Time    : 2023/01/10 11:57:03
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : 
the api is designed to be used with classes decorated with dataclass_json & dataclass
'''


from typing import List
from ..util import parse
from ..constants import DType
from ..constants import DatabaseType

# from importlib import import_module
from ..datastores.mysql import MySQL
from ..classes.objects import ConnConfig
from ..exceptions import InvaliInputError




class Operation(object):
    def __init__(self, conn) -> None:
        """init database operation

        :param conn: connection config for datebase
        :type conn: dict
        """
        self.conn = conn
        self.client = self.get_client()

    def get_client(self):
        """get database client according to database, default mysql
        """
        cfg = ConnConfig()
        cfg = cfg.from_dict(self.conn)
        if DType.DATABASE.value in self.conn.keys():
            database = self.conn.get(DType.DATABASE.value)
        else:
            database = 'mysql'
        if database == DatabaseType.MYSQL.value:
            return MySQL(**(cfg.to_dict()))
        elif database == DatabaseType.CLICKHOUSE.value:
            pass
        elif database == DatabaseType.ORACLE.value:
            pass
        elif database == DatabaseType.SQLITE3.value:
            pass

    def query(self, instance, Dtype=None, **kwargs):
        """query table by instance

        :param instance: instance
        :type instance: class used dataclass_json
        :param Dtype: the class should create
        :type Dtype: user defined class
        :return: queried results
        :rtype: list,list
        """
        rets = []
        try:
            tb, cols, vals = parse(instance, with_nan=False)
            if tb:
                title, rows = self.client.query_by_cols(
                    tb, cols, vals, **kwargs)
                for row in rows:
                    # mods = import_module(
                    #     '.classes.objects', package='dataasync')
                    # module = mods.__dict__.get(instance.__class__.__name__)
                    inst = Dtype()
                    inst = inst.from_dict(dict(zip(title, row)))
                    rets.append(inst)
                    return rets
            else:
                raise InvaliInputError('the input instance is none, pls check')
        except Exception as e:
            raise RuntimeError(str(e))

    def create(obj):
        pass

    def insert(obj):
        pass

    def update(obj):
        pass

    def delete(self, instance):
        """query table by instance

        :param instance: instance
        :type instance: class used dataclass_json
        :return: queried results
        :rtype: list,list
        """
        try:
            tb, cols, vals = parse(instance, with_nan=False)
            if tb:
                self.client.delete_by_cols(tb, cols, vals)
            else:
                raise InvaliInputError('the input instance is none, pls check')
        except Exception as e:
            raise RuntimeError(str(e))

    def delete_all(self, instances: List):
        """query table by instance

        :param instance: instance
        :type instance: class used dataclass_json
        :return: queried results
        :rtype: list,list
        """
        try:
            if instances:
                for inst in instances:
                    self.delete(instance=inst)
        except Exception as e:
            raise InvaliInputError('the input instance is none, pls check')

    def insert(self, instance):
        """insert instance into table

        :param instance: the target instance
        :type instance: any
        :raises RuntimeError: errors
        """
        try:
            tb, cols, vals = parse(instance)
            self.client.insert_record(tb, cols, vals)
        except Exception as e:
            raise RuntimeError(str(e))

    def update(self, tb, scols, svals, ccols, cvals):
        """set intance set scols=svals where ccols = cvals

        :param instance: instance
        :type instance: any
        :param scols: set columns
        :type scols: list
        :param svals: set values
        :type svals: list
        """
        try:
            self.client.update_cols(
                tb, scols=scols, svals=svals, ccols=ccols, cvals=cvals)
        except Exception as e:
            raise RuntimeError(str(e))

    def insert_all(self, instances: list):
        """insert instances into db

        :param instances: instances
        :type instances: list
        :raises RuntimeError: error
        """
        try:
            if instances:
                vals = []
                tb, cols, _ = parse(instances[0])
                for instance in instances:
                    _, _, val = parse(instance)
                    vals.append(val)
                self.client.insert_table(tb, cols, vals)
        except Exception as e:
            raise RuntimeError(str(e))

    def insert_update(self, instance):
        """insert or update an instance into table

        :param instance: the target instance
        :type instance: any
        :raises RuntimeError: error
        """
        try:
            tb, cols, vals = parse(instance)
            self.client.insert_update(tb, cols, vals)
        except Exception as e:
            raise RuntimeError(str(e))

    def insert_updates(self, instances):
        """insert or update instances into tables

        :param instances: target instances
        :type instances: list
        :raises RuntimeError: error
        """
        try:
            for item in instances:
                self.insert_update(instance=item)
        except Exception as e:
            raise RuntimeError(str(e))
