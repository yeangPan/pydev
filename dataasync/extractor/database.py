#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : DatabaseExtractor.py
@Time    : 2023/01/17 10:09:00
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''


from typing import List

from .base import DateParser
from .base import BaseExtractor

from ..util import get_database_conn
from ..classes.reinforcementDict import ReinforcementDict

class DatabaseExtractor(BaseExtractor):

    def __init__(self, cfg: ReinforcementDict, tdays:List) -> None:
        self.cfg = cfg
        self.tdays = tdays
        super().__init__(cfg)


    def extract(self):
        """extract data from database

        :param sql: sql scripts
        :type sql: str
        :return: sql query result
        :rtype: tuple including title and result:(title, result)
        """
        client = get_database_conn(self.cfg)
        return client.query(sql=self.get_database_sql())

    def get_database_sql(self):
        """construct sql query string
        if sql is provided in the config, return the sql string
        if not,  construct the sql from exec_params given
        :1). selected_cols;
        :2). where conditions include (column, value) pair plus connector(and, or, etc);
        :3). orders conditions;
        :return: query string 
        :rtype: str
        """
        if self.cfg.has('exec_params'):
            if self.cfg.exec_params.has('sql'):
                sql = DateParser().parser(tdays=self.tdays, string=self.cfg.exec_params.sql)
            else:
                table = self.cfg.tablename
                params = self.cfg.exec_params
                selected_cols = params.selected_cols
                if selected_cols == "${ALL}":
                    selected_cols = '*'
                flag = 0
                conditions = []
                # orderbys = []
                if params.has('conditions'):
                    for cond in params.conditions:
                        connector = '' if flag == 0 else cond.connect
                        val = DateParser().parser(tdays=self.tdays, string=cond.value)
                        cond_str = " {} {}{}{}".format(
                            connector, cond.column, cond.comarator, val)
                        conditions.append(cond_str)
                        flag += 1
                cond_total = ''.join(conditions)
                sql = 'select {} from {} where {};'.format(
                    selected_cols, table, cond_total)
        else:
            sql = 'select * from {};'.format(table)
        return sql