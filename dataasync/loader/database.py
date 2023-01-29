#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : database.py
@Time    : 2023/01/20 10:55:45
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''


from typing import List
from pandas import DataFrame
from .base import BaseLoader
from ..util import get_database_conn
from ..exceptions import IllegalDataFormatError
from ..classes.reinforcementDict import ReinforcementDict


class DatabaseLoader(BaseLoader):

    
    def get_data_formatted(self, data:List):
        """the data from the logic should be well formatted, the neccesary info includes:
        \n1). title, corresping to data columns
        \n2). data values, the width of the value should be equal to the length of title
        :param data: the result from logic
        :type data: List
        :raises IllegalDataFormatError: when the data format has wrong format
        :return: the formatted data
        :rtype: tuple
        """
        if isinstance(data, DataFrame):
            return data.columns, data.values
        elif isinstance(data, tuple):
            return data[0], data[1]
        else:
            raise IllegalDataFormatError(
                'the data format has wrong format, pls check')

    def load(self, table: str, cfg: ReinforcementDict, data: List):
        """save the result from daily updating

        :param table: table name for the data
        :type table: str
        :param cfg: database connection
        :type cfg: ReinforcementDict
        :param data: data values
        :type data: list
        :raises RuntimeError: load error
        """
        try:
            client = get_database_conn(cfg)
            cols, vals = self.get_data_formatted(data=data)
            client.insert_table(tbname=table, cols=cols, values=vals)
        except Exception as e:
            raise RuntimeError('error happended when loading data to {} with cfg:\n{} and data values:\n{}'.format(
                table, str(cfg), str(data)))
