#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : sql.py
@Time    : 2023/01/11 09:28:08
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''


from abc import ABC
from abc import abstractmethod


class SqlClient(ABC):


    def __init__(self, host, user, passwd, port, database) -> None:
        """init Client with conn info

        :param host: database host
        :type host: str
        :param user: username
        :type user: str
        :param passwd: password
        :type passwd: str
        :param port: port
        :type port: str
        :param database: database databasename, mysql/oracle/clickhouse, etc.
        :type database: str

        """
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.database = database

    @abstractmethod
    def check_db(self):pass

    @abstractmethod
    def check_tb(self,tbname):pass

    @abstractmethod
    def connection(self):pass

    @abstractmethod
    def query(self,sql):pass

    @abstractmethod
    def insert(self,sql):pass
    
    @abstractmethod
    def update(self,sql):pass

    @abstractmethod
    def delete(self,sql):pass

    
