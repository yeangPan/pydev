#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : exceptions.py
@Time    : 2023/01/11 09:04:56
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''

class BaseError(Exception):
    code = None
    def __init__(self, message=None):
        self.message = message
    def __str__(self):
        return "code:{}, message:{}".format(self.code, self.message)


class InvaliInputError(Exception):pass

class IllegalValueError(BaseException):pass

class IllegalDataFormatError(BaseException):pass

class DatabaseError(BaseException):pass

class NoneValueError(BaseException):pass

class DatabaseInputError(BaseException):pass

class DatabaseRuntimeError(BaseException):pass

class DatabaseConnectionError(BaseException):pass

class NotEqualWarning(RuntimeWarning):pass

class ItemsEqualWarning(RuntimeWarning):pass

