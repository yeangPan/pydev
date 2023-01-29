#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 17:04:22
@version    :1.0
@Description:
'''


import os
import sys
# import time
import random
import logging
import multiprocessing
# from shutil import copyfile
from logging.handlers import TimedRotatingFileHandler


class logger:
    def __init__(self, logname=None, logdir=None):
        tempdir = os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', 'logs'))
        d = logdir if logdir else tempdir
        name = logname if logname else 'log'
        fullname = '{}.log'.format(name)
        ofn = os.path.join(d, fullname)
        d = os.path.dirname(ofn)
        if not os.path.exists(d):
            os.makedirs(d)
            os.chmod(d, 0o777)

        # mtime = time.strftime(
        #     '%Y%m%d', time.localtime(os.stat(ofn).st_mtime))
        # utime = time.strftime('%Y%m%d', time.localtime())
        # # if log file is more than 1MB, copy to a file and clear log file
        # if utime != mtime:
        #     fname = '{}.{}.log'.format(logname,mtime)
        #     nfn = os.path.join(d, fname)
        #     # print(os.path.getsize(log_path))
        #     copyfile(ofn, nfn)
        #     with open(ofn, 'w') as f:
        #         f.truncate(0)
        #         f.close()
        self.logger_format = logging.Formatter(
            '%(asctime)s %(filename)s - line:%(lineno)d - tid:%(thread)d %(levelname)s ===>>> %(message)s')
        self.c_logger_format = logging.Formatter(
            '%(asctime)s %(filename)s - line:%(lineno)d - tid:%(thread)d %(levelname)s ===>>> %(message)s')
        self.logger = logging.getLogger(str(random.random()))
        self.logger.handlers.clear()
        self.logger.setLevel(logging.DEBUG)
        # self.filehandler = logging.FileHandler(ofn, mode='a')
        self.filehandler = TimedRotatingFileHandler(filename=ofn,
                                                    when='MIDNIGHT',
                                                    interval=1,
                                                    backupCount=100,
                                                    encoding='utf-8')
        self.filehandler.setLevel(logging.INFO)
        self.filehandler.setFormatter(self.logger_format)

        self.stdouthandler = logging.StreamHandler(sys.stdout)
        self.stdouthandler.setLevel(logging.INFO)
        self.stdouthandler.setFormatter(self.c_logger_format)

        self.logger.addHandler(self.stdouthandler)
        self.logger.addHandler(self.filehandler)

        self.__lock = multiprocessing.Lock()

    def info(self, msg):
        self.__lock.acquire()
        self.logger.info(msg, stacklevel=2)
        self.__lock.release()

    def debug(self, msg):
        self.__lock.acquire()
        self.logger.debug(msg, stacklevel=2)
        self.__lock.release()

    def warning(self, msg):
        self.__lock.acquire()
        self.logger.warning(msg, stacklevel=2)
        self.__lock.release()

    def error(self, msg):
        self.__lock.acquire()
        self.logger.error(msg, stacklevel=2)
        self.__lock.release()

    def critical(self, msg):
        self.__lock.acquire()
        self.logger.critical(msg, stacklevel=2)
        self.__lock.release()
