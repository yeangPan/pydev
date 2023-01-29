#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 17:24:39
@version    :1.0
@Description:
'''


import re
import os
import h5py
import time
import socket
import hashlib
import platform
import zipfile
import subprocess
import numpy as np
import pandas as pd
from math import ceil
from uuid import uuid4
from typing import List
from .constants import Comparator
from datetime import date, datetime, timedelta

def timestr(t: datetime):
    timeString = str(t)[:17].replace('-', '').replace(' ', '').replace(':', '')
    return timeString


def generateId():
    return str(uuid4())


def rep(pattern: str, dateString: str):
    """generate file name for daily generated file

    :param fnPath: file path
    :type fnPath: str 
    :param dateString: date with format YYYYMMDD
    :type dateString: str
    """
    dateString = dateString.replace('-', '')
    pattern = pattern.replace('${YYYYMMDD}', dateString)
    pattern = pattern.replace('${yyyymmdd}', dateString)
    pattern = pattern.replace('${YYYYMM}', dateString[:6])
    pattern = pattern.replace('${yyyymm}', dateString[:6])
    pattern = pattern.replace('${YYYY}', dateString[:4])
    pattern = pattern.replace('${yyyy}', dateString[:4])
    pattern = pattern.replace('${MMDD}', dateString[4:8])
    pattern = pattern.replace('${mmdd}', dateString[4:8])
    pattern = pattern.replace('${MM}', dateString[4:6])
    pattern = pattern.replace('${mm}', dateString[4:6])
    pattern = pattern.replace('${DD}', dateString[6:8])
    pattern = pattern.replace('${dd}', dateString[6:8])
    d = time.strftime('%Y-%m-%d', time.strptime(dateString, '%Y%m%d'))
    pattern = pattern.replace('${YYYY-MM-DD}', d)
    pattern = pattern.replace('${yyyy-mm-dd}', d)
    pattern = pattern.replace('${YYYY-MM}', d[:7])
    pattern = pattern.replace('${yyyy-mm}', d[:7])
    pattern = pattern.replace('${MM-DD}', d[-5:])
    pattern = pattern.replace('${mm-dd}', d[-5:])
    return pattern


def parse(instance, with_nan=True):
    """parse serialized data info

    :param instance: instance for certain class
    :type instance: any, user defined class
    :param with_nan: whether parse none column, defaults to True
    :type with_nan: bool, optional
    :return: table name(class), parsed columns and values
    :rtype: tuple
    """
    if instance:
        tb = instance.__class__.__name__
        cols, vals = [], []
        for k, v in instance.__dict__.items():
            if not with_nan:
                if v:
                    cols.append(k)
                    vals.append(v)
            else:
                cols.append(k)
                vals.append(v)
        return tb, cols, vals


def iters(rootdirs: str) -> list:
    """search all files
    :param rootdirs: dirs name
    :type rootdirs: str
    :return: files with full path
    :rtype: list
    """
    files = []
    for home, _, fns in os.walk(rootdirs):
        for fn in fns:
            file = os.path.join(home, fn)
            if os.path.isfile(file):
                files.append(file)
    return files


def find_key(content: str):
    """
    1.正则匹配串前加了r就是为了使得里面的特殊符号不用写反斜杠了。
    2.[ ]具有去特殊符号的作用,也就是说[(]里的(只是平凡的括号
    3.正则匹配串里的()是为了提取整个正则串中符合括号里的正则的内容
    :param content: the string need to be processed
    :type content: str
    """

    if isinstance(content, str):
        p = re.compile('\$[{](.*)[}]', re.S)
        return re.findall(p, content)


def exclude(sourcePath: str, ignores: List[str]):
    """ exclude ignore list settings

    :param sourcePath: the project path 
    :type sourcePath: str
    :param ignores: the ignore list including files or directories.
    :type ignores: List[str]
    """
    files = []
    for root, _, fns in os.walk(sourcePath):
        for fn in fns:
            temp = os.path.join(root, fn)
            flag = False
            for ig in ignores:
                reg = os.path.join(sourcePath, ig)
                if reg in temp:
                    flag = flag | True
            if not flag:
                files.append(temp)
    return files


def find(array: List, number: any, direction: Comparator, count=1):
    """find the target values that satisfy the condition

    :param array: the target list
    :type array: List
    :param number: the target value
    :type number: any
    :param direction: compare direction
    :type direction: Comparator
    :param count: the number of value needed from the target list, defaults to 1
    :type count: int, optional
    :return: values from array that satisfies the condition
    :rtype: List
    """
    arr = np.array(array)
    if direction == Comparator.SMALLER.value:
        cond = np.where(arr < number, True, False)
        ret = arr[cond][-count:] if count > 0 else arr[cond]
        return ret
    elif direction == Comparator.SMALLER_OR_EQUAL.value:
        cond = np.where(arr <= number, True, False)
        ret = arr[cond][-count:] if count > 0 else arr[cond]
        return ret
    elif direction == Comparator.BIGGER.value:
        cond = np.where(arr > number, True, False)
        ret = arr[cond][:count] if count > 0 else arr[cond]
        return ret
    elif direction == Comparator.BIGGER_OR_EQUAL.value:
        cond = np.where(arr >= number, True, False)
        ret = arr[cond][:count]if count > 0 else arr[cond]
        return ret
    else:
        return array


def get_host():
    """
    查询本机ip地址
    :return: ip
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def generate_min_bar(today: str = None, minutes: int = 15):
    """generate minute bars

    :param today: date of today, defaults to None
    :type today: str, optional
    :param minutes: the interval, defaults to 15
    :type minutes: int, optional
    :return: generated minute bars
    :rtype: List
    """
    d = str(date.today()).replace('-', '') if not today else today
    freq = '{}T'.format(minutes)
    prev = ['{} 09:45:00'.format(d), '{} 11:35:00'.format(d)]
    next = ['{} 13:00:00'.format(d), '{} 15:30:00'.format(d)]
    lstPrev = pd.date_range(start=prev[0], end=prev[1], freq=freq).to_list()
    lstNext = pd.date_range(start=next[0], end=next[1], freq=freq).to_list()
    lst = [x-timedelta(minutes=0) for x in lstPrev+lstNext]
    return lst


def bdays(fn: str):
    with open(fn, mode='r', encoding='utf_8_sig') as f:
        lines = f.read()
        return [int(x) for x in lines.split('\n')]


def runcmd(cmd):
    """exec command using subprocess

    :param cmd: command
    :type cmd: str
    :return: exec result/msg/duration/stime/etime
    :rtype: multiple returns
    """
    stime = datetime.now()
    [ret, msg] = subprocess.getstatusoutput(cmd)
    etime = datetime.now()
    duration = (etime-stime).total_seconds()
    return ret, msg, duration, stime, etime


def get_platform():
    """get platform

    :return: platform of the programs
    :rtype: str
    """
    return platform.system().lower()


def runxml(home, xml, time: str = '', envname='base'):
    timecmd = ' -e {}'.format(time) if time else ''
    prefix = 'source /home/{}/.bashrc && '.format(
        os.getlogin()) if get_platform() == 'linux' else ''
    cmd = r"""{}conda activate {} && python {} {}{}""".format(
        prefix, envname, home, xml, timecmd)
    ret, msg, duration, stime, etime = runcmd(cmd)
    return ret, msg, duration, stime, etime


def readAlpha(fn, isIntra: bool = True):
    """load alpha data

    :param fn: file with full path
    :type fn: str
    :param isIntra: whether the alpha is intra, defaults to True
    :type isIntra: bool, optional
    :return: alpha data with dates and symbols
    :rtype: tuple
    """
    f = h5py.File(fn, mode='r')
    data = f['alphadata'][:]
    t = f['bars'][:] if isIntra else f['dates'][:]
    symbol = f['uid'].asstr()[...]
    return data, t, symbol


def inter(arrA, arrB):
    """intersaction from iterable variable arrA and arrB

    :param arrA: arrA
    :type arrA: iterable
    :param arrB: arrB
    :type arrB: iterable
    :return: intersaction
    :rtype: iterable
    """
    ia = np.array((range(len(arrA))))
    ib = np.array((range(len(arrB))))
    sa = [(x in arrB) for x in arrA]
    sb = [(x in arrA) for x in arrB]
    return ia[sa], ib[sb]


def compressZip(sourcePath, targetPath, name, ignores):
    '''
    :param sourcePath:待压缩文件所在文件目录
    :param targetPath:目标文件目录
    :param fn:目标文件目录
    :param ignores:忽略清单列表
    :return:null
    '''
    if not os.path.exists(targetPath):
        os.mkdir(targetPath)
    today = targetPath + os.sep + time.strftime('%Y%m%d')
    target = today + '.' + name + '.zip'
    tarZip = zipfile.ZipFile(target, 'w', zipfile.ZIP_STORED)
    fileList = exclude(sourcePath, ignores)
    for filename in fileList:
        tarZip.write(filename, filename[len(sourcePath):])
    tarZip.close()


def unZip(sourceFile, targetPath):
    '''
    :param sourceFile: 待解压zip路径
    :param targetPath: 目标文件目录
    :return:
    '''
    file = zipfile.ZipFile(sourceFile, 'r')
    file.extractall(targetPath)


def fileHash(fileName, hashType="md5", blockSize=1024*1024):
    """
    Support md5, sha1, sha224, sha256, sha384, sha512, blake2b, blake2s,
    sha3_224, sha3_256, sha3_384, sha3_512, shake_128, and shake_256
    """
    with open(fileName, 'rb') as file:
        hash = hashlib.new(hashType, b"")
        data = file.read(blockSize)
        hash.update(data)
    return hash.hexdigest()


def sep(lst, num):
    """seperate list to num lists

    :param lst: original list
    :type lst: iterable
    :param num: number
    :type num: int
    :return: return list
    :rtype: list
    """
    return list(map(lambda x: lst[x * num: x * num + num], list(range(0, ceil(len(lst) / num)))))


def get_database_conn(settings):
        """get sql connection according to database type, supported databases:
        MYSQL,CLICKHOUSE,sqlite3, etc

        :raises TypeError: invalid database type
        :return: sql connection
        :rtype: database connection
        """
        from .constants import DType
        from .constants import DatabaseType
        from .classes.objects import ConnConfig
        config = settings.to_dict()
        if settings.has(DType.DATABASE.value):
            t = str(settings.database_type).strip().lower()
        else:
            t = DatabaseType.MYSQL.value
        conn = ConnConfig()
        conn = conn.from_dict(config)
        if t == DatabaseType.MYSQL.value:
            from .datastores.mysql import MySQL
            client = MySQL(**(conn.to_dict()))
        elif t == DatabaseType.CLICKHOUSE.value:
            client = None
        else:
            raise TypeError('unsupported batabase')
        return client
