#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 16:59:01
@version    :1.0
@Description:
'''


import os
import datetime
from enum import Enum

# 项目工程路径及配置相关
HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
UPDATE:str = 'update'
HISTORY: str = 'history'
ALL: str = 'all'
CONFIG = os.path.join(HOME,'config','async.yml')

# 程序执行日期相关
TODAY = datetime.datetime.today()
if datetime.datetime.now().hour < 6:
    TODAY = TODAY - datetime.timedelta(1)
TODAY = TODAY.strftime('%Y%m%d')

class Comparator(Enum):
    BIGGER: str = '>'
    SMALLER: str = '<'
    BIGGER_OR_EQUAL: str = '>='
    SMALLER_OR_EQUAL: str = '<='
    EQUAL: str = '='
    DOUBLE_EQUAL: str = '=='


class DType(Enum):
    """the format of data source or data saved
    """
    DATABASE = 'database'
    API = 'api'
    FILE = 'file'
    FILEGENERATOR = 'filegerator'
    
    
class DataType(Enum):
    """the format of data data
    """
    FACTOR = 'factor'
    BASEDATA = 'basedata'
    HYPERDATA = 'hyperdata'


class DatabaseType(Enum):
    MYSQL = 'mysql'
    ORACLE = 'oracle'
    CLICKHOUSE = 'clickhouse'
    SQLITE3 = 'sqlite3'


class ExtractorType(Enum):
    DATABASEEXTRACTOR = 'DatabaseExtractor'
    APIEXTRACTOR = 'ApiExtractor'
    FILEEXTRACTOR = 'FileExtractor'
    FILEFROMDIREXTRACTOR = 'FilefromDirExtractor'


class LoaderType(Enum):
    DATABASELOADER = 'DatabaseExtractor'
    APILOADER = 'ApiExtractor'
    FILELOADER = 'FileExtractor'
    FILEFROMDIRLOADER = 'FilefromDirExtractor'

# mysql类型对应clickhouse类型
MYSQL_CH_TYPE_MAP = {
    'bit': 'Int8',
    'bigint': 'Int64',
    'decimal': 'Float64',
    'date': 'Date',
    'datetime': 'DateTime',
    'varchar': 'String',
    'longtext': 'String',
    'text': 'String'
}

# dataframe类型对应clickhouse类型
DF_CH_TYPE_MAP = {
    'uint8': 'UInt8',
    'uint16': 'UInt16',
    'uint32': 'UInt32',
    'uint64': 'UInt64',
    'ulonglong': 'UInt256',
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'longlong': 'Int256',
    'float16': 'Float32',
    'float32': 'Float32',
    'float64': 'Float64',
    'O': 'String',
    'object': 'String',
    'datetime64[ns]': 'Date',
    '<M8[ns]': 'Date'
}

NORM_FREQ_MAP = {
    'D': 'D',
    'WS': 'W-MON',
    'W': 'W-SUN',
    'MS': 'MS',
    'M': 'M',
    'QS': 'QS',
    'Q': 'Q',
    'YS': 'YS',
    'Y': 'Y'
}

BUSY_FREQ_MAP = {
    'D': 'TRADE_DATE',
    'WS': 'WEEK_START_DATE',
    'W': 'WEEK_END_DATE',
    'MS': 'MONTH_START_DATE',
    'M': 'MONTH_END_DATE',
    'QS': 'QUARTER_START_DATE',
    'Q': 'QUARTER_END_DATE',
    'YS': 'YEAR_START_DATE',
    'Y': 'YEAR_END_DATE'
}

STOCK_SECTION = {
    '0201010101': '沪市主板A',
    '0201010102': '沪市主板B',
    '0201010103': '深市主板A',
    '0201010104': '深市主板B',
    '0201010105': '深市中小板A',
    '0201010106': '深市创业板A',
    '0201010107': '沪市代理交易深市股票',
    '0201010108': '深市代理交易沪市股票',
    '0201010109': '沪市科创板',
    '0201010302': '优先股'
}

EXCHANGE_CODE = {
    'XZCE': 'ZHENGZHOU COMMODITY EXCHANGE',
    'XSME': 'SHENZHEN MERCANTILE EXCHANGE',
    'XSGE': 'SHANGHAI FUTURES EXCHANGE',
    'SGEX': 'SHANGHAI GOLD EXCHANGE',
    'XSHE': 'SHENZHEN STOCK EXCHANGE',
    'XSHG': 'SHANGHAI STOCK EXCHANGE',
    'XCFE': 'CHINA FOREIGN EXCHANGE TRADE SYSTEM',
    'XSSC': 'SHANGHAI STOCK EXCHANGE ‐ SHANGHAI ‐ HONG KONG STOCK CONNECT',
    'TPME': 'TIANJIN PRECIOUS METALS EXCHANGE',
    'CCFX': 'CHINA FINANCIAL FUTURES EXCHANGE',
    'XSIE': 'SHANGHAI INTERNATIONAL ENERGY EXCHANGE',
    'XDCE': 'DALIAN COMMODITY EXCHANGE'
}

IDX_NAME = {
    "SSE50": "000016",
    "CSI300": "000300",
    "CSI500": "000905",
    "CSI800": "000906",
    "CSI1000": "000852",
    "CNT": "399006",
    "CHINEXTTR": "399606",
    "CHINEXTC": "399102"
}
