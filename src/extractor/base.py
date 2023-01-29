#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : extractor.py
@Time    : 2023/01/17 09:45:49
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : data extractor base class

'''


from abc import ABC
from abc import abstractmethod
from datetime import date,timedelta


from ..util import find
from ..util import find_key

class BaseExtractor(ABC):
    def __init__(self, settings) -> None:
        self.setttings = settings

    def setup(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

class DateParser:
    def __init__(self) -> None:
        pass
    
    def parser(cls,tdays:list, string:str):
        """convert the date format into date

        :param tdays: trade days list
        :type tdays: list
        :param string: the target content
        :type string: str
        :return: the replaced content
        :rtype: str
        """
        content = find_key(string)
        if content:
            try:
                for item in content:
                    loweritem = str(item).lower()
                    if '.' in loweritem:
                        values = loweritem.split('.')
                        lags = int(values[-1])
                        today = int(str(date.today().strftime('%Y%M%d')))
                        if 'b' in loweritem:
                            if lags>0:
                                d = find(tdays,today,'<')[-1]
                            else:
                                d = find(tdays,today,'>')[0]
                        else:
                            d = int((date.today()-timedelta(days=lags)).strftime('%Y%M%d'))
                    else:
                        d = int((date.today()).strftime('%Y%M%d'))
                    string = string.replace(item,str(d))
            except Exception as e:
                raise RuntimeError('illegeal format string for {} with error msg:\n{}'.format(string,str(e)))
        return string
                
                    
                    