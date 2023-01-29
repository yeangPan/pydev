#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : base.py
@Time    : 2023/01/18 08:51:24
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''



from abc import ABC
from abc import abstractmethod



class BaseLoader(ABC):
    def __init__(self, settings) -> None:
        self.setttings = settings

    def setup(self):
        pass

    @abstractmethod
    def load(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

