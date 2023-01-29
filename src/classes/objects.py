#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 17:13:32
@version    :1.0
@Description:
'''

import numpy as np
from typing import List
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class ConnConfig:
    """database config detail
    """
    # host for database
    host: str = None
    # user to login in
    user: str = None
    # pass for user
    passwd: str = None
    # port for connection
    port: str = None
    # dbname
    database: str = None


@dataclass_json
@dataclass
class Asset:
    # asset code
    symbol: str = None
    # asset px
    price: str = None
    # port weight
    weight: float = None

    def __repr__(self) -> str:
        return ','.join(['{}:{}'.format(x[0], x[1]) for x in self.__dict__.items()])

    def __str__(self) -> str:
        return ','.join(['{}:{}'.format(x[0], x[1]) for x in self.__dict__.items()])


@dataclass_json
@dataclass
class Port:
    # assets
    assets: List[Asset] = None

    def __repr__(self) -> str:
        return '\n'.join([str(x) for x in self.assets])

    def __str__(self) -> str:
        return '\n'.join([str(x) for x in self.assets])

    @property
    def symbols(self):
        keys = []
        for item in self.assets:
            keys.append(item.symbol)
        return keys

    @property
    def weights(self):
        weights = []
        for item in self.assets:
            weights.append(item.weight)
        return weights

    def weight(self, symbol):
        weight = 0
        for item in self.assets:
            if item.symbol == symbol:
                weight = item.weight
        return weight

    def price(self, symbol):
        for item in self.assets:
            if item.symbol == symbol:
                return item.price

    @property
    def prices(self):
        prices = []
        for item in self.assets:
            prices.append(item.price)
        return prices

    @classmethod
    def filter(cls):
        port = []
        for item in cls.assets:
            if item.weight >= 0:
                port.append(item)
        cls.assets = port

    def check(self):
        s = 0
        for item in self.assets:
            s = s+item.weight
        if s == 1:
            return True
        else:
            return False

    def tvr(self, portPrev, portNext):
        symbolsPrev = portPrev.symbols
        symbolsNext = portNext.symbols
        symbols = set(portPrev.symbols).union(portNext.symbols)
        err = []
        for s in symbols:
            err.append(abs(symbolsPrev.weight(s)-symbolsNext.weight(s)))
        return np.sum(err)
