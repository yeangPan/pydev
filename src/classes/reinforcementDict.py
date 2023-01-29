#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    : reinforcementDict.py
@Time    : 2023/01/11 09:05:14
@Author  : yangp
@Contact : yangp@wanyantech.com
@Version : 0.1
@License : Apache License Version 2.0, January 2023
@Desc    : None
'''


class ReinforcementDict(object):
    def __init__(self, Dict):
        for key, value in Dict.items():
            if type(value) == dict:
                setattr(self, key, ReinforcementDict(value))
            elif type(value) == list:
                l = []
                for item in value:
                    if type(item) == dict:
                        l.append(ReinforcementDict(item))
                    else:
                        l.append(item)
                setattr(self, key, l)
            else:
                setattr(self, key, value)

    def __str__(self) -> str:
        return str(self.__dict__)
    
    def __repr__(self) -> str:
        return str(self.__dict__)

    def __getitem__(self, key):
        """magic func to make class Subscriptable 

        :param key: needed key
        :type key: str
        :return: value corresponding to key
        :rtype: any
        """
        if key in self.__dict__.keys():
            return self.__dict__[key]
        else:
            raise KeyError('key {} not in object'.format(key))

    def has(self, key):
        """return value indicates whether the key in keys of dict

        :param key: target key
        :type key: any
        :return: bool value
        :rtype: bool
        """
        return key in list(self.__dict__.keys())

    def hasval(self, val):
        """return value indicates whether the value in values from the dict

        :param val: target value
        :type val: any
        :return: bool value
        :rtype: bool
        """
        return val in list(self.__dict__.values())

    def pop(self, key):
        """pop out an item according to key

        :param key: key of the target item
        :type key: any
        """
        self.__dict__.pop(key)

    def update(self, key):
        self.__dict__.update(key)

    def set(self, key, val):
        """set a new item from a pair of vars

        :param key: target key
        :type key: any
        :param val: target value
        :type val: any
        """
        self.__setattr__(key, val)

    def get(self, key):
        """get corresponding value from the reinforcement dict according to its key, if none, return none

        :param key: target key value
        :type key: any
        :return: the target value according to key
        :rtype: any
        """
        if self.has(key):
            return self.__getattribute__(key)

    @property
    def keys(self):
        """get all keys from the reinforcementdict

        :return: all keys of the dict
        :rtype: dict_keys
        """
        return self.__dict__.keys()

    @property
    def values(self):
        """get all values from the reinforcementdict

        :return: all values of the dict
        :rtype: dict_values
        """
        return self.__dict__.values()

    def items(self):
        """get all items from the reinforcementdict

        :return: all items of the dict
        :rtype: dict_items
        """
        return self.__dict__.items()

    def delete(self, key):
        """delete target item according to key value

        :param key: the target key
        :type key: any
        """
        if self.has(key):
            self.__dict__.__delitem__(key)

    def to_dict(self):
        """convert self into dict
        """
        return self.__convert_to_dict(self.__dict__)

    def __convert_to_dict(self, instance):
        """convert an instance into dict

        :param instance: an instance
        :type instance: Reinforcementdict
        :return: corresponding dict to the original instance
        :rtype: dict
        """
        d = dict()
        for k, v in instance.items():
            if isinstance(v, ReinforcementDict):
                d[k] = self.__convert_to_dict(v)
            else:
                d[k] = v
        return d
