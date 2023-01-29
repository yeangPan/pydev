#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 11:03:27
@version    :1.0
@Description:
'''

import os
import yaml
from ..exceptions import NoneValueError
from .reinforcementDict import ReinforcementDict

class Yml(ReinforcementDict):
    def __init__(self, cfgname):
        """init Yml config

        :param cfgname: cfg file name with full path
        :type cfgname: str
        :raises FileExistsError: raise error and return related information
        """
        if os.path.exists(cfgname):
            with open(cfgname, 'r', encoding='utf-8') as f:
                content = yaml.load(f.read(), yaml.FullLoader)
            super(Yml, self).__init__(content)
        else:
            raise FileExistsError('{} not exist'.format(cfgname))

    @classmethod
    def create(cls, cfg, cfgname):
        """generate config file according to config

        :param cfg: the target configuration
        :type cfg: ReinforcementDict
        :param cfgname: the target save path for the config file
        :type cfgname: str
        """
        # generate config content
        content = cls.build(cfg=cfg)
        if cfgname:
            p = os.path.dirname(cfgname)
            if not os.path.exists(p):
                os.makedirs(p)
            with open(p, mode='w', encoding='utf_8_sig') as f:
                yaml.dump(data=content, stream=f, allow_unicode=True)
        else:
            raise NoneValueError('input variable cfgname is None, pls check')

    @classmethod
    def build(cls, cfg) -> dict:
        """ convert config into dict content

        :param cfg: the target configuration
        :type cfg: ReinforcementDict
        :return: _description_
        :rtype: dict
        """
        d = dict()
        if isinstance(cfg, ReinforcementDict):
            for k, v in cfg.items():
                if isinstance(v, str):
                    d.__setitem__(k, v)
                elif isinstance(v, int):
                    d.__setitem__(k, v)
                elif isinstance(v, float):
                    d.__setitem__(k, v)
                elif isinstance(v, type(None)):
                    continue
                elif isinstance(v, list):
                    l = []
                    for item in v:
                        l.append(cls.build(item))
                    d.__setitem__(k, l)
                else:
                    d.__setitem__(k, cls.build(v))
        else:
            if not isinstance(cfg, type(None)):
                d.__setitem__(k, str(v))
        return d
