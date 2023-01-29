#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/11 18:36:11
@version    :1.0
@Description:
'''

import os
import json
from ..exceptions import NoneValueError
from .reinforcementDict import ReinforcementDict


class Json(ReinforcementDict):
    def __init__(self, cfgname):
        """init cfg from yaml
        :param cfgname: cfg name with full path, defaults to None
        :type cfgname: str, optional
        """
        if os.path.exists(cfgname):
            with open(cfgname, 'r', encoding='utf-8') as f:
                content = json.loads(f.read())
            super(Json, self).__init__(content)
        else:
            raise FileExistsError('{} not exist'.format(cfgname))

    @classmethod
    def create(self,cfg,cfgname):
        """generate config file according to config

        :param cfg: the target configuration
        :type cfg: ReinforcementDict
        :param cfgname: the target save path for the config file
        :type cfgname: str
        """
        d = self.build(cfg)
        if cfgname:
            if not os.path.exists(os.path.dirname(cfgname)):
                os.makedirs(os.path.dirname(cfgname))
            with open(cfgname, mode='w', encoding='utf_8_sig') as f:
                content = json.dumps(d)
                f.write(content)
        else:
            raise NoneValueError('input variable cfgname is None, pls check')

    @classmethod
    def build(self, cfg) -> dict:
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
                        l.append(self.build(item))
                    d.__setitem__(k, l)
                else:
                    d.__setitem__(k, self.build(v))
        else:
            if not isinstance(cfg, type(None)):
                d.__setitem__(k, str(v))
        return d
