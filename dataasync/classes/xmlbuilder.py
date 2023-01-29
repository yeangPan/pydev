#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 11:04:15
@version    :1.0
@Description:
'''

import os
from .xmls import Xml
from xml.dom.minidom import Element
from xml.dom.minidom import getDOMImplementation
from .reinforcementDict import ReinforcementDict


class XmlBuilder:

    def __init__(self, xmlcfg: Xml = None, spath: str = None) -> None:
        self.xmlcfg = xmlcfg
        self.spath = spath
        impl = getDOMImplementation()
        self.doc = impl.createDocument(None, None, None)
        self.create()

    def create(self):
        self.build(self.doc, self.xmlcfg)
        if self.spath:
            if not os.path.exists(os.path.dirname(self.spath)):
                os.makedirs(os.path.dirname(self.spath))
            with open(self.spath, mode='w', encoding='utf_8_sig') as f:
                f.write(self.doc.toprettyxml(indent="\t"))

    def build(self, node: Element, cfg, name=None) -> Element:
        if isinstance(cfg, ReinforcementDict):
            for k, v in cfg.items():

                if isinstance(v, str):
                    node.setAttribute(k, v)
                elif isinstance(v, int):
                    node.setAttribute(k, str(v))
                elif isinstance(v, float):
                    node.setAttribute(k, str(v))
                elif isinstance(v, type(None)):
                    continue
                elif isinstance(v, list):
                    for item in v:
                        parnode = self.doc.createElement(k)
                        node.appendChild(parnode)
                        self.build(parnode, item)
                else:
                    parnode = self.doc.createElement(k)
                    node.appendChild(parnode)
                    self.build(parnode, v, k)
        else:
            if not isinstance(cfg, type(None)):
                node.setAttribute(name, str(cfg))
