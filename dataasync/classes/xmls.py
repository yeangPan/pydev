#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 10:49:06
@version    :1.0
@Description:
'''

import os
from typing import List
from xml.dom.minidom import parse
from xml.dom.minidom import Element
from .reinforcementDict import ReinforcementDict


class Xml(ReinforcementDict):
    def __init__(self, cfgname):
        """init cfg from xml

        :param cfgname: cfg name with full path, defaults to None
        :type cfgname: str, optional
        """
        if os.path.exists(cfgname):
            xml = parse(cfgname)
            root = xml.documentElement
            content = self.parseNode(root)
            cfg = dict()
            cfg[root.tagName] = content
            super(Xml, self).__init__(cfg)
        else:
            raise FileExistsError('file {} not exist'.format(cfgname))

    def parseNode(self, root: Element = None) -> dict:
        rootnode = dict()
        nodedict = dict()
        if root:
            attrs = root.attributes.keys()
            childs = root.childNodes
            if attrs:
                for attr in attrs:
                    nodedict[attr] = root.getAttribute(attr)
            if childs:
                nodeSet = self.calcNodeSet(childs)
                for k, v in nodeSet.items():
                    if len(v) > 1:
                        snodes = []
                        for item in v:
                            snodes.append(self.parseNode(root=item))
                        nodedict[k] = snodes
                    else:
                        nodedict[k] = self.parseNode(root=v[0])
        rootnode.update(nodedict)
        return rootnode

    def calcNodeSet(self, nodes: List[Element]) -> dict:
        nodeset = dict()
        names = set()
        for node in nodes:
            if isinstance(node, Element):
                names.add(node.tagName)
        for sname in names:
            nodeList = []
            for node in nodes:
                if (isinstance(node, Element)):
                    if (node.tagName == sname):
                        nodeList.append(node)
            nodeset[sname] = nodeList
        return nodeset

    
