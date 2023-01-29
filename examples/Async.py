#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 17:59:40
@version    :1.0
@Description:
'''

import os
import sys
import argparse
import warnings
import traceback
from env import *
from dataasync.logger import logger
from dataasync.constants import HOME
from dataasync.constants import TODAY
from dataasync.constants import CONFIG
from dataasync.classes.ymls import Yml
from dataasync.extractor.database import DatabaseExtractor

log = logger(logname=CONFIG.GLOBALVAR.LOG.LOG_NAME)

warnings.filterwarnings('ignore')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--meta', default=os.path.join(HOME,'examples','async.yml'), help='meta info config file')
    parser.add_argument('-c', '--config', default=os.path.join(HOME,'config','async.yml'), help='project config file')
    args = parser.parse_args()
    if args.config:
        cfg = Yml(cfgname=args.config)
    else:
        cfg = CONFIG
    log.info('program starts with config {}'.format(os.path.basename(args.config)))
    cfg = Yml(cfgname=args.meta)
    e = DatabaseExtractor(cfg=cfg.ashareeod.meta.extractor.DatabaseExtractor.data)
    e.extract()
    
