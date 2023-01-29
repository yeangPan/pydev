#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 17:31:32
@version    :1.0
@Description:
'''

import os
import smtplib
import logging
from ..logger import logger
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header


class SendEmail:
    def __init__(self):
        self._sender = 'alpha@wanyantech.cn'
        self._sender_password = '****'
        self._server_url = 'smtp.exmail.qq.com'
        self._server_port = 465
        self._logger = None

    def set_sender(self, sender, sender_password):
        self._sender = sender
        self._sender_password = sender_password

    def set_server(self, server_url, server_port):
        self._server_url = server_url
        self._server_port = server_port

    def set_logger(self, logger: logging.Logger):
        if not isinstance(logger, logging.Logger):
            raise TypeError("logger type error.")
        self._logger = logger

    def get_logger(self):
        if not isinstance(self._logger, logging.Logger):
            self._logger = logger()
        return self._logger

    def send_email(self, to: list, carbon_copy=None, subject='无主题', message='空', attachment=''):
        to_all = []
        for i in to:
            to_all.append(i)

        if carbon_copy is None:
            carbon_copy = list()
        for i in carbon_copy:
            to_all.append(i)

        # 邮件头
        msg = MIMEMultipart()
        msg['From'] = self._sender
        msg['To'] = ','.join(to)
        msg['Cc'] = ','.join(carbon_copy)

        # 邮件标题
        msg['Subject'] = Header(subject, 'utf8')

        # 正文
        msg.attach(MIMEText(message, 'html', 'utf8'))

        if attachment:
            # 附件
            part = MIMEApplication(open(attachment, 'rb').read())
            part.add_header('Content-Disposition', 'attachment',
                            filename=os.path.basename(attachment))
            msg.attach(part)

        # 发送邮件
        logger = self.get_logger()
        # noinspection PyBroadException
        try:
            logger.info('开始发送邮件...')
            logger.info('主题: {}'.format(subject))
            logger.info('发件人: {}'.format(self._sender))
            logger.info('收件人: {}'.format(", ".join(to_all)))
            smtp = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
            smtp.login(self._sender, self._sender_password)
            smtp.sendmail(msg['From'], to_all, msg.as_string())
            smtp.quit()
            logger.info('邮件发送完毕!')
            return True
        except:
            logger.exception('邮件发送异常!')
            return False
