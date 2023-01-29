#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/11/03 17:32:36
@version    :1.0
@Description:
'''


import requests
import json


class Wechat:
    """
    微信推送
    """

    def __init__(self):
        self.partyID = '2'
        self.corpID = 'wwbb6e6fc818065b09'
        self.secret = 'nwfkRtGDu1FsvWOjNo04ouXXecPjA_zhCBEF-UkVHis'
        self.agentID = '1000003'

    def __get_token(self, corpid, secret):

        Url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        Data = {
            "corpid": corpid,
            "corpsecret": secret
        }
        r = requests.get(url=Url, params=Data)
        token = r.json()['access_token']
        return token

    def send_message(self, message, messagetype):  # text textcard markdown
        token = self.__get_token(self.corpID, self.secret)
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"

        data = {
            "toparty": self.partyID,
            "msgtype": messagetype,
            "agentid": self.agentID,
            messagetype: {
                "content": message
            },
            "safe": "0"
        }

        result = requests.post(url=url, data=json.dumps(data))
        return result.text

    def send_file(self, path, filetype):   # image, vioce, video, file
        token = self.__get_token(self.corpID, self.secret)
        post_url = f"https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token={token}&type={filetype}"
        data = {"media": open(path, 'rb')}
        r = requests.post(url=post_url, files=data)
        media_id = r.json()['media_id']
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        data = {
            "toparty": self.partyID,
            "msgtype": filetype,
            "agentid": self.agentID,
            filetype: {
                "media_id": media_id
            },
            "safe": "0"
        }
        result = requests.post(url=url, data=json.dumps(data))
        return result.text


