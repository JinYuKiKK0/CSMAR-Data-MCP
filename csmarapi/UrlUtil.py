#coding=utf-8
'''
Created on 2019年11月1日
配置文件工具类
@author: jiahang.miao
'''
import configparser
import os

class UrlUtil(object):

    __db_host=''
    def __init__(self):
        path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config = configparser.ConfigParser()
        config.read(path+"/csmarapi/config.ini", encoding="utf-8")
        config.sections()  # 获取section节点
        config.options('url')  # 获取指定section 的options即该节点的所有键
        self.__listDbsUrl=config.get('url',"query.listDbsUrl")
        self.__loginUrl=config.get('url',"query.loginUrl")
        self.__listFieldsUrl=config.get('url',"query.listFieldsUrl")
        self.__listTablesUrl=config.get('url',"query.listTablesUrl")
        self.__previewUrl=config.get('url',"query.previewUrl")
        self.__packResultUrl=config.get('url','query.getPackResultUrl')
        self.__packUrl=config.get('url','query.packUrl')
        self.__queryUrl=config.get('url','query.queryUrl')
        self.__queryCountUrl=config.get('url','query.queryCountUrl')
        self.__websocketUrl=config.get('url','query.websocketUrl')
        self.__version=config.get('url','conf.version')
        
    def getListDbsUrl(self):
        return self.__listDbsUrl
    
    def getLoginUrl(self):
        return self.__loginUrl
    
    def getListFieldsUrl(self):
        return self.__listFieldsUrl
    
    def getListTablesUrl(self):
        return self.__listTablesUrl
    
    def getPreviewUrl(self):
        return self.__previewUrl
    
    def getPackResultUrl(self):
        return self.__packResultUrl

    def getPackUrl(self):
        return self.__packUrl

    def getQueryUrl(self):
        return self.__queryUrl

    def getQueryCountUrl(self):
        return self.__queryCountUrl

    def getWebsocketUrl(self):
        return self.__websocketUrl

    def getVersionNum(self):
        return self.__version
