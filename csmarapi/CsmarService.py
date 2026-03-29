#coding=utf-8
'''
Created on 2019年12月25日
csmarapi服务接口
@author: jiahang.miao
'''
import json
import urllib3
from csmarapi.UrlUtil import UrlUtil
import logging
import time
import zipfile
import csv
import os
from urllib import parse
import datetime
from websocket import create_connection
import sys
import pandas as pd

class CsmarService(object):
    
    '''
    csmarapi服务接口
    '''
    '''
        存放loadData方法解析的结果集
    '''
    dataList=[]
    
    def __init__(self):
        '''
        Constructor
        '''
        self.urlUtil= UrlUtil()
        logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                    datefmt='%a %d %b %Y %H:%M:%S')
        self.logger = logging.getLogger('CsmarService')
        fh = logging.FileHandler('csmar-log.log', mode='a', encoding='utf-8', delay=False)
        fh.setLevel(logging.INFO)
        fmt = logging.Formatter('%(asctime)s %(filename)s %(levelname)s %(message)s')
        fh.setFormatter(fmt)
        self.logger.handlers.append(fh)
       
    def logon(self,account,pwd,lang='0',belong='0'):
        if type(account).__name__!='str' or type(pwd).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        targetUrl = self.urlUtil.getLoginUrl()
        encodedArgs = {'account': str(account),'force':'1','pwd':str(pwd),"clientType":'5',"version":self.urlUtil.getVersionNum()}
        body = json.dumps(encodedArgs).encode('utf-8')
        headers={'Content-Type': 'application/json','lang': lang,'belong': belong}
        return self.doPost(targetUrl,body=body,headers=headers)
    
    def getToken(self,account,pwd):
        loginInfo = self.logon(account, pwd)
        return loginInfo['data']['token']
    
    def login(self,account,pwd,lang='0',belong='0'):
        '''
                    函数功能：用户登录信息以及语言设置写入文件
                    
                    参数：
        
           account(str)：账号
           
           pwd(str)：密码
           
           __lang(str)：语言:简体":"0","英文":"1","繁体":"2"  
           
                    返回值：无
        '''
        if lang not in ['0','1','2']:
            lang='0'
        if belong not in ['0','1']:
            belong='0'
        loginInfo = self.logon(account, pwd, lang, belong)
        if loginInfo['code']==0:
            self.logger.info(loginInfo['msg'])
            self.writeToken(loginInfo['data']['token'],lang,belong)
        else:
            self.writeToken('',lang,belong)          
        
        
    def getTokenFromFile(self):
        fo = open('token.txt','r') 
        alist = fo.readlines()
        fo.close()
        if len(alist)==0:
            self.logger.error('The account has been offline, please sign in again.')
            return False
        return alist
    
    def getListDbs(self):
        '''
                    函数功能：查看已购买的数据库列表
                    
                    参数：无
                    
                    返回值：json字符串
        '''
        targetUrl = self.urlUtil.getListDbsUrl()
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'belong':alist[2]}
        dataDic = self.doGet(targetUrl, headers=headers)
        if dataDic['code']==0:
            dataList = dataDic['data']
            return dataList
    
    def getListFields(self,tableName):
        '''
                    函数功能：查看数据表的字段列表
                    
                    参数：无
                    
                    返回值：json字符串
        '''
        if type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        targetUrl = self.urlUtil.getListFieldsUrl()
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'belong':alist[2]}
        targetUrl=targetUrl+'?table='+tableName
        dataDic = self.doGet(targetUrl, headers=headers)
        if dataDic['code']==0:
            dataList = dataDic['data']
            return dataList

    
    def getListTables(self,databaseName):
        '''
                    函数功能：查看已购买的数据库表列表
                    
                    参数：无
                    
                    返回值：json字符串
        '''
        if type(databaseName).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        #字符编码强制转换
        databaseName=parse.quote(databaseName)
        
        targetUrl = self.urlUtil.getListTablesUrl()
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'belong':alist[2]}
        listFieldsUrl=targetUrl+'?dbName='+databaseName
        dataDic = self.doGet(listFieldsUrl, headers=headers)
        if dataDic['code']==0:
            dataList = dataDic['data']
            return dataList
    
    def preview(self,tableName):
        '''
                    函数功能：查看已购买的数据库表列表
                    
                    参数：
                    
            
            tableName(str)：表名称
                    
                    返回值：json字符串
        '''
        if type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        targetUrl = self.urlUtil.getPreviewUrl()
        body =json.dumps( {'table':tableName}).encode('utf-8')
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'Content-Type': 'application/json'}
        dataDic = self.doPost(targetUrl,body=body,headers=headers)
        if dataDic['code']==0:
            dataList = dataDic['data']['previewDatas']
            return dataList


    def query(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：查询已购买的数据库表数据
                    
                    参数：
                    
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            tableName(str)：大包表名称
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
                    
                    返回值：json字符串
        '''
        if type(columns).__name__!='list' or type(condition).__name__!='str'  or type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return 
        targetUrl = self.urlUtil.getQueryUrl()
        __http = urllib3.PoolManager()
        bodyDic={'columns': columns,'condition':condition,'table':tableName}
        if _other_args!=None and len(_other_args)>=1:
            startTime=_other_args[0]
            if startTime!=None and len(startTime)!=0:
                if not self.is_valid_date(startTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return False
                bodyDic['startTime']=startTime
        if _other_args!=None and len(_other_args)>=2:
            endTime=_other_args[1]
            if endTime!=None and len(endTime)!=0:
                if not self.is_valid_date(endTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return False
                bodyDic['endTime']=endTime
        body =json.dumps(bodyDic).encode('utf-8')
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'Content-Type': 'application/json'}
        dataDic = self.doPost(targetUrl,body=body,headers=headers)
        if dataDic['code']==0:
            dataList = dataDic['data']['previewDatas']
            return dataList

 
    '''
    Created on 2022年5月18日
    添加DataFrame类型接口
    @author: shuai.he
    '''
    def to_df(self,dic):
        try:
            cols = list(dic[0].keys())
            row_num = len(dic)
            lists = [[dic[0][key]]*row_num for key in cols]
            for i in range(1,row_num):
                for j,key in enumerate(cols):
                    lists[j][i] = dic[i][key]
            df = pd.DataFrame({col:value for col,value in zip(cols, lists)})
            return df
        except Exception:
            return

    def query_df(self,columns,condition,tableName,*_other_args):
        dataList = self.query(columns,condition,tableName,*_other_args)
        if dataList == False:
            return dataList
        return self.to_df(dataList)
        
                
 
 
    def queryCount(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：查询已购买的数据库表数据总数
                    
                    参数：
                    
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            tableName(str)：大包表名称
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
                    
                    返回值：数据总数
        '''
        if type(columns).__name__!='list' or type(condition).__name__!='str'  or type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return   
        targetUrl = self.urlUtil.getQueryCountUrl()
        __http = urllib3.PoolManager()
        bodyDic={'columns': columns,'condition':condition,'table':tableName}
        if _other_args!=None and len(_other_args)>=1:
            startTime=_other_args[0]
            if startTime!=None and len(startTime)!=0:
                if not self.is_valid_date(startTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return 
                bodyDic['startTime']=startTime
        if _other_args!=None and len(_other_args)>=2:
            endTime=_other_args[1]
            if endTime!=None and len(endTime)!=0:
                if not self.is_valid_date(endTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return 
                bodyDic['endTime']=endTime
        body =json.dumps(bodyDic).encode('utf-8')
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'Content-Type': 'application/json'}
        dataDic = self.doPost(targetUrl,body=body,headers=headers)
        if dataDic['code']==0:
            self.logger.info('The total number of data obtained by condition is ' + str(dataDic['data']))
            return dataDic['data']

        
    
    def pack(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：向接口发送打包指令
                    
                    参数：
                    
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
            
            tableName(str)：大包表名称
            
                    返回值：json字符串
        '''
        if type(columns).__name__!='list' or type(condition).__name__!='str'  or type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return 
        #字符编码强制转换
        #condition=parse.quote(condition)
        
        targetUrl = self.urlUtil.getPackUrl()
        __http = urllib3.PoolManager()
        bodyDic={'columns': columns,'condition':condition,'table':tableName}
        bodyDic={'columns': columns,'condition':condition,'table':tableName}
        if _other_args!=None and len(_other_args)>=1:
            startTime=_other_args[0]
            if startTime!=None and len(startTime)!=0:
                if not self.is_valid_date(startTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return
                bodyDic['startTime']=startTime
        if _other_args!=None and len(_other_args)>=2:
            endTime=_other_args[1]
            if endTime!=None and len(endTime)!=0:
                if not self.is_valid_date(endTime):
                    self.logger.error("Date format error, should be 'yyyy-MM-dd'")
                    return
                bodyDic['endTime']=endTime
        body =json.dumps(bodyDic).encode('utf-8')
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'Content-Type': 'application/json'}
        return self.doPost(targetUrl,body=body,headers=headers)
    
    def packSignCodeWriteToFile(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：获取打包结果的状态码写入本地文件
                    
                    参数：
                    
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
            
            tableName(str)：大包表名称
            
                    返回值：打包结果的状态码
        '''
        
        if type(columns).__name__!='list' or type(condition).__name__!='str'  or type(tableName).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        packRequest = self.pack(columns,condition,tableName,*_other_args)
        if packRequest['code']==0:
            signCode = packRequest['data']
            fo = open('signCode.txt','w')
            fo.write(signCode)
            fo.close()
            self.logger.info('packaging return code：signCode='+signCode)
            return signCode
        return False
    
    def getPackResultBySignCode(self,signCode):
        '''
                    函数功能：获取服务器上面的打包文件
                    
                    参数：
                    
            signCode(str)：获取打包结果的状态码
            
                    返回值：json字符串
        '''
        if type(signCode).__name__!='str':
            self.logger.error('Parameter type error')
            return False
        targetUrl = self.urlUtil.getPackResultUrl()
        alist = self.getTokenFromFile()
        headers={'Lang': alist[1].strip('\n'),'Token':alist[0].strip('\n'),'belong':alist[2]}
        listFieldsUrl=targetUrl+'/'+signCode
        return self.doGet(listFieldsUrl, headers=headers)
    
    def getSignCodeFromFile(self):
        try:
            fo = open('signCode.txt','r') 
            signCode = fo.read()
            fo.close()
        except FileNotFoundError  as err:
            self.logger.error(format(err))
            return None
        if signCode==None or len(signCode)==0:
            self.logger.error('用户未发送打包指令')
            return None
        self.logger.info('打包返回码：signCode='+signCode)
        return signCode
    
    
    def getPackResult(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：发送打包指令成功之后，每10秒向接口发送获取服务器上面的打包文件的请求，直到返回状态status等于1，说明打包成功
                    
                    返回打包文件的下载地址filePath，函数会自动下载到默认文件夹c:\\csmardata\\zip\\下面
                    
                    参数：
        
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
            
            tableName(str)：大包表名称
        
                    返回值：json字符串
        '''
        #字符编码强制转换
        #condition=parse.quote(condition)
        
        signCode = self.packSignCodeWriteToFile(columns,condition,tableName,*_other_args)
        if signCode==False:
            self.logger.error('Failed to get the packaging return code [signCode]')
            return
        while True:
            packResult = self.getPackResultBySignCode(signCode)
            if packResult['code'] != 0:
                break
            if packResult['data']['status']=='1':
                filePath= str(packResult['data']['filePath'])
                self.logger.info('Package successfully. File size is：'+str(packResult['data']['fileSize']))
                # self.logger.info('生成文件下载请求'+filePath)
                _http = urllib3.PoolManager()
                r = _http.request('GET', filePath)
                data = r.data
                downloadDir = 'c:\\csmardata\\zip\\'
                if os.path.exists(downloadDir)==False:
                    os.makedirs(downloadDir)
                f = open(downloadDir+signCode+'.zip', 'wb+')
                f.write(data)
                f.close()
                self.logger.info('Generate the local file path as ：c:/csmardata/zip/'+signCode+'.zip')
                break
            elif packResult['data']['status']=='0':
                self.logger.info('An exception occurred during file packaging,please check the query criteria')
                break
            else:
                self.logger.info('Downloading, please wait patiently!')
            time.sleep(10)

    def getPackResultExt(self,columns,condition,tableName,*_other_args):
        '''
                    函数功能：发送打包指令成功之后，每10秒向接口发送获取服务器上面的打包文件的请求，直到返回状态status等于1，说明打包成功
                    
                    返回打包文件的下载地址filePath，函数会自动下载到默认文件夹c:\\csmardata\\zip\\下面
                    
                    参数：
        
            columns(list)：打包的字段
            
            condition(str)：查询条件
            
            startTime(str)：开始时间
            
            endTime(str)：结束时间
            
            tableName(str)：大包表名称
        
                    返回值：json字符串
        '''
        #字符编码强制转换
        #condition=parse.quote(condition)
        
        signCode = self.packSignCodeWriteToFile(columns,condition,tableName,*_other_args)
        if signCode==False:
            return 
        ws = create_connection(self.urlUtil.getWebsocketUrl())
        tokenList = self.getTokenFromFile()
        token = tokenList[0].strip('\n')
        bodyDic = {'outlineId': signCode, 'status': 'start', 'token': token}
        cred_text = json.dumps(bodyDic)
        ws.send(cred_text)
        self.logger.info("downloading...")
        while True:
            result = ws.recv()
            packResult = json.loads(result)
            if packResult['code'] != 0:
                print('\n')
                self.logger.error(packResult['msg'])
                break
            if packResult['data']['status'] == '1':
                self.process_bar(packResult['data']['percentage'])
                filePath = str(packResult['data']['filePath'])
                print('\n')
                self.logger.info('Package successfully. File size is：' +
                                 str(packResult['data']['fileSize']))
                # self.logger.info('生成文件下载请求' + filePath)
                _http = urllib3.PoolManager()
                r = _http.request('GET', filePath)
                data = r.data
                downloadDir = 'c:\\csmardata\\zip\\'
                if os.path.exists(downloadDir) == False:
                    os.makedirs(downloadDir)
                zipPath = downloadDir + signCode + '.zip'
                f = open(zipPath, 'wb+')
                f.write(data)
                f.close()
                self.logger.info('Generate the local file path as ：' + zipPath)
                break
            elif packResult['data']['status']=='0':
                self.logger.info('An exception occurred during file packaging,please check the query criteria')
                break
            else:
                self.process_bar(packResult['data']['percentage'])          
    
    def unzipSingle(self,fileName):
        '''
                    函数功能：解压单个文件到默认文件夹c:\\csmardata\\
                    
                    参数：
        
           fileName(str)：完整文件名称fileName。例如C:\\csmardata\\zip\\662348367897071616.zip
           
                    返回值：无
        '''
        #解压单个文件到默认文件夹c:\\csmardata\\
        zf = zipfile.ZipFile(fileName)
        (filePath,tempFileName)=os.path.split(fileName)
        (preFileName,extendsName)=os.path.splitext(tempFileName)
        destDir = 'c:\\csmardata\\'+preFileName
        try:
            zf.extractall(path=destDir)
            self.logger.info('The file has been successfully extracted to the directory:'+destDir)
        except RuntimeError as e:
            self.logger.error(e)
        zf.close()   
    
    def loadData(self,fileName,count=10000):
        '''
                    函数功能：解析的csv文件，把结果存入dataList。例如C:\\csmardata\\BETA_Ybeta.csv
                    
                    参数：
        
           fileName(str)：完整文件名称fileName。例如C:\\csmardata\\BETA_Ybeta.csv
           
           count(int)：截取的记录数
           
                    返回值：无
        '''
        if fileName.endswith('.csv')==False:
            self.logger.error('Wrong file type that needs to be resolved')
            return False
            
        if type(fileName).__name__!='str' or type(count).__name__!='int':
            self.logger.error('Parameter type error')
            return False
        with open(fileName, encoding='utf-8-sig') as csvf:
            reader = csv.reader(csvf)
            for i,row in enumerate(reader):
                tempList=[]
                for tempStr in row:
                    if tempStr.find('\t')!=-1:
                        tempList.append(tempStr.replace('\t',''))
                    else:
                        tempList.append(tempStr)
                self.dataList.append(tempList)
                if count!=None and i==count:
                    self.logger.info(tempList )
                    break
                else: 
                    self.logger.info(tempList )
        
        return self.dataList
        
    #由于需求调整，该功能合并到loadData里面去了。   
    def getDataList(self):
        '''
                    函数功能：返回loadData方法解析的结果集
                    
                    参数：无
                    
                    返回值：dataList
        '''
        return self.dataList


    def is_valid_date(self, str_date):  # 判断是否为自定义日期格式;
        try:
            time.strptime(str_date, "%Y-%m-%d")
            return True
        except Exception:
            return False

    def writeToken(self, token, lang, belong):
        fo = open('token.txt','w')
        alist=[token+'\n',str(lang)+'\n',str(belong)]
        fo.writelines(alist)
        fo.close()

    def process_bar(self, i):
        a = int((i + 1) / 5)
        b = 20 - a
        sys.stdout.write('\r|%s%s|%d%%' % (a * '▇', b * ' ', i))
        sys.stdout.flush()
        time.sleep(.1)

    def doPost(self, targetUrl, body=None, headers=None):
        __http = urllib3.PoolManager()
        resp = __http.request('POST', targetUrl, body=body, headers=headers)
        dataDic =json.loads(resp.data.decode('utf-8'))
        if dataDic['code']==-3004:
            self.logger.error("The account has been offline, please sign in again.")
        elif dataDic['code']==-1:
            self.logger.error("System error.")
        elif dataDic['code']==0:
            return dataDic 
        else:
            self.logger.error(dataDic['msg'])
        return dataDic

    def doGet(self, targetUrl, headers=None):
        __http = urllib3.PoolManager()
        resp = __http.request('GET', targetUrl, headers=headers)
        dataDic =json.loads(resp.data.decode('utf-8'))
        if dataDic['code']==-3004:
            self.logger.error("The account has been offline, please sign in again.")
        elif dataDic['code']==-1:
            self.logger.error("System error.")
        elif dataDic['code']==0:
            return dataDic 
        else:
            self.logger.error(dataDic['msg'])
        return dataDic
