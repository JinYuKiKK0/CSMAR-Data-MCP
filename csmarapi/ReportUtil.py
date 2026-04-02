#coding=utf-8
'''
Created on 2020年1月9日
输出整齐表格数据
@author: jiahang.miao
'''
import prettytable as pt
class ReportUtil(object):
    '''
            输出整齐表格数据
    '''


    def __init__(self,dataList):
        '''
                    初始化的时候即打印数据
        '''
        try:
            if type(dataList).__name__!='list' or len(dataList)==0:
                return 
            listKeys = [i for i in dataList[0].keys()]
            tb = pt.PrettyTable()
            tb.field_names = listKeys
            for j in dataList:
                listValues = [i for i in j.values()]
                tb.add_row(listValues)
                
            print(tb)
        except BaseException as e:
            print(e)