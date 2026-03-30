"""
CSMAR SDK 原始接口测试脚本
直接调用底层 CsmarService，不经过 MCP 包装层，用于查看原始异常信息
"""
import json
import traceback
from csmarapi.CsmarService import CsmarService


def test_login(service: CsmarService, account: str, password: str):
    print("\n" + "=" * 60)
    print("测试 1: 登录接口 (logon)")
    print("=" * 60)
    try:
        response = service.logon(account, password, lang='0', belong='0')
        print(f"响应类型: {type(response)}")
        print(f"原始响应: {json.dumps(response, ensure_ascii=False, indent=2)}")
        if response and response.get('code') == 0:
            token = response.get('data', {}).get('token', '')
            if token:
                print(f"\n登录成功! Token: {token[:20]}...")
                service.writeToken(token, '0', '0')
                print("已将新 Token 写入 token.txt")
                return True
            else:
                print("登录成功但未返回token")
                return False
        else:
            print(f"\n登录失败!")
            return False
    except Exception as e:
        print(f"\n异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")
        return False


def test_list_dbs(service: CsmarService):
    print("\n" + "=" * 60)
    print("测试 2: 获取已购买数据库列表 (getListDbs)")
    print("=" * 60)
    try:
        result = service.getListDbs()
        print(f"响应类型: {type(result)}")
        if result is False:
            print("返回 False (可能token无效)")
        elif result is None:
            print("返回 None")
        else:
            print(f"成功获取 {len(result) if isinstance(result, list) else '?'} 个数据库")
            print(f"原始响应: {json.dumps(result, ensure_ascii=False, indent=2)[:500]}...")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_list_tables(service: CsmarService, db_name: str):
    print("\n" + "=" * 60)
    print(f"测试 3: 获取数据库表列表 (getListTables) - 数据库: {db_name}")
    print("=" * 60)
    try:
        result = service.getListTables(db_name)
        print(f"响应类型: {type(result)}")
        if result is False:
            print("返回 False (可能token无效或数据库不存在)")
        elif result is None:
            print("返回 None")
        else:
            print(f"成功获取 {len(result) if isinstance(result, list) else '?'} 个表")
            if isinstance(result, list) and len(result) > 0:
                print(f"前3个表: {json.dumps(result[:3], ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_list_fields(service: CsmarService, table_name: str):
    print("\n" + "=" * 60)
    print(f"测试 4: 获取表字段列表 (getListFields) - 表: {table_name}")
    print("=" * 60)
    try:
        result = service.getListFields(table_name)
        print(f"响应类型: {type(result)}")
        if result is False:
            print("返回 False (可能token无效或表不存在)")
        elif result is None:
            print("返回 None")
        else:
            print(f"成功获取 {len(result) if isinstance(result, list) else '?'} 个字段")
            if isinstance(result, list) and len(result) > 0:
                print(f"前3个字段: {json.dumps(result[:3], ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_query_count(service: CsmarService, table_name: str, columns: list):
    print("\n" + "=" * 60)
    print(f"测试 5: 查询数据总数 (queryCount) - 表: {table_name}")
    print("=" * 60)
    try:
        result = service.queryCount(columns, "1=1", table_name)
        print(f"响应类型: {type(result)}")
        print(f"原始响应: {result}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_preview(service: CsmarService, table_name: str):
    print("\n" + "=" * 60)
    print(f"测试 6: 预览数据 (preview) - 表: {table_name}")
    print("=" * 60)
    try:
        result = service.preview(table_name)
        print(f"响应类型: {type(result)}")
        if result is False:
            print("返回 False (可能token无效或表不存在)")
        elif result is None:
            print("返回 None")
        else:
            print(f"成功获取 {len(result) if isinstance(result, list) else '?'} 行预览数据")
            if isinstance(result, list) and len(result) > 0:
                print(f"第一行数据: {json.dumps(result[0], ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_raw_do_get(service: CsmarService):
    print("\n" + "=" * 60)
    print("测试 7: 原始 GET 请求 (doGet) - 直接调用底层方法")
    print("=" * 60)
    try:
        target_url = service.urlUtil.getListDbsUrl()
        print(f"请求URL: {target_url}")
        
        alist = service.getTokenFromFile()
        if alist is False or not alist:
            print("无法获取token，请先登录")
            return
        
        headers = {
            'Lang': alist[1].strip('\n'), 
            'Token': alist[0].strip('\n'), 
            'belong': alist[2].strip('\n') if len(alist) > 2 else '0'
        }
        print(f"请求Headers: {headers}")
        
        result = service.doGet(target_url, headers=headers)
        print(f"响应类型: {type(result)}")
        print(f"原始响应: {json.dumps(result, ensure_ascii=False, indent=2)[:1000]}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def test_raw_do_post(service: CsmarService, table_name: str):
    print("\n" + "=" * 60)
    print("测试 8: 原始 POST 请求 (doPost) - 直接调用底层方法")
    print("=" * 60)
    try:
        target_url = service.urlUtil.getQueryCountUrl()
        print(f"请求URL: {target_url}")
        
        alist = service.getTokenFromFile()
        if alist is False or not alist:
            print("无法获取token，请先登录")
            return
        
        headers = {
            'Lang': alist[1].strip('\n'), 
            'Token': alist[0].strip('\n'), 
            'Content-Type': 'application/json'
        }
        print(f"请求Headers: {headers}")
        
        body_dic = {
            'columns': ['*'],
            'condition': '1=1',
            'table': table_name
        }
        body = json.dumps(body_dic).encode('utf-8')
        print(f"请求Body: {body.decode('utf-8')}")
        
        result = service.doPost(target_url, body=body, headers=headers)
        print(f"响应类型: {type(result)}")
        print(f"原始响应: {json.dumps(result, ensure_ascii=False, indent=2)[:1000]}")
    except Exception as e:
        print(f"异常类型: {type(e).__name__}")
        print(f"异常信息: {e}")
        print(f"完整堆栈:\n{traceback.format_exc()}")


def main():
    print("=" * 60)
    print("CSMAR SDK 原始接口测试脚本")
    print("=" * 60)
    
    import sys
    if len(sys.argv) < 3:
        print("\n使用方法: python test_csmar_raw.py <account> <password>")
        print("示例: python test_csmar_raw.py your_account your_password")
        sys.exit(1)
    
    account = sys.argv[1]
    password = sys.argv[2]
    
    print(f"\n账号: {account}")
    print(f"密码: {'*' * len(password)}")
    
    service = CsmarService()
    
    login_success = test_login(service, account, password)
    
    if login_success:
        test_list_dbs(service)
        test_raw_do_get(service)
        
        test_db = "股票市场交易数据库"
        test_list_tables(service, test_db)
        
        test_table = "TRD_Dalyr"
        test_list_fields(service, test_table)
        test_query_count(service, test_table, ['*'])
        test_preview(service, test_table)
        test_raw_do_post(service, test_table)
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
