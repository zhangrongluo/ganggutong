""" 
创建和更新香港10年期政府债券收益率数据库 港股通年度ROE 历史交易记录数据库
"""

import os
import time
import sqlite3
import pandas as pd
from DrissionPage import SessionOptions, SessionPage, ChromiumOptions, ChromiumPage, WebPage
from cons import tmp_path, CURVE_CSV, INDICATOR_ROE_FROM_2007, ROE_TABLE
from stocklist import get_all_stocks_in_HK
from concurrent.futures import ThreadPoolExecutor

def download_10y_bond_curve_data():
    """
    下载香港10年期政府债券收益率数据,保存为csv文件.
    从2010-01-11开始到最新更新日期数据,源文件每月更新一次.
    """
    co = ChromiumOptions().headless()
    page = ChromiumPage(addr_or_opts=co)
    url = "https://www.hkgb.gov.hk/sc/statistics/statistic.html"  # 香港10年期政府债券收益率
    page.get(url)
    ele = page.ele("tag:a@href=/en/others/documents/T090403.xls")
    mission = ele.click.to_download(tmp_path, "curve.xls")
    mission.wait()  # 下载
    df = pd.read_excel(tmp_path + "/curve.xls", skiprows=30, usecols=[0, 11], names=["date", "close"])
    df = df[df["close"] != "-"]  # 去除空行
    df = df.dropna()  # 去除空值
    df = df.sort_values(by="date", ascending=False)  # 按日期降序排列
    df["date"] = df["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df["close"] = df["close"].apply(lambda x: round(x, 4))
    os.remove(tmp_path + "/curve.xls")
    df.to_csv(CURVE_CSV, index=False)

def get_ROE_indicators_from_xueqiu(code: str, count: int, type: str):
    """
    从雪球网获取港股通ROE财务指标信息
    :param code: 股票代码, 例如: 00001 或 00144
    :param count: 财务指标的期数, 例如: 5
    :param type: 财务指标类型Q1、Q2、Q3、Q4、all,分别代表第一二三四季度及全部期间
    :return: 返回一个字典, 键为年度名称, 值为年度ROE值
    """
    url = "https://stock.xueqiu.com/v5/stock/finance/hk/indicator.json"
    params = {
        'symbol': code,
        'type': f'{type}',
        'is_detail': 'true',
        'count': f'{count}',
        'timestamp': ''
    }
    session = SessionPage()
    session.get(url='https://xueqiu.com/')  # 获取cookies
    session.get(url=url, params=params)  # 获取数据
    response = session.json  # 获取json数据
    result = {}  # 定义返回值
    for indicator in response['data']['list']:
        if indicator['roe'][0] is not None:
            result[indicator['report_name']] = round(indicator['roe'][0], 4)
    return result

def get_stock_name_by_code_from_xueqiu(code: str):
    """
    通过股票代码获取股票名称
    :param code: 股票代码, 例如: '00001' or '00144'
    :return: 股票名称
    NOTE:
    这个函数从雪球获取数据不是最优解,但是相比通过港股通来解决,还是不错的选择.
    """
    url = "https://stock.xueqiu.com/v5/stock/finance/hk/indicator.json"
    params = {
        'symbol': code,
        'type': 'Q4',
        'is_detail': 'true',
        'count': '1',
        'timestamp': ''
    }
    session = SessionPage()
    session.get(url='https://xueqiu.com/')  # 获取cookies
    session.get(url=url, params=params)  # 获取数据
    response = session.json  # 获取json数据
    return response['data']['quote_name']

def create_ROE_indicators_table_from_2007(code: str):
    """ 
    创建2007至上年年度ROE至indicator_roe_from_2007.sqlite3中
    :param code: 股票代码, 例如: '00001' or '00144'
    :return: None
    """
    start_year = '20071231'
    end_year = str(time.localtime().tm_year-1)+'1231'
    count = int(end_year[0:4]) - 2007 + 1
    roe_dict = get_ROE_indicators_from_xueqiu(code=code, count=count, type='Q4')
    periods = pd.date_range(start=start_year, end=end_year, freq='YE').strftime("%Y%年报")  # 补齐缺失的年度
    for period in periods:
        if period not in roe_dict.keys():
            roe_dict[period] = None
    for key in list(roe_dict.keys()):  # 删除2000以前的键值对
        if int(key[0:4]) < 2007:
            del roe_dict[key]
    roe_dict = dict(sorted(roe_dict.items(), key=lambda x: x[0], reverse=True))  # 按照键降序排序
    con = sqlite3.connect(INDICATOR_ROE_FROM_2007)
    with con:
        fields = list(roe_dict.keys())

        sql = f"""CREATE TABLE IF NOT EXISTS '{ROE_TABLE}' (""" +  "\n" + \
        "stockcode TEXT PRIMARY KEY," + "\n" + "stockname TEXT," + "\n"
        for index, item in enumerate(fields):
            if index == len(fields) - 1:  
                sql += f"Y{item[0:4]} REAL" + "\n"
            else:  
                sql += f"Y{item[0:4]} REAL," + "\n"
        sql += ")"
        con.execute(sql)  # 创建表

        stockname = get_stock_name_by_code_from_xueqiu(code=code)
        insert_value =[code, stockname] + list(roe_dict.values())
        sql = f"""INSERT OR IGNORE INTO '{ROE_TABLE}' VALUES (""" + "\n" 
        for index, item in enumerate(insert_value):
            if index == len(insert_value) - 1:
                sql += "?" + "\n"
            else:
                sql += "?," + "\n"
        sql += ")"
        con.execute(sql, insert_value)  # 插入数据


if __name__ == "__main__":
    con = sqlite3.connect(INDICATOR_ROE_FROM_2007)
    sql = f"""SELECT stockcode FROM {ROE_TABLE} """
    with con:
        df = pd.read_sql_query(sql, con)
        codes_sql = df["stockcode"].tolist()
    df = get_all_stocks_in_HK(date="2022-11-17")
    codes_hk = df["code"].tolist()
    codes = [code for code in codes_hk if code not in codes_sql]
    print(f"需要更新的股票代码: {codes}")
    with ThreadPoolExecutor() as executor:
        executor.map(create_ROE_indicators_table_from_2007, codes)