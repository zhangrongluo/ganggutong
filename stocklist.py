""" 
管理和获取港股通(深沪)股票列表清单
"""
import re
from DrissionPage import ChromiumOptions, ChromiumPage
import pandas as pd
import os
import datetime
import time
from cons import ROOT_PATH, tmp_path

def get_stock_info_SHtoHK() -> pd.DataFrame:
    """ 
    港股通(沪)最新股票列表
    :return: 下载文件的DataFrame
    """
    co = ChromiumOptions().headless()
    page = ChromiumPage(addr_or_opts=co)
    url = "https://www.sse.com.cn/services/hkexsc/disclo/eligible/"  # 港股通清单(上海)
    page.get(url)
    tmp_name = f"stockinfo-shtohk-{datetime.date.today()}.xls"
    mission = page.ele("#download").click.to_download(tmp_path, tmp_name)
    mission.wait(3)
    df = pd.read_excel(os.path.join(tmp_path, tmp_name), dtype={"证券代码": str})
    os.remove(os.path.join(tmp_path, tmp_name))
    df.columns = ["code", "en_name", "ch_name", "class"]
    df["code"] = df["code"].str.strip()
    df["en_name"] = df["en_name"].str.strip()
    df["ch_name"] = df["ch_name"].str.strip()
    df["class"] = df["class"].str.strip()
    return df

def get_stock_info_SZtoHK() -> pd.DataFrame:
    """
    港股通(深)最新股票列表
    :return: 下载文件的DataFrame
    """
    co = ChromiumOptions().headless()
    page = ChromiumPage(addr_or_opts=co)
    url = "https://www.szse.cn/szhk/hkbussiness/underlylist/"  # 港股通清单(深圳)
    page.get(url)
    tmp_name = f"stockinfo-sztohk-{datetime.date.today()}"
    ele = page.ele("tag:div@class=pull-right report-excel").ele("tag:a")
    mission = ele.click.to_download(tmp_path, tmp_name)
    mission.wait(3)
    df = pd.read_excel(os.path.join(tmp_path, tmp_name + ".xlsx"), dtype={"证券代码": str})
    os.remove(os.path.join(tmp_path, tmp_name + ".xlsx"))
    df.columns = ["code", "ch_name", "en_name"]
    df["code"] = df["code"].str.strip()
    df["ch_name"] = df["ch_name"].str.strip()
    df["en_name"] = df["en_name"].str.strip()
    return df

def get_stock_adinfo_SHtoHK() -> pd.DataFrame:
    """
    港股通(沪)最新调进调出股票列表
    :return: 下载文件的DataFrame
    """
    co = ChromiumOptions().headless()
    page = ChromiumPage(addr_or_opts=co)
    url = "https://www.sse.com.cn/services/hkexsc/disclo/eligiblead/"  # 港股通进出清单(上海)
    page.get(url)
    tmp_name_ad = f"stockadinfo-shtohk-{datetime.date.today()}.xls"
    mission = page.ele("#download").click.to_download(tmp_path, tmp_name_ad)
    mission.wait(3)
    df = pd.read_excel(os.path.join(tmp_path, tmp_name_ad), dtype={"证券代码": str, "生效日期": str})
    os.remove(os.path.join(tmp_path, tmp_name_ad))
    df.columns = ["code", "en_name", "ch_name", "class", "in_out", "date"]
    df["code"] = df["code"].str.strip()
    df["en_name"] = df["en_name"].str.strip()
    df["ch_name"] = df["ch_name"].str.strip()
    df["class"] = df["class"].str.strip()
    df["in_out"] = df["in_out"].str.strip()
    df["date"] = df["date"].str.strip()
    return df

def get_stock_adinfo_SZtoHK() -> pd.DataFrame:
    """
    港股通(深)最新调进调出股票列表
    :return: 下载文件的DataFrame
    """
    co = ChromiumOptions().headless()
    page = ChromiumPage(addr_or_opts=co)
    url = "https://www.szse.cn/szhk/hkbussiness/underlyadjust/index.html"  # 港股通进出清单(深圳)
    page.get(url)
    tmp_name_ad = f"stockadinfo-sztohk-{datetime.date.today()}"
    ele = page.ele("tag:div@class=pull-right report-excel").ele("tag:a")
    mission = ele.click.to_download(tmp_path, tmp_name_ad)
    mission.wait(3)
    df = pd.read_excel(os.path.join(tmp_path, tmp_name_ad + ".xlsx"), dtype={"证券代码": str, "调整生效日期": str})
    os.remove(os.path.join(tmp_path, tmp_name_ad + ".xlsx"))
    df.columns = ["code", "ch_name", "en_name", "in_out", "date"]
    df["code"] = df["code"].str.strip()
    df["ch_name"] = df["ch_name"].str.strip()
    df["en_name"] = df["en_name"].str.strip()
    df["in_out"] = df["in_out"].str.strip()
    df["date"] = df["date"].str.strip()
    return df

def get_stock_list_specified_date_SHtoHK(date):
    """
    获取指定日期start港股通(沪)股票列表清单
    :param date: 指定日期
    :return: DataFrame
    NOTE:
    港股通(沪)最早日期为2014-11-17
    """
    if date < "2014-11-17":
        return pd.DataFrame(columns=["code", "en_name", "ch_name", "class"])
    ad_df = get_stock_adinfo_SHtoHK()
    list_df = get_stock_info_SHtoHK()
    # 以list_df为基准，根据ad_df中的in_out和date列，倒推list_df中的内容至start日状态:
    # 选取ad_df中start日后的调进调出数据df,遍历df, 若in_out为"调进"，则将list_df中对应code的行删除, 
    # 若in_out为"调出"，则将ad_df中对应code的行code en_name ch_name class添加至list_df
    df = ad_df[ad_df["date"] > date]
    for index, row in df.iterrows():
        if row["in_out"] == "调进":
            list_df = list_df[list_df["code"] != row["code"]]
        else:
            # 从ad中找到对应code的数据，取出code, en_name, ch_name, class列
            tmp = ad_df[ad_df["code"] == row["code"]][["code", "en_name", "ch_name", "class"]]
            list_df = pd.concat([list_df, tmp], ignore_index=True)
    list_df.drop_duplicates(inplace=True, keep="last", subset=["code"])
    list_df.sort_values(by="code", inplace=True)
    return list_df

def get_stock_list_specified_date_SZtoHK(date):
    """ 
    获取指定日期start港股通(深)股票列表清单
    :param date: 指定日期
    :return: DataFrame
    NOTE:
    港股通(深)最早日期为2016-12-05
    """
    if date < "2016-12-05":
        return pd.DataFrame(columns=["code", "ch_name", "en_name"])
    ad_df = get_stock_adinfo_SZtoHK()
    list_df = get_stock_info_SZtoHK()
    # 以list_df为基准，根据ad_df中的in_out和date列，倒推list_df中的内容至start日状态:
    # 选取ad_df中start日后的调进调出数据df,遍历df, 若in_out为"调入"，则将list_df中对应code的行删除, 
    # 若in_out为"调出"，则将ad_df中对应code的行code en_name ch_name添加至list_df
    df = ad_df[ad_df["date"] > date]
    for index, row in df.iterrows():
        if row["in_out"] == "调入":
            list_df = list_df[list_df["code"] != row["code"]]
        else:
            # 从ad中找到对应code的数据，取出code, en_name, ch_name, class列
            tmp = ad_df[ad_df["code"] == row["code"]][["code", "en_name", "ch_name"]]
            list_df = pd.concat([list_df, tmp], ignore_index=True)
    list_df.drop_duplicates(inplace=True, keep="last", subset=["code"])
    list_df.sort_values(by="code", inplace=True)
    return list_df

def get_all_stocks_in_HK(date):
    """ 
    获取指定日期date港股通(沪深)股票列表清单
    :param date: 指定日期
    :return: DataFrame, columns = ["code", "en_name", "ch_name", "class"]
    """
    sh_stocks_df = get_stock_list_specified_date_SHtoHK(date=date)
    sz_stocks_df = get_stock_list_specified_date_SZtoHK(date=date)
    all_stocks_df = pd.concat([sh_stocks_df, sz_stocks_df], ignore_index=True)
    all_stocks_df.drop_duplicates(inplace=True, subset=["code"])
    all_stocks_df.sort_values(by="code", inplace=True)
    return all_stocks_df

if __name__ == "__main__":
    date = "2024-05-06"
    tmp = get_all_stocks_in_HK(date=date)
    tmp.to_csv(f"hk-stock-list-{date}.csv", index=False)
