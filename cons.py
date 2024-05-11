""" 
常量列表
"""
import os

# 内置文件
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
TRADE_RECORD_PATH = os.path.join(ROOT_PATH, "trade-record")  # 股票历史交易记录文件保存目录
DATA_PACKAGE_PATH = os.path.join(ROOT_PATH, "data-package")  # 数据包保存目录
TEST_CONDITION_PATH = os.path.join(ROOT_PATH, "test-condition")  # 测试条件保存目录
tmp_path = os.path.join(ROOT_PATH, "tmp-download")  # 临时下载文件保存目录

if not os.path.exists(DATA_PACKAGE_PATH):
    os.mkdir(DATA_PACKAGE_PATH)
if not os.path.exists(TRADE_RECORD_PATH):
    os.mkdir(TRADE_RECORD_PATH)
if not os.path.exists(TEST_CONDITION_PATH):
    os.mkdir(TEST_CONDITION_PATH)
if not os.path.exists(tmp_path):
    os.mkdir(tmp_path)

# 内置文件
INDICATOR_ROE_FROM_2007 = os.path.join(ROOT_PATH, "data-package", "indicator-roe-from-2007.sqlite3")  # ROE数据文件
CURVE_CSV = os.path.join(ROOT_PATH, "data-package", "curve.csv")  # 香港10年政府债券收益率
TEST_CONDITION_SQLITE3 = os.path.join(ROOT_PATH, "test-condition", "test-condition.sqlite3")  # 测试条件数据文件
LIST_CSV = os.path.join(ROOT_PATH, "data-package", "stock-list.csv")  # 港股通股票列表

# 数据库表名
ROE_TABLE = "indicators"  # indicator-roe-from-2000.sqlite3中的表
