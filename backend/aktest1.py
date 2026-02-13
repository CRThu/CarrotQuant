# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "akshare",
#     "requests",
#     "curl-cffi",
# ]
# ///

import akshare as ak
import requests
from curl_cffi.requests import Session

# 1. 创建一个持久的模拟浏览器 Session
# impersonate="chrome110" 会自动模拟 Chrome 的 SSL 指纹和默认 Headers
session = Session(impersonate="chrome120")

# 2. 全局替换 requests 的 get 和 post 方法
# 这样后面所有的 akshare 调用都会默认使用这个模拟 session
requests.get = session.get
requests.post = session.post

df = ak.stock_zh_a_hist(symbol="000001")
print(df)