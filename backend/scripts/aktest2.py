# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "akshare",
#     "requests",
# ]
# ///

import requests
import akshare as ak

# 1. 准备你的 Cookie 和想要模拟的 User-Agent
EM_COOKIE = ""
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"

# 2. 核心：猴子补丁 (Monkey Patching)
# 我们保存原始的 Session.request 方法
original_session_request = requests.Session.request


def patched_request(self, method, url, *args, **kwargs):
    """
    这个函数会拦截 requests 所有的请求
    """
    # 获取或初始化 headers
    headers = kwargs.get('headers', {})
    if headers is None:
        headers = {}
    else:
        headers = headers.copy()  # 拷贝一份，避免影响 akshare 原有的逻辑

    # 强制注入 Cookie 和 User-Agent
    headers['Cookie'] = EM_COOKIE
    headers['User-Agent'] = UA

    # 回填回参数
    kwargs['headers'] = headers

    # 调用原始的方法执行请求
    return original_session_request(self, method, url, *args, **kwargs)


# 替换 requests 库的全局 Session 请求方法
# 这样即使 akshare 内部执行 s = requests.Session()，s.get 也会被拦截
requests.Session.request = patched_request

# 3. 运行测试
try:
    print("正在通过拦截器请求东方财富接口...")
    df = ak.stock_board_industry_name_em()
    print(df)
except Exception as e:
    print(f"请求失败: {e}")