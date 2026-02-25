# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "akshare"
# ]
# ///

import akshare as ak

df = ak.stock_zh_a_hist(symbol="000001")
print(df)