import asyncio
import os
import sys
import pandas as pd
from datetime import datetime

# 保证能找到 models 和 services 模块
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from services.market_manager import market_manager
from models.market import MarketDownloadRequest

async def test_real_download():
    """实战下行测试：验证重构后的下载系统是否真正可用"""
    # 我们测试三个典型场景
    test_scenarios = [
        # 1. 分区表 - 个股日线 (后复权)
        {"table_name": "cn_stock_em_daily_adj", "symbols": ["000001"], "months": ["202501"]},
        # 2. 分区表 - 板块日线 (用户刚改了 hfq)
        {"table_name": "cn_sector_em_daily_adj", "symbols": ["小金属"], "months": ["202501"]},
        # 3. 快照表 - 全量股票信息
        {"table_name": "cn_stock_em", "symbols": None, "months": None}
    ]
    
    for scene in test_scenarios:
        print(f"\n>>> 启动任务: {scene['table_name']}...")
        try:
            req = MarketDownloadRequest(**scene)
            task_id = await market_manager.start_market_download_task(req)
            print(f"任务已创建: {task_id}")
            
            # 等待任务完成 (带超时)
            timeout = 60 # 60秒
            start_time = datetime.now()
            while True:
                task = market_manager.get_task(task_id)
                if not task:
                    print("错误: 找不到任务对象")
                    break
                    
                if task.status in ["COMPLETED", "FAILED"]:
                    print(f"任务结束! 状态: {task.status} | 消息: {task.message}")
                    if task.status == "FAILED":
                        print(f"详情: {task.message}")
                    break
                
                if (datetime.now() - start_time).total_seconds() > timeout:
                    print("测试超时!")
                    break
                    
                await asyncio.sleep(2)
        except Exception as e:
            print(f"启动任务异常: {e}")

if __name__ == "__main__":
    asyncio.run(test_real_download())
