
import os
from pathlib import Path
from typing import List
import pandas as pd
import duckdb
from loguru import logger

from models.market import TABLE_REGISTRY

class DuckDBStorage:
    """
    通用数据存储管理器 (动态 Schema 版)
    """
    
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def save_month(self, df: pd.DataFrame, table_name: str, year: int, month: int):
        """
        保存单月全量数据。
        Path: {root}/{table_name}/year={year}/{year}-{month}.parquet
        """
        if df.empty:
            logger.warning(f"[{table_name}] {year}-{month} 数据为空，跳过保存")
            return

        # 1. 获取实际字段
        actual_fields = df.columns.tolist()

        # 2. 构建目标路径 (Hive 分区结构)
        target_dir = self.root_dir / table_name / f"year={year}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{year}-{month:02d}.parquet"

        conn = duckdb.connect()
        try:
            conn.register('input_df', df)
            
            # 自动识别排序列
            order_by = "trade_date"
            if "sector_name" in actual_fields:
                order_by = "trade_date, sector_name"
            elif "stock_code" in actual_fields:
                order_by = "trade_date, stock_code"

            # 3. 写入 - 调用中心化构建器
            from core.sql_builder import build_save_parquet_sql
            sql = build_save_parquet_sql(
                source_df_name='input_df', 
                actual_columns=actual_fields, 
                order_by=order_by, 
                file_path=str(file_path)
            )
            
            logger.debug(f"执行存储 SQL (表: {table_name}):\n{sql}")

            if file_path.exists():
                file_path.unlink()
                
            conn.execute(sql)
            logger.info(f"已保存月度数据 [{table_name}]: {file_path}")

        except Exception as e:
            logger.error(f"保存月度数据失败 [{table_name}] {year}-{month}: {e}")
            raise e
        finally:
            conn.close()
