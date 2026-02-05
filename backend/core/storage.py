
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

        # 1. 获取注册好的 Schema (按数据类型区分)
        # 兼容旧逻辑或通过 table_name 路由
        data_type = "sector" if "sector" in table_name else "stock"
        cast_columns = TABLE_REGISTRY.get(data_type)
        if not cast_columns:
            raise ValueError(f"无法为表 {table_name} 匹配 Schema (类型: {data_type})")

        # 2. 构建目标路径 (Hive 分区结构)
        target_dir = self.root_dir / table_name / f"year={year}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{year}-{month:02d}.parquet"

        conn = duckdb.connect()
        try:
            conn.register('input_df', df)
            
            # 自动识别排序列
            cols_names = [c.split(" AS ")[-1].strip() for c in cast_columns]
            order_by = "trade_date"
            if "sector_name" in cols_names:
                order_by = "trade_date, sector_name"
            elif "stock_code" in cols_names:
                order_by = "trade_date, stock_code"

            # 3. 写入 (ZSTD 压缩) - 显式选择字段以确保 Parquet 纯净 (不含分区列 year)
            sql = f"""
            COPY (
                SELECT 
                    {', '.join(cast_columns)}
                FROM input_df
                ORDER BY {order_by}
            ) TO '{str(file_path).replace(os.sep, '/')}' 
            (FORMAT 'parquet', COMPRESSION 'ZSTD');
            """
            
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
