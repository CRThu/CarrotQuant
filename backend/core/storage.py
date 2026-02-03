
import os
from pathlib import Path
from typing import List
import pandas as pd
import duckdb
from loguru import logger

class DuckDBStorage:
    """
    通用数据存储管理器 (Simplified Path Structure)
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

        # 1. 构建 Hive 路径: table/year=YYYY/
        target_dir = self.root_dir / table_name / f"year={year}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{year}-{month:02d}.parquet"

        conn = duckdb.connect()
        try:
            conn.register('input_df', df)
            
            # 2. 定义强制 schema 转换
            cast_columns = [
                "CAST(trade_date AS DATE) AS trade_date",
                "CAST(sector_name AS VARCHAR) AS sector_name",
                "CAST(open AS DOUBLE) AS open",
                "CAST(close AS DOUBLE) AS close",
                "CAST(high AS DOUBLE) AS high",
                "CAST(low AS DOUBLE) AS low",
                "CAST(volume AS UBIGINT) AS volume",
                "CAST(amount AS DOUBLE) AS amount",
                "CAST(amplitude AS DOUBLE) AS amplitude",
                "CAST(pct_change AS DOUBLE) AS pct_change",
                "CAST(change_amount AS DOUBLE) AS change_amount",
                "CAST(turnover AS DOUBLE) AS turnover"
            ]
            
            # 3. 写入 (ZSTD 压缩)
            sql = f"""
            COPY (
                SELECT 
                    {', '.join(cast_columns)}
                FROM input_df
                ORDER BY trade_date, sector_name
            ) TO '{str(file_path).replace(os.sep, '/')}' 
            (FORMAT 'parquet', COMPRESSION 'ZSTD');
            """
            
            if file_path.exists():
                file_path.unlink()
                
            conn.execute(sql)
            logger.info(f"已保存月度数据: {file_path}")

        except Exception as e:
            logger.error(f"Save month failed {year}-{month}: {e}")
            raise e
        finally:
            conn.close()
