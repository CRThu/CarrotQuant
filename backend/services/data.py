import os
import duckdb
import numpy as np
from loguru import logger
from typing import List, Dict, Optional
from datetime import date

from core.exceptions import DataNotFoundError
from core.config import settings
from models.market import TableData, MarketDataContainer

from core.sql_builder import build_pivot_sql, build_select_sql
from services.utils.processor import ffill_2d, zero_fill

class DataManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
            cls._instance.conn = duckdb.connect(database=":memory:")
        return cls._instance

    def initialize(self):
        """初始化数据环境"""
        logger.info(f"配置驱动双轨加载引擎启动。根目录: {settings.DATA_DIR}")

    def get_storage_metadata(self) -> Dict[str, Dict]:
        """元数据审计：直接从数据文件中提取统计信息"""
        metadata = {}
        if not os.path.exists(settings.DATA_DIR): 
            return metadata

        for entry in os.scandir(settings.DATA_DIR):
            if entry.is_dir():
                t_name, t_path = entry.name, entry.path
                try:
                    # 使用中心化 SQL 构建器进行审计统计
                    from core.sql_builder import build_metadata_sql
                    sql = build_metadata_sql(t_path)
                    res = self.conn.execute(sql).fetchone()
                    
                    if res and res[0] is not None:
                        metadata[t_name] = {
                            "start_date": res[0],
                            "end_date": res[1],
                            "row_count": res[2],
                            "path": t_path
                        }
                except Exception as e:
                    logger.warning(f"审计表 {t_name} 失败: {e}")
                    continue
        return metadata

    def load_market_data(self, 
                          table_names: List[str], 
                          start_date: date, 
                          end_date: date,
                          symbols: Optional[List[str]] = None) -> MarketDataContainer:
        """
        主入口: 根据 TABLE_REGISTRY 配置分流至具体加载轨道
        """
        from models.market import TABLE_REGISTRY
        all_tables_data = {}
        meta = self.get_storage_metadata()

        for t_name in table_names:
            if t_name not in meta:
                raise DataNotFoundError(t_name)
            
            config = TABLE_REGISTRY.get(t_name)
            if not config:
                logger.warning(f"表 {t_name} 未在 TABLE_REGISTRY 中注册，跳过。")
                continue

            # 确定有效的 Parquet 路径
            # 直接扫描目录获取年份子文件夹
            year_dirs = [d for d in os.listdir(meta[t_name]["path"]) if d.startswith("year=")]
            years = []
            for d in year_dirs:
                try:
                    y = int(d.split("=")[1])
                    if start_date.year <= y <= end_date.year:
                        years.append(y)
                except: continue

            if not years:
                raise DataNotFoundError(t_name, start_date, end_date)
            parquet_paths = [os.path.join(meta[t_name]["path"], f"year={y}", "*.parquet") for y in years]

            # 双轨分流
            if config["load_mode"] == "matrix":
                results = self._load_matrix_track(t_name, config, parquet_paths, start_date, end_date, symbols)
                all_tables_data.update(results)
            elif config["load_mode"] == "mapping":
                result = self._load_mapping_track(t_name, config, parquet_paths, start_date, end_date, symbols)
                all_tables_data[t_name] = result

        return MarketDataContainer(all_tables_data)

    def _load_matrix_track(self, t_name, config, paths, start_date, end_date, symbols) -> Dict[str, TableData]:
        """
        矩阵轨：高性能 PIVOT 加载行情字段
        """
        id_col, fields = config["id_col"], config["fields"]
        
        # 1. 生成 SQL 并执行
        sql = build_pivot_sql(t_name, paths, id_col, fields, str(start_date), str(end_date), symbols)
        raw_dict = self.conn.execute(sql).fetchnumpy()
        
        if 't' not in raw_dict or len(raw_dict['t']) == 0:
            return {}

        # 1. Key 归一化：清除所有 Key 中的双引号，应对 DuckDB 自动添加引号的情况
        clean_dict = {k.replace('"', ''): v for k, v in raw_dict.items()}

        # 2. 精准反推 Symbol：通过后缀匹配从列名中还原标的代码
        # 严禁使用 split，因为标的代码本身可能包含特殊符号
        field_suffix = f"_{fields[0]}"
        raw_symbols = set()
        for k in clean_dict.keys():
            if k.endswith(field_suffix):
                # 截取后缀前的部分即为 Symbol
                s_name = k[:-len(field_suffix)]
                raw_symbols.add(s_name)
        unique_symbols = sorted(list(raw_symbols))
        
        # 3. 字段平铺与矩阵重组
        timeline = clean_dict['t'].astype(str).tolist()
        track_results = {}
        for f in fields:
            # 创建 2D 矩阵 (Time, Symbol)
            mat = np.full((len(timeline), len(unique_symbols)), np.nan)
            
            # 使用归一化后的 Key 直接访问，彻底消除逻辑抖动
            for s_idx, s_val in enumerate(unique_symbols):
                k = f"{s_val}_{f}"
                if k in clean_dict:
                    val = clean_dict[k]
                    if hasattr(val, 'mask'):
                        val = val.astype(np.float64).filled(np.nan)
                    mat[:, s_idx] = val
            
            # 4. 向量化清洗
            if f in config.get("zerofill_cols", []):
                mat = zero_fill(mat)
            elif f in config.get("ffill_cols", []):
                mat = ffill_2d(mat)
                
            flat_name = f"{t_name}_{f}"
            track_results[flat_name] = TableData(name=flat_name, timeline=timeline, symbols=unique_symbols, data=mat)
            
        return track_results

    def _load_mapping_track(self, t_name, config, paths, start_date, end_date, symbols) -> TableData:
        """
        映射轨：获取标签映射 (如 股票->板块)
        """
        id_col, val_col = config["id_col"], config["val_col"]
        sql = build_select_sql(t_name, paths, [id_col, val_col], str(start_date), str(end_date), {"stock_code": symbols} if symbols else None)
        
        res = self.conn.execute(sql).fetchnumpy()
        # 转换为字典: {id: val}
        mapping = {k: v for k, v in zip(res[id_col], res[val_col])}
        
        return TableData(name=t_name, data=mapping)

data_manager = DataManager()
