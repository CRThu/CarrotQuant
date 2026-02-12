import os
import duckdb
import numpy as np
from loguru import logger
from typing import List, Dict, Optional
from datetime import date

from core.exceptions import DataNotFoundError
from core.config import settings
from models.market import TableData, MarketDataContainer

from core.sql_builder import build_pivot_sql, build_snapshot_query_sql
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
        """
        元数据审计：基于配置中心 (TABLE_REGISTRY) 进行 O(1) 路径检查。
        严禁扫描整个 data 目录。
        """
        from models.market import TABLE_REGISTRY
        metadata = {}

        for t_name, config in TABLE_REGISTRY.items():
            storage_type = config.get("storage_type", "partition")
            
            # 路径定义
            if storage_type == "snapshot":
                # 快照模式：路径固定为 data/{table}/{table}.parquet
                t_path = os.path.join(settings.DATA_DIR, t_name, f"{t_name}.parquet")
            else:
                # 分区模式：路径为 data/{table}/
                t_path = os.path.join(settings.DATA_DIR, t_name)

            if not os.path.exists(t_path):
                continue

            try:
<<<<<<< Updated upstream
                # 使用中心化 SQL 构建器进行审计统计
                from core.sql_builder import build_metadata_sql
                sql = build_metadata_sql(t_path)
=======
                # 配置驱动：根据 TABLE_REGISTRY 判定是否为时序表
                is_timeseries = (config.get("load_mode") == "matrix") or (config.get("storage_type") == "partition")
                from core.sql_builder import build_metadata_sql
                sql = build_metadata_sql(t_path, is_timeseries=is_timeseries)
>>>>>>> Stashed changes
                res = self.conn.execute(sql).fetchone()
                
                if res and res[2] > 0: # row_count > 0
                    metadata[t_name] = {
                        "start_date": res[0],
                        "end_date": res[1],
                        "row_count": res[2],
                        "path": t_path,
                        "storage_type": storage_type
                    }
            except Exception as e:
                logger.warning(f"审计表 {t_name} 失败 (路径: {t_path}): {e}")
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

            storage_type = meta[t_name].get("storage_type", "partition")
            
            if storage_type == "snapshot":
                # 快照模式：直接使用单一 Parquet 文件
                parquet_paths = [meta[t_name]["path"]]
            else:
                # 分区模式：根据年份加载多个文件
                t_dir = meta[t_name]["path"]
                year_dirs = [d for d in os.listdir(t_dir) if d.startswith("year=")]
                years = []
                for d in year_dirs:
                    try:
                        y = int(d.split("=")[1])
                        if start_date.year <= y <= end_date.year:
                            years.append(y)
                    except: continue

                if not years:
                    raise DataNotFoundError(t_name, start_date, end_date)
                parquet_paths = [os.path.join(t_dir, f"year={y}", "*.parquet") for y in years]

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
<<<<<<< Updated upstream
        支持 1-to-1 与 1-to-many 自动切换
        """
        id_col, val_col = config["id_col"], config["val_col"]
        # 注意：此处 filters 暂时硬编码为 stock_code，未来可根据 config 扩展
        sql = build_select_sql(t_name, paths, [id_col, val_col], str(start_date), str(end_date), {id_col: symbols} if symbols else None)
        
        res = self.conn.execute(sql).fetchnumpy()
        id_arr, val_arr = res[id_col], res[val_col]

        if len(id_arr) == 0:
            return TableData(name=t_name, data={})

        # 自动探测是否为一对多映射
        # 如果 ID 数量多于唯一 ID 数量，则进入一对多逻辑
        mapping = {}
        unique_ids = set(id_arr)
        if len(unique_ids) < len(id_arr):
            # 一对多：Dict[ID, List[Value]]
            for k, v in zip(id_arr, val_arr):
                if k not in mapping:
                    mapping[k] = []
                mapping[k].append(v)
        else:
            # 一对一：Dict[ID, Value]
            mapping = {k: v for k, v in zip(id_arr, val_arr)}
=======
        支持“智能查询”：载入所有 fields 并存储为 List[Dict]
        """
        # 1. 字段获取 (Strict Mode: 必须配置 fields)
        fields = config["fields"]
            
        # 2. 构建查询
        # 注意：快照表仅有一个路径
        parquet_path = paths[0]
        
        # 如果指定了 symbols，假设是指 id_col (仅作简单过滤，完全过滤由 TableData 接手)
        filters = None
        id_col = config.get("id_col") 
        if symbols and id_col:
            filters = {id_col: symbols}

        sql = build_snapshot_query_sql(parquet_path, fields, filters)
>>>>>>> Stashed changes
        
        # 3. 执行查询并转换为 List[Dict]
        # fetch_arrow_table().to_pylist() 是最高效的转换方式之一
        records = self.conn.execute(sql).fetch_arrow_table().to_pylist()

        # 4. 封装
        # 映射表中 timeline 通常为空，symbols 可以是所有记录的 id_col 集合
        all_symbols = [r.get(id_col) for r in records] if id_col else []
        
        return TableData(name=t_name, symbols=all_symbols, data=records)

data_manager = DataManager()
