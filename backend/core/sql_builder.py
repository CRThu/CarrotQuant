import os

def build_pivot_sql(table_name: str, 
                    parquet_paths: list, 
                    id_col: str, 
                    fields: list, 
                    start_date: str, 
                    end_date: str, 
                    symbols: list = None) -> str:
    """
    专门拼装 DuckDB 原生 PIVOT 语句，用于高性能矩阵加载
    """
    path_sql = ", ".join([f"'{p}'" for p in parquet_paths])
    sym_sql_list = ", ".join([f"'{s}'" for s in symbols]) if symbols else "SELECT DISTINCT " + id_col + " FROM read_parquet([" + path_sql + "], hive_partitioning=true)"
    
    # 构造聚合表达式 (PIVOT 需要聚合函数)
    value_exprs = ", ".join([f"FIRST({f}) AS {f}" for f in fields])
    
    # EXCLUDE 掉索引列，防止 PIVOT 结果冲突
    pivot_sql = f"""
        PIVOT (
            SELECT CAST(trade_date AS VARCHAR) as t, {id_col} as s, * EXCLUDE(trade_date, {id_col}, year)
            FROM read_parquet([{path_sql}], hive_partitioning=true)
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        ) ON s IN ({sym_sql_list}) USING {value_exprs} GROUP BY t
        ORDER BY t
    """
    return pivot_sql

def build_select_sql(table_name: str, 
                     parquet_paths: list, 
                     columns: list, 
                     start_date: str, 
                     end_date: str, 
                     filters: dict = None) -> str:
    """
    专门拼装普通 SELECT 语句，用于简单映射加载
    """
    path_sql = ", ".join([f"'{p}'" for p in parquet_paths])
    col_sql = ", ".join(columns)
    
    where_clause = f"trade_date >= '{start_date}' AND trade_date <= '{end_date}'"
    if filters:
        for k, v in filters.items():
            if isinstance(v, list):
                val_list = ", ".join([f"'{i}'" for i in v])
                where_clause += f" AND {k} IN ({val_list})"
            else:
                where_clause += f" AND {k} = '{v}'"
                
    select_sql = f"""
        SELECT {col_sql}
        FROM read_parquet([{path_sql}], hive_partitioning=true)
        WHERE {where_clause}
    """
    return select_sql

def build_metadata_sql(table_path: str) -> str:
    """
    拼装元数据审计 SQL：提取精确的时间范围和行数统计
    """
    # 统一使用 read_parquet 深度扫描，hive_partitioning=true 识别分区结构
    return f"""
        SELECT 
            MIN(trade_date)::VARCHAR as start_date, 
            MAX(trade_date)::VARCHAR as end_date, 
            COUNT(*)::INTEGER as row_count 
        FROM read_parquet('{table_path}/**/*.parquet', hive_partitioning=true)
    """

from models.market import DATA_SCHEMA

def build_save_parquet_sql(source_df_name: str, actual_columns: list, order_by: str, file_path: str) -> str:
    """
    拼装数据存储 SQL：通过 COPY 指令将 DataFrame 写入 Parquet。
    采用动态 CAST 机制，确保类型严肃性与保真。
    """
    cast_exprs = []
    for col in actual_columns:
        # 强制 Schema 注册检查
        if col not in DATA_SCHEMA:
            raise KeyError(f"字段 '{col}' 未在 DATA_SCHEMA 中注册，请先在 models/market.py 中定义其类型。存储已拒绝以保证 Schema 严肃性。")
        
        # 安全性处理：使用双引号包裹字段名以防特殊字符
        sql_type = DATA_SCHEMA[col]
        cast_exprs.append(f'CAST("{col}" AS {sql_type}) AS "{col}"')

    # 路径处理：将 Windows 的 \ 替换为 / 以防 DuckDB 报错
    safe_path = file_path.replace("\\", "/")
    
    return f"""
        COPY (
            SELECT 
                {', '.join(cast_exprs)}
            FROM {source_df_name}
            ORDER BY {order_by}
        ) TO '{safe_path}' 
        (FORMAT 'parquet', COMPRESSION 'ZSTD');
    """
