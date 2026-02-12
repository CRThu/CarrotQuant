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

def build_snapshot_query_sql(parquet_path: str, columns: list, filters: dict = None) -> str:
    """
    专门拼装快照查询 SQL (无 trade_date 约束)
    """
    col_sql = ", ".join(columns)
    safe_path = parquet_path.replace("\\", "/")
    
    where_clause = "1=1"
    if filters:
        for k, v in filters.items():
            if v is None: continue
            if isinstance(v, list):
                val_list = ", ".join([f"'{i}'" for i in v])
                where_clause += f" AND {k} IN ({val_list})"
            else:
                where_clause += f" AND {k} = '{v}'"

    return f"""
        SELECT {col_sql}
        FROM read_parquet('{safe_path}')
        WHERE {where_clause}
    """

def build_metadata_sql(table_path: str, is_timeseries: bool = True) -> str:
    """
    拼装元数据审计 SQL：提取时间范围和行数统计
    is_timeseries=False 时不查询 trade_date（快照表无此列）
    """
    # 路径处理：兼容 Windows 路径并支持单文件
    safe_path = table_path.replace("\\", "/")
    if safe_path.endswith(".parquet"):
        from_clause = f"read_parquet('{safe_path}')"
    else:
        from_clause = f"read_parquet('{safe_path}/**/*.parquet', hive_partitioning=true)"
    
    if is_timeseries:
        date_cols = "MIN(trade_date)::VARCHAR AS start_date, MAX(trade_date)::VARCHAR AS end_date"
    else:
        date_cols = "NULL AS start_date, NULL AS end_date"

    return f"""
        SELECT 
            {date_cols}, 
            COUNT(*)::INTEGER as row_count 
        FROM {from_clause}
    """

from models.market import DATA_SCHEMA

def build_save_parquet_sql(source_df_name: str, actual_columns: list, order_by: str, file_path: str) -> str:
    """
    专门拼装分区数据(save_month)存储 SQL。
    包含 year 分区键的处理。
    """
    cast_exprs = []
    for col in actual_columns:
        if col == "year":
            cast_exprs.append(f'CAST("year" AS INTEGER) AS "year"')
            continue
        if col not in DATA_SCHEMA:
            raise KeyError(f"字段 '{col}' 未在 DATA_SCHEMA 中注册。")
        sql_type = DATA_SCHEMA[col]
        cast_exprs.append(f'CAST("{col}" AS {sql_type}) AS "{col}"')

    safe_path = file_path.replace("\\", "/")
    return f"""
        COPY (
            SELECT {', '.join(cast_exprs)}
            FROM {source_df_name}
            ORDER BY {order_by}
        ) TO '{safe_path}' (FORMAT 'parquet', COMPRESSION 'ZSTD');
    """

def build_save_table_sql(source_df_name: str, actual_columns: list, order_by: str, file_path: str) -> str:
    """
    专门拼装单表快照(save_snapshot)存储 SQL。
    """
    cast_exprs = []
    for col in actual_columns:
        if col not in DATA_SCHEMA:
            raise KeyError(f"字段 '{col}' 未在 DATA_SCHEMA 中注册。")
        sql_type = DATA_SCHEMA[col]
        cast_exprs.append(f'CAST("{col}" AS {sql_type}) AS "{col}"')

    safe_path = file_path.replace("\\", "/")
    return f"""
        COPY (
            SELECT {', '.join(cast_exprs)}
            FROM {source_df_name}
            ORDER BY {order_by}
        ) TO '{safe_path}' (FORMAT 'parquet', COMPRESSION 'ZSTD');
    """
