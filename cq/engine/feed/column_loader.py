"""
列式按需加载器 (ColumnDataLoader)

负责从 Parquet (Hive 分区结构) 或 CSV 文件中按列加载数据，
并利用 Polars 进行时间轴与股票 ID 的极速数据对齐，构建 C-Contiguous 的 2D NumPy 矩阵块。
"""

from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
import numpy as np
import polars as pl


class LazyCustomFields:
    """
    按需懒透视字典 (Lazy Custom Fields)
    首次访问字段时才利用 Polars 将其 pivot 成 (T, N) 的 2D C-Contiguous NumPy 矩阵并进行缓存。
    """

    def __init__(
        self,
        df: Optional[pl.DataFrame],
        all_timestamps: List[str],
        all_symbols: List[str],
        initial_fields: Optional[Dict[str, np.ndarray]] = None,
    ):
        self._df = df
        self.all_timestamps = all_timestamps
        self.all_symbols = all_symbols
        self._cache: Dict[str, np.ndarray] = initial_fields or {}

    def __getitem__(self, key: str) -> np.ndarray:
        return self.get(key)

    def get(self, key: str, default: Optional[np.ndarray] = None) -> np.ndarray:
        if key in self._cache:
            return self._cache[key]

        if self._df is not None and key in self._df.columns:
            pivoted = self._df.pivot(
                on="symbol",
                index="datetime",
                values=key,
                aggregate_function="first",
            ).sort("datetime")

            missing_syms = set(self.all_symbols) - set(pivoted.columns)
            for sym in missing_syms:
                pivoted = pivoted.with_columns(pl.lit(np.nan).alias(sym))

            matrix_df = pivoted.select(self.all_symbols)
            mat = np.ascontiguousarray(matrix_df.to_numpy(), dtype=np.float64)
            self._cache[key] = mat
            return mat

        if default is not None:
            return default
        raise KeyError(f"自定义列/特征 '{key}' 不存在。")

    def __contains__(self, key: str) -> bool:
        if key in self._cache:
            return True
        return self._df is not None and key in self._df.columns

    def keys(self) -> List[str]:
        keys_set = set(self._cache.keys())
        if self._df is not None:
            base_cols = {"symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount", "back_adj_factor"}
            for col in self._df.columns:
                if col not in base_cols:
                    keys_set.add(col)
        return list(keys_set)


class AdjMarketData:
    """
    复权行情数据视角 (支持动态懒求值代理与显式矩阵)
    """

    def __init__(
        self,
        close: Optional[np.ndarray] = None,
        open_p: Optional[np.ndarray] = None,
        high: Optional[np.ndarray] = None,
        low: Optional[np.ndarray] = None,
        parent: Optional["MarketData"] = None,
    ):
        self._parent = parent
        self._explicit_close = close
        self._explicit_open = open_p
        self._explicit_high = high
        self._explicit_low = low

    @property
    def close(self) -> np.ndarray:
        if self._explicit_close is not None:
            return self._explicit_close
        if self._parent is not None:
            if self._parent.adj_factor is None:
                return self._parent.close
            return self._parent.close * self._parent.adj_factor
        raise AttributeError("AdjMarketData 既无显式矩阵也无关联 parent MarketData。")

    @property
    def open(self) -> np.ndarray:
        if self._explicit_open is not None:
            return self._explicit_open
        if self._parent is not None:
            if self._parent.adj_factor is None:
                return self._parent.open
            return self._parent.open * self._parent.adj_factor
        return self.close

    @property
    def high(self) -> np.ndarray:
        if self._explicit_high is not None:
            return self._explicit_high
        if self._parent is not None:
            if self._parent.adj_factor is None:
                return self._parent.high
            return self._parent.high * self._parent.adj_factor
        return self.close

    @property
    def low(self) -> np.ndarray:
        if self._explicit_low is not None:
            return self._explicit_low
        if self._parent is not None:
            if self._parent.adj_factor is None:
                return self._parent.low
            return self._parent.low * self._parent.adj_factor
        return self.close


class MarketData:
    """
    全市场行情矩阵容器 (MarketData)

    属性:
        timestamps: 时间戳数组 (T,)
        symbols: 标的代码列表 (N,)
        open, high, low, close: 原始未复权价格矩阵 (T, N) - 真实资金交割使用
        raw_close: 原始未复权收盘价别名 (T, N)
        adj: AdjMarketData 复权价格子对象 - 提供 adj.close, adj.open 供策略计算指标使用 (懒计算视图)
        adj_factor: 复权因子矩阵 (T, N) 可选，无复权需求时为 None
        volume: 成交量矩阵 (T, N)
        amount: 成交额矩阵 (T, N)
        is_tradable: 可交易标志矩阵 (T, N)
        custom_fields: 自定义特征字典/懒加载容器 LazyCustomFields
    """

    def __init__(
        self,
        timestamps: np.ndarray,
        symbols: List[str],
        open_price: np.ndarray,
        high_price: np.ndarray,
        low_price: np.ndarray,
        close_price: np.ndarray,
        raw_close_price: Optional[np.ndarray] = None,
        adj_close_price: Optional[np.ndarray] = None,
        adj_open_price: Optional[np.ndarray] = None,
        adj_high_price: Optional[np.ndarray] = None,
        adj_low_price: Optional[np.ndarray] = None,
        volume: Optional[np.ndarray] = None,
        amount: Optional[np.ndarray] = None,
        is_tradable: Optional[np.ndarray] = None,
        adj_factor: Optional[np.ndarray] = None,
        custom_fields: Optional[Union[Dict[str, np.ndarray], LazyCustomFields]] = None,
    ):
        self.timestamps = timestamps
        self.symbols = symbols
        self.shape = open_price.shape  # (T, N)
        self.n_steps, self.n_symbols = self.shape
        self.n_stocks = self.n_symbols  # 兼容属性

        # 1. 原始未复权价格矩阵 (真实资金扣除使用)
        if raw_close_price is not None:
            self.close = np.ascontiguousarray(raw_close_price, dtype=np.float64)
        else:
            self.close = np.ascontiguousarray(close_price, dtype=np.float64)

        self.raw_close = self.close
        self.open = np.ascontiguousarray(open_price, dtype=np.float64)
        self.high = np.ascontiguousarray(high_price, dtype=np.float64)
        self.low = np.ascontiguousarray(low_price, dtype=np.float64)

        # 2. 复权子视角与复权因子
        if adj_factor is not None:
            self.adj_factor = np.ascontiguousarray(adj_factor, dtype=np.float64)
        else:
            self.adj_factor = None

        if adj_close_price is not None:
            adj_c = np.ascontiguousarray(adj_close_price, dtype=np.float64)
            adj_o = np.ascontiguousarray(adj_open_price, dtype=np.float64) if adj_open_price is not None else self.open
            adj_h = np.ascontiguousarray(adj_high_price, dtype=np.float64) if adj_high_price is not None else self.high
            adj_l = np.ascontiguousarray(adj_low_price, dtype=np.float64) if adj_low_price is not None else self.low
            self.adj = AdjMarketData(close=adj_c, open_p=adj_o, high=adj_h, low=adj_l)
        else:
            self.adj = AdjMarketData(parent=self)

        # 3. 辅助量价矩阵
        if volume is not None:
            self.volume = np.ascontiguousarray(volume, dtype=np.float64)
        else:
            self.volume = np.zeros(self.shape, dtype=np.float64)

        if amount is not None:
            self.amount = np.ascontiguousarray(amount, dtype=np.float64)
        else:
            self.amount = np.zeros(self.shape, dtype=np.float64)

        if is_tradable is not None:
            self.is_tradable = np.ascontiguousarray(is_tradable, dtype=np.bool_)
        else:
            if np.all(self.volume == 0.0):
                self.is_tradable = np.ascontiguousarray(~np.isnan(self.close) & (self.close > 0), dtype=np.bool_)
            else:
                self.is_tradable = np.ascontiguousarray(~np.isnan(self.close) & (self.volume > 0), dtype=np.bool_)

        if custom_fields is not None:
            self.custom_fields = custom_fields
        else:
            self.custom_fields = LazyCustomFields(df=None, all_timestamps=list(timestamps), all_symbols=symbols)

    def __getitem__(self, key: str) -> np.ndarray:
        return self.get_custom_field(key)

    def get_custom_field(self, key: str) -> np.ndarray:
        if isinstance(self.custom_fields, LazyCustomFields):
            return self.custom_fields[key]
        if key in self.custom_fields:
            return self.custom_fields[key]
        raise KeyError(f"MarketData 中未找到自定义列 '{key}'。")

    def __repr__(self) -> str:
        return f"<MarketData: T={self.n_steps}, N={self.n_symbols}, symbols={len(self.symbols)}>"


# 别名兼容
MarketDataContainer = MarketData


class ColumnDataLoader:
    """
    列式行情与复权因子按需加载器 (支持 Parquet 与 CSV 格式的数据源)
    """

    @classmethod
    def load_parquet(
        cls,
        path: Union[str, Path],
        adj_factor_path: Optional[Union[str, Path]] = None,
        columns: Optional[List[str]] = None,
        custom_columns: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> MarketData:
        """
        从 Parquet 目录装载全量行情矩阵。
        """
        path = Path(path)
        base_cols = {"symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"}

        df = pl.scan_parquet(str(path / "**" / "*.parquet"))
        schema_names = df.collect_schema().names()

        requested_custom = set(columns or []) | set(custom_columns or [])
        if requested_custom:
            target_cols = list(base_cols | requested_custom | {"symbol", "datetime"})
        else:
            target_cols = schema_names

        if start_date:
            df = df.filter(pl.col("datetime") >= start_date)
        if end_date:
            df = df.filter(pl.col("datetime") <= end_date)
        if symbols:
            df = df.filter(pl.col("symbol").is_in(symbols))

        available_cols = [col for col in target_cols if col in schema_names]
        df = df.select(available_cols).collect()

        if df.is_empty():
            raise ValueError(f"No parquet data found in {path} with specified filters.")

        if adj_factor_path:
            adj_path = Path(adj_factor_path)
            adj_df = pl.scan_parquet(str(adj_path / "**" / "*.parquet"))
            if start_date:
                adj_df = adj_df.filter(pl.col("datetime") >= start_date)
            if end_date:
                adj_df = adj_df.filter(pl.col("datetime") <= end_date)
            if symbols:
                adj_df = adj_df.filter(pl.col("symbol").is_in(symbols))
            adj_df = adj_df.select(["symbol", "datetime", "back_adj_factor"]).collect()

            df = df.join(adj_df, on=["symbol", "datetime"], how="left")

        return cls._build_container_from_df(df, custom_columns=custom_columns)

    @classmethod
    def scan_parquet_chunks(
        cls,
        path: Union[str, Path],
        adj_factor_path: Optional[Union[str, Path]] = None,
        columns: Optional[List[str]] = None,
        custom_columns: Optional[List[str]] = None,
        partition_by: str = "year",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ):
        """
        从 Parquet 目录按 Hive 分区 (如按年 year 或按月 month) 惰性分块生成器。
        """
        path = Path(path)
        scan_lazy = pl.scan_parquet(str(path / "**" / "*.parquet"))

        if start_date:
            scan_lazy = scan_lazy.filter(pl.col("datetime") >= start_date)
        if end_date:
            scan_lazy = scan_lazy.filter(pl.col("datetime") <= end_date)
        if symbols:
            scan_lazy = scan_lazy.filter(pl.col("symbol").is_in(symbols))

        # 提取时间切片
        dt_df = scan_lazy.select(pl.col("datetime").cast(pl.String)).collect()
        if dt_df.is_empty():
            return

        if partition_by == "year":
            partition_keys = sorted(list(set(d[:4] for d in dt_df["datetime"])))
            for y_key in partition_keys:
                sub_start = f"{y_key}-01-01"
                sub_end = f"{y_key}-12-31 23:59:59"
                yield cls.load_parquet(
                    path=path,
                    adj_factor_path=adj_factor_path,
                    columns=columns,
                    custom_columns=custom_columns,
                    start_date=sub_start,
                    end_date=sub_end,
                    symbols=symbols,
                )
        elif partition_by == "month":
            partition_keys = sorted(list(set(d[:7] for d in dt_df["datetime"])))
            for m_key in partition_keys:
                sub_start = f"{m_key}-01"
                sub_end = f"{m_key}-31 23:59:59"
                yield cls.load_parquet(
                    path=path,
                    adj_factor_path=adj_factor_path,
                    columns=columns,
                    custom_columns=custom_columns,
                    start_date=sub_start,
                    end_date=sub_end,
                    symbols=symbols,
                )
        else:
            yield cls.load_parquet(
                path=path,
                adj_factor_path=adj_factor_path,
                columns=columns,
                custom_columns=custom_columns,
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
            )

    @classmethod
    def load_csv(
        cls,
        path: Union[str, Path],
        adj_factor_path: Optional[Union[str, Path]] = None,
        columns: Optional[List[str]] = None,
        custom_columns: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        symbols: Optional[List[str]] = None,
    ) -> MarketData:
        """
        从 CSV 目录或文件装载行情矩阵。
        """
        path = Path(path)
        base_cols = {"symbol", "datetime", "timestamp", "open", "high", "low", "close", "volume", "amount"}

        if path.is_file():
            scan_path = str(path)
        else:
            scan_path = str(path / "**" / "*.csv")

        df = pl.scan_csv(scan_path, infer_schema_length=10000)
        schema_names = df.collect_schema().names()

        requested_custom = set(columns or []) | set(custom_columns or [])
        if requested_custom:
            target_cols = list(base_cols | requested_custom | {"symbol", "datetime"})
        else:
            target_cols = schema_names

        if start_date:
            df = df.filter(pl.col("datetime") >= start_date)
        if end_date:
            df = df.filter(pl.col("datetime") <= end_date)
        if symbols:
            df = df.filter(pl.col("symbol").is_in(symbols))

        available_cols = [col for col in target_cols if col in schema_names]
        df = df.select(available_cols).collect()

        if df.is_empty():
            raise ValueError(f"No CSV data found in {path} with specified filters.")

        if adj_factor_path:
            adj_path = Path(adj_factor_path)
            adj_scan = str(adj_path) if adj_path.is_file() else str(adj_path / "**" / "*.csv")
            adj_df = pl.scan_csv(adj_scan, infer_schema_length=10000)
            if start_date:
                adj_df = adj_df.filter(pl.col("datetime") >= start_date)
            if end_date:
                adj_df = adj_df.filter(pl.col("datetime") <= end_date)
            if symbols:
                adj_df = adj_df.filter(pl.col("symbol").is_in(symbols))
            adj_df = adj_df.select(["symbol", "datetime", "back_adj_factor"]).collect()

            df = df.join(adj_df, on=["symbol", "datetime"], how="left")

        return cls._build_container_from_df(df, custom_columns=custom_columns)

    @classmethod
    def _build_container_from_df(
        cls,
        df: pl.DataFrame,
        custom_columns: Optional[List[str]] = None,
    ) -> MarketData:
        """从 Polars DataFrame 构建 2D 对齐矩阵与 LazyCustomFields"""
        all_timestamps = df.select("datetime").unique().sort("datetime")["datetime"].to_list()
        all_symbols = df.select("symbol").unique().sort("symbol")["symbol"].to_list()

        def pivot_to_matrix(col_name: str, fill_value: float = np.nan) -> np.ndarray:
            if col_name not in df.columns:
                return np.full((len(all_timestamps), len(all_symbols)), fill_value, dtype=np.float64)
            
            pivoted = df.pivot(
                on="symbol",
                index="datetime",
                values=col_name,
                aggregate_function="first"
            ).sort("datetime")

            missing_syms = set(all_symbols) - set(pivoted.columns)
            for sym in missing_syms:
                pivoted = pivoted.with_columns(pl.lit(fill_value).alias(sym))

            matrix_df = pivoted.select(all_symbols)
            return matrix_df.to_numpy().astype(np.float64)

        open_mat = pivot_to_matrix("open")
        high_mat = pivot_to_matrix("high")
        low_mat = pivot_to_matrix("low")
        raw_close_mat = pivot_to_matrix("close")
        volume_mat = pivot_to_matrix("volume", fill_value=0.0)
        amount_mat = pivot_to_matrix("amount", fill_value=0.0)

        adj_factor_mat = None
        if "back_adj_factor" in df.columns:
            adj_factor_mat = pivot_to_matrix("back_adj_factor", fill_value=1.0)
            adj_factor_mat = np.nan_to_num(adj_factor_mat, nan=1.0)

        custom_fields = LazyCustomFields(
            df=df,
            all_timestamps=all_timestamps,
            all_symbols=all_symbols,
        )

        return MarketData(
            timestamps=np.array(all_timestamps),
            symbols=all_symbols,
            open_price=open_mat,
            high_price=high_mat,
            low_price=low_mat,
            close_price=raw_close_mat,
            raw_close_price=raw_close_mat,
            volume=volume_mat,
            amount=amount_mat,
            adj_factor=adj_factor_mat,
            custom_fields=custom_fields,
        )

