"""
单元测试：自定义列/特征按需加载与 LazyCustomFields 懒透视行为测试
"""

from pathlib import Path
import numpy as np
import polars as pl
import pytest
from cq.engine import ColumnDataLoader, MarketData, BarContext, strategy, Engine


@pytest.fixture
def sample_csv_with_custom_fields(tmp_path: Path) -> Path:
    """生成带有自定义列 factor_b 和 pe_ttm 的测试 CSV 文件"""
    df = pl.DataFrame({
        "symbol": ["SYM0", "SYM1", "SYM0", "SYM1"],
        "datetime": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        "open": [10.0, 20.0, 11.0, 21.0],
        "high": [10.5, 20.5, 11.5, 21.5],
        "low": [9.5, 19.5, 10.5, 20.5],
        "close": [10.0, 20.0, 11.0, 21.0],
        "volume": [1000.0, 2000.0, 1100.0, 2100.0],
        "amount": [10000.0, 40000.0, 12100.0, 44100.0],
        "factor_b": [0.85, -0.42, 0.92, -0.15],
        "pe_ttm": [15.2, 28.4, 15.5, 27.9],
    })
    csv_dir = tmp_path / "csv_data"
    csv_dir.mkdir()
    df.write_csv(csv_dir / "data.csv")
    return csv_dir


def test_column_loader_custom_fields(sample_csv_with_custom_fields: Path):
    """测试 ColumnDataLoader 自动识别并提供 LazyCustomFields"""
    data = ColumnDataLoader.load_csv(sample_csv_with_custom_fields)

    assert "factor_b" in data.custom_fields
    assert "pe_ttm" in data.custom_fields
    assert "non_exist" not in data.custom_fields

    # 测试 keys() 方法
    keys = data.custom_fields.keys()
    assert "factor_b" in keys
    assert "pe_ttm" in keys

    # 首次读取触发 Lazy Pivot
    mat_b = data["factor_b"]
    assert mat_b.shape == (2, 2)
    np.testing.assert_allclose(mat_b[:, 0], [0.85, 0.92])
    np.testing.assert_allclose(mat_b[:, 1], [-0.42, -0.15])

    # 测试 get 带默认值
    default_arr = np.ones((2, 2))
    res_default = data.custom_fields.get("non_exist", default=default_arr)
    np.testing.assert_array_equal(res_default, default_arr)


def test_custom_columns_filter(sample_csv_with_custom_fields: Path):
    """测试 custom_columns 按需显式筛选过滤"""
    data = ColumnDataLoader.load_csv(
        sample_csv_with_custom_fields,
        custom_columns=["factor_b"]
    )

    assert "factor_b" in data.custom_fields
    # 验证没有加载未指定的 pe_ttm
    with pytest.raises(KeyError):
        _ = data["pe_ttm"]


def test_bar_context_custom_fields_access(sample_csv_with_custom_fields: Path):
    """测试 BarContext 中 ctx.get('factor_b'), ctx.get_history('factor_b'), ctx.custom['factor_b']"""
    data = ColumnDataLoader.load_csv(sample_csv_with_custom_fields)

    recorded_b = []
    recorded_b_hist = []

    @strategy
    def my_strat(ctx: BarContext):
        # 通过不同形式访问自定义列
        b_now = ctx.get("factor_b")
        b_hist = ctx.get_history("factor_b")

        recorded_b.append(b_now.copy())
        recorded_b_hist.append(b_hist.copy())

        # 字典与代理语法验证
        np.testing.assert_allclose(ctx["factor_b"], b_now)
        np.testing.assert_allclose(ctx.custom["factor_b"][ctx.step, :], b_now)

    engine = Engine(initial_cash=100000.0)
    engine.run(strategy=my_strat, data=data)

    # 步 t=0 (2024-01-01)
    np.testing.assert_allclose(recorded_b[0], [0.85, -0.42])
    np.testing.assert_allclose(recorded_b_hist[0], [[0.85, -0.42]])

    # 步 t=1 (2024-01-02)
    np.testing.assert_allclose(recorded_b[1], [0.92, -0.15])
    np.testing.assert_allclose(recorded_b_hist[1], [[0.85, -0.42], [0.92, -0.15]])
