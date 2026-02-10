import numpy as np

def ffill_2d(arr: np.ndarray) -> np.ndarray:
    """
    纯 NumPy 实现的 2D 矩阵前值填充 (Axis 0 - 时间轴)
    要求: 仅接受 NumPy 数组，不依赖业务对象
    """
    mask = np.isnan(arr)
    # 获取非 NaN 值的原始行索引
    idx = np.where(~mask, np.arange(mask.shape[0])[:, None], 0)
    # 沿着时间轴向下累积最大索引，实现前值位置传播
    np.maximum.accumulate(idx, axis=0, out=idx)
    # 利用高级索引提取对应位置的值
    col_idx = np.arange(idx.shape[1])
    return arr[idx, col_idx]

def zero_fill(arr: np.ndarray) -> np.ndarray:
    """
    将 NaN 替换为 0.0
    """
    return np.nan_to_num(arr, nan=0.0)
