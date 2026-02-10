from datetime import date
from typing import Optional

class DataNotFoundError(Exception):
    """
    当请求的表名未注册、磁盘路径缺失或 SQL 查询结果为空时抛出。
    """
    def __init__(self, table_name: str, start_date: Optional[date] = None, end_date: Optional[date] = None):
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date
        date_range = f" ({start_date} to {end_date})" if start_date and end_date else ""
        self.message = f"Data not found for table '{table_name}'{date_range}"
        super().__init__(self.message)
