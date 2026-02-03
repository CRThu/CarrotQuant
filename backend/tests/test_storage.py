import os
import shutil
import pytest
import pandas as pd
import duckdb
from pathlib import Path
from core.storage import DuckDBStorage

@pytest.fixture
def temp_storage_dir(tmp_path):
    """Create a temporary directory for storage tests."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    return str(storage_dir)

def test_save_month(temp_storage_dir):
    """Test saving monthly data to partitioned parquet files."""
    storage = DuckDBStorage(root_dir=temp_storage_dir)
    
    # Create dummy data
    data = {
        "trade_date": ["2024-01-02", "2024-01-03"],
        "sector_name": ["Semicon", "Semicon"],
        "open": [100.0, 101.0],
        "close": [101.0, 102.0],
        "high": [102.0, 103.0],
        "low": [99.0, 100.0],
        "volume": [1000, 1100],
        "amount": [100000.0, 110000.0],
        "amplitude": [3.0, 3.0],
        "pct_change": [1.0, 1.0],
        "change_amount": [1.0, 1.0],
        "turnover": [0.5, 0.5]
    }
    df = pd.DataFrame(data)
    
    # Ensure types match what strict typing might expect (though DuckDB casts)
    # Pandas usually infers well, but date needs to be date object or string consistent with casting
    # The storage implementation uses CAST(trade_date AS DATE), so string 'YYYY-MM-DD' works.
    
    table_name = "test_sector"
    year = 2024
    month = 1
    
    # Execute save
    storage.save_month(df, table_name, year, month)
    
    # Verify file existence
    expected_path = Path(temp_storage_dir) / table_name / f"year={year}" / f"{year}-{month:02d}.parquet"
    assert expected_path.exists()
    
    # Verify content using duckdb to avoid pyarrow/fastparquet dependency
    # Note: DuckDBStorage casts columns, so we should expect the types to be consistent with the Schema in storage.py
    read_df = duckdb.sql(f"SELECT * FROM '{str(expected_path)}'").df()
    
    assert len(read_df) == 2
    assert "trade_date" in read_df.columns
    # Check if data matches. Parquet might convert dates to datetime.date or timestamp
    # DuckDB CAST(x AS DATE) results in a Date type. Pandas read_parquet typically reads strictly.
    
    # Let's check a value
    assert read_df.iloc[0]["open"] == 100.0
    assert read_df.iloc[0]["sector_name"] == "Semicon"
    
    # Clean up is handled by pytest tmp_path fixture automatically
