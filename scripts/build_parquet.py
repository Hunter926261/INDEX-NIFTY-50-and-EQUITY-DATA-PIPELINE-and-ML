import pandas as pd
from pathlib import Path
from scripts.config import BASE_DIR
from scripts.logger import get_logger

logger = get_logger("parquet_builder")

# -----------------------------
# Directories
# -----------------------------
EQUITY_MASTER_DIR = BASE_DIR / "data" / "processed" / "equity" / "master"
INDEX_MASTER_DIR = BASE_DIR / "data" / "processed" / "index" / "master"

EQUITY_PARQUET_DIR = BASE_DIR / "data" / "processed" / "equity" / "parquet"
INDEX_PARQUET_DIR = BASE_DIR / "data" / "processed" / "index" / "parquet"

EQUITY_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Convert Equity Master
# -----------------------------
equity_files = sorted(EQUITY_MASTER_DIR.glob("nse_master_*.csv"))

if equity_files:
    equity_path = equity_files[-1]
    df_equity = pd.read_csv(equity_path)

    df_equity['trade_date'] = pd.to_datetime(df_equity['trade_date'])

    parquet_path = EQUITY_PARQUET_DIR / equity_path.name.replace(".csv", ".parquet")

    df_equity.to_parquet(parquet_path, index=False)

    logger.info(f"Equity parquet created: {parquet_path.name}")
    print("Equity Parquet Created ✅")

else:
    logger.warning("No equity master found.")

# -----------------------------
# Convert Index Master
# -----------------------------
index_files = sorted(INDEX_MASTER_DIR.glob("nifty50_index_master_*.csv"))

if index_files:
    index_path = index_files[-1]
    df_index = pd.read_csv(index_path)

    df_index['trade_date'] = pd.to_datetime(df_index['trade_date'])

    parquet_path = INDEX_PARQUET_DIR / index_path.name.replace(".csv", ".parquet")

    df_index.to_parquet(parquet_path, index=False)

    logger.info(f"Index parquet created: {parquet_path.name}")
    print("Index Parquet Created ✅")

else:
    logger.warning("No index master found.")