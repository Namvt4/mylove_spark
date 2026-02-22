"""
Giai đoạn 1: Thu thập và Tiền xử lý dữ liệu với PySpark
- Nguồn: Yahoo Finance (yfinance)
- Xử lý: PySpark DataFrame API
- Tickers: Gold (GC=F), Crude Oil (CL=F), USD Index (DX-Y.NYB)
"""

import yfinance as yf
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, DateType
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def fetch_data_yfinance(start="2020-01-01", end="2025-12-31"):
    """Tải dữ liệu từ Yahoo Finance (pandas) trước khi chuyển sang Spark."""
    tickers = {
        "Gold": "GC=F",
        "WTI": "CL=F",
        "DXY": "DX-Y.NYB",
    }

    frames = {}
    for name, ticker in tickers.items():
        print(f"  Loading {name} ({ticker})...")
        df = yf.download(ticker, start=start, end=end, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Close"]].rename(columns={"Close": name})
        frames[name] = df
        print(f"  {name}: {len(df)} records")

    return frames


def pandas_to_spark(spark, frames):
    """Chuyển đổi pandas DataFrames sang PySpark DataFrames và thực hiện merge."""
    print("\n  [PySpark] Converting pandas DataFrames to Spark...")

    # Merge pandas trước (vì yfinance trả về pandas)
    merged_pd = pd.concat(frames.values(), axis=1, join="inner")
    merged_pd.index = pd.to_datetime(merged_pd.index)
    merged_pd = merged_pd.sort_index()
    merged_pd = merged_pd.reset_index()
    merged_pd = merged_pd.rename(columns={"Date": "date"})

    # Chuyển sang Spark DataFrame
    spark_df = spark.createDataFrame(merged_pd)

    print(f"  [PySpark] Spark DataFrame created: {spark_df.count()} rows, {len(spark_df.columns)} columns")
    spark_df.printSchema()

    return spark_df


def preprocess_spark(spark_df):
    """Tiền xử lý dữ liệu bằng PySpark - xử lý missing, thống kê."""
    print("\n  [PySpark] Preprocessing data...")

    # Đếm missing values
    missing_counts = spark_df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in spark_df.columns]
    ).collect()[0]

    total_missing = sum(missing_counts[c] for c in spark_df.columns)
    print(f"  [PySpark] Missing values before: {total_missing}")

    # Forward fill bằng Window function
    from pyspark.sql.window import Window

    window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    for col_name in ["Gold", "WTI", "DXY"]:
        spark_df = spark_df.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(window_spec)
        )

    # Backward fill
    window_spec_back = Window.orderBy(F.col("date").desc()).rowsBetween(Window.unboundedPreceding, 0)
    for col_name in ["Gold", "WTI", "DXY"]:
        spark_df = spark_df.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(window_spec_back)
        )

    # Drop any remaining nulls
    spark_df = spark_df.dropna()

    # Thống kê mô tả bằng PySpark
    print("\n  [PySpark] Descriptive Statistics:")
    spark_df.select("Gold", "WTI", "DXY").describe().show()

    # Thêm computed columns bằng PySpark
    spark_df = spark_df.withColumn("Gold_WTI_ratio", F.col("Gold") / F.col("WTI"))
    spark_df = spark_df.withColumn("Gold_DXY_ratio", F.col("Gold") / F.col("DXY"))

    record_count = spark_df.count()
    date_range = spark_df.select(F.min("date"), F.max("date")).collect()[0]
    print(f"  [PySpark] After preprocessing: {record_count} records")
    print(f"  [PySpark] Date range: {date_range[0]} -> {date_range[1]}")

    return spark_df


def spark_to_pandas(spark_df):
    """Chuyển Spark DataFrame về pandas cho model training."""
    print("  [PySpark] Converting Spark DataFrame to pandas...")
    # QUAN TRỌNG: orderBy trước khi toPandas để đảm bảo thứ tự thời gian
    merged = spark_df.select("date", "Gold", "WTI", "DXY").orderBy("date").toPandas()
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.set_index("date")
    merged = merged.sort_index()  # Double-check sort
    merged.index.name = "Date"
    print(f"    Date range: {merged.index[0].date()} -> {merged.index[-1].date()}")
    return merged


def prepare_prophet_format(merged, target="Gold"):
    """Chuẩn bị format Prophet (ds, y) từ pandas DataFrame."""
    df_prophet = merged.reset_index()
    df_prophet = df_prophet.rename(columns={"Date": "ds", target: "y"})

    cols_keep = ["ds", "y"]
    for col in ["WTI", "DXY"]:
        if col in df_prophet.columns:
            cols_keep.append(col)

    df_prophet = df_prophet[cols_keep]
    df_prophet["ds"] = pd.to_datetime(df_prophet["ds"])
    df_prophet = df_prophet.sort_values("ds").reset_index(drop=True)  # Đảm bảo thứ tự
    return df_prophet


def save_data(spark_df, merged, df_prophet):
    """Lưu dữ liệu - pandas CSV (Spark processing đã hoàn tất trước bước này)."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Lưu pandas CSV
    csv_path = os.path.join(DATA_DIR, "merged_data.csv")
    merged.to_csv(csv_path)

    prophet_path = os.path.join(DATA_DIR, "prophet_data.csv")
    df_prophet.to_csv(prophet_path, index=False)

    # Lưu Spark schema info
    schema_path = os.path.join(DATA_DIR, "spark_schema.txt")
    with open(schema_path, "w") as f:
        f.write(f"Spark DataFrame Schema:\n")
        for field in spark_df.schema.fields:
            f.write(f"  {field.name}: {field.dataType}\n")
        f.write(f"\nRecords: {spark_df.count()}\n")

    print(f"  Saved CSV: {csv_path}")
    print(f"  Saved CSV: {prophet_path}")
    print(f"  Saved schema: {schema_path}")


def run(spark, start="2020-01-01", end="2025-12-31"):
    """Pipeline thu thập & tiền xử lý dữ liệu với PySpark."""
    print("=" * 60)
    print("  PHASE 1: DATA COLLECTION & PREPROCESSING (PySpark)")
    print("=" * 60)

    # 1. Fetch từ Yahoo Finance (pandas)
    frames = fetch_data_yfinance(start=start, end=end)

    # 2. Chuyển sang PySpark DataFrame
    spark_df = pandas_to_spark(spark, frames)

    # 3. Tiền xử lý bằng PySpark
    spark_df = preprocess_spark(spark_df)

    # 4. Chuyển về pandas cho model training
    merged = spark_to_pandas(spark_df)
    df_prophet = prepare_prophet_format(merged)

    # 5. Lưu dữ liệu
    save_data(spark_df, merged, df_prophet)

    return merged, df_prophet, spark_df


if __name__ == "__main__":
    from spark_session import get_spark_session, stop_spark
    spark = get_spark_session()
    run(spark)
    stop_spark(spark)
