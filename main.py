"""
Dự án Antigravity (PySpark Edition)
====================================
Phân tích Liên thị trường & Dự báo Giá Vàng

Pipeline sử dụng PySpark cho xử lý dữ liệu:
  - SparkSession quản lý tập trung
  - Window functions cho feature engineering
  - Spark DataFrame API cho tiền xử lý
  - Parquet output cho hiệu suất

So sánh 2 giai đoạn: 2014-2019 vs 2020-2025
"""

import sys
import os
import time
import warnings

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import get_spark_session, stop_spark
import data_collection
import correlation_analysis
import model_xgboost
import model_prophet
import evaluation
import visualizations
import period_comparison

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "output", "figures")


def run_period(spark, period_name, start, end):
    """Chạy toàn bộ pipeline cho một giai đoạn."""
    print("\n" + "#" * 60)
    print(f"#  PERIOD: {period_name} ({start} -> {end})")
    print("#" * 60)

    # 1. Data Collection & Preprocessing (PySpark)
    merged, df_prophet, spark_df = data_collection.run(spark, start=start, end=end)

    # 2. Correlation Analysis (PySpark + statsmodels)
    corr_results = correlation_analysis.run(spark_df, merged, prefix=period_name)

    # 3. XGBoost (PySpark features -> XGBoost training)
    xgb_results = model_xgboost.run(spark, spark_df, merged)

    # 4. Prophet
    prophet_results = model_prophet.run(df_prophet)

    # 5. Evaluation
    df_metrics, all_metrics = evaluation.compare_models(xgb_results, prophet_results)

    # 6. Visualizations
    visualizations.run(merged, xgb_results, prophet_results, df_metrics, prefix=period_name)

    return {
        "merged": merged,
        "df_prophet": df_prophet,
        "spark_df": spark_df,
        "corr_results": corr_results,
        "xgb_results": xgb_results,
        "prophet_results": prophet_results,
        "df_metrics": df_metrics,
        "all_metrics": all_metrics,
    }


def main():
    start_time = time.time()

    print("+" + "=" * 58 + "+")
    print("|  DU AN ANTIGRAVITY (PySpark Edition)                     |")
    print("|  Phan tich Lien thi truong & Du bao Gia Vang             |")
    print("|  Gold x WTI x DXY | XGBoost vs Prophet                  |")
    print("|  Data Processing: Apache Spark                           |")
    print("|  So sanh 2 giai doan: 2014-2019 vs 2020-2025             |")
    print("+" + "=" * 58 + "+")

    # Khởi tạo SparkSession
    print("\n  Initializing SparkSession...")
    spark = get_spark_session()
    print(f"  Spark version: {spark.version}")
    print(f"  Spark master: {spark.sparkContext.master}")

    try:
        # ============================================================
        # GIAI ĐOẠN 1: 2014-2019 (Pre-COVID)
        # ============================================================
        results_old = run_period(spark, "2014_2019", "2014-01-01", "2019-12-31")

        # ============================================================
        # GIAI ĐOẠN 2: 2020-2025 (Post-COVID)
        # ============================================================
        results_new = run_period(spark, "2020_2025", "2020-01-01", "2025-12-31")

        # ============================================================
        # SO SÁNH LIÊN GIAI ĐOẠN
        # ============================================================
        comparison = period_comparison.run(
            corr_old=results_old["corr_results"],
            corr_new=results_new["corr_results"],
            metrics_old=results_old["all_metrics"],
            metrics_new=results_new["all_metrics"],
            merged_old=results_old["merged"],
            merged_new=results_new["merged"],
        )

        # ============================================================
        # BÁO CÁO
        # ============================================================
        evaluation.generate_report_multi(
            results_old=results_old,
            results_new=results_new,
            comparison=comparison,
        )

    finally:
        # Luôn dừng SparkSession
        print("\n  Stopping SparkSession...")
        stop_spark(spark)

    # ============================================================
    # TỔNG KẾT
    # ============================================================
    elapsed = time.time() - start_time
    print("\n+" + "=" * 58 + "+")
    print("|  COMPLETED! (PySpark Edition)                            |")
    print("+" + "=" * 58 + "+")
    print(f"\n  Runtime: {elapsed:.1f} seconds")
    print(f"  Output:  {os.path.join(os.path.dirname(__file__), 'output')}")
    print(f"  Figures: output/figures/")
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    main()
