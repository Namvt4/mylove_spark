"""
Spark Session Manager
Quản lý SparkSession dùng chung cho toàn bộ pipeline.
"""

from pyspark.sql import SparkSession
import os

# Set HADOOP_HOME for Windows
if os.name == "nt":
    hadoop_home = r"C:\hadoop"
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + ";" + os.environ.get("PATH", "")


def get_spark_session(app_name="Antigravity_Gold_Forecasting"):
    """Tạo hoặc lấy SparkSession hiện có."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )

    # Giảm log noise
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def stop_spark(spark):
    """Dừng SparkSession."""
    if spark:
        spark.stop()
