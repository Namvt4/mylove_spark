"""
Giai đoạn 2: Phân tích Tương quan Nâng cao với PySpark
- Static Correlation (PySpark corr)
- Rolling Correlation (PySpark Window)
- Granger Causality Test
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from statsmodels.tsa.stattools import grangercausalitytests
import os
import warnings

warnings.filterwarnings("ignore")

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "output", "figures")


def static_correlation_spark(spark_df, prefix=""):
    """Tính tương quan Pearson bằng PySpark."""
    print("\n  [PySpark] Computing Pearson Correlation...")

    pairs = [("Gold", "WTI"), ("Gold", "DXY"), ("WTI", "DXY")]
    corr_values = {}
    for a, b in pairs:
        corr_val = spark_df.stat.corr(a, b)
        corr_values[(a, b)] = corr_val
        corr_values[(b, a)] = corr_val
        print(f"    corr({a}, {b}) = {corr_val:.4f}")

    # Tạo ma trận tương quan dạng pandas cho visualization
    cols = ["Gold", "WTI", "DXY"]
    corr_matrix = pd.DataFrame(np.eye(3), index=cols, columns=cols)
    for (a, b), v in corr_values.items():
        corr_matrix.loc[a, b] = v

    # Vẽ heatmap
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(
        corr_matrix,
        annot=True, fmt=".4f", cmap="RdBu_r",
        center=0, vmin=-1, vmax=1, square=True,
        linewidths=0.5, ax=ax,
        annot_kws={"size": 14, "weight": "bold"},
    )
    title_suffix = f" [{prefix}]" if prefix else ""
    ax.set_title(f"Ma tran Tuong quan Pearson (PySpark)\n(Gold - WTI - DXY){title_suffix}",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()

    fname = f"{prefix}_pearson_correlation_heatmap.png" if prefix else "pearson_correlation_heatmap.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved: {path}")

    return corr_matrix


def rolling_correlation_spark(spark_df, windows=[30, 60], prefix=""):
    """Tính rolling correlation bằng PySpark Window functions."""
    print(f"\n  [PySpark] Computing Rolling Correlation (windows={windows})...")

    # Chuyển sang pandas cho plotting (rolling corr phức tạp trong Spark)
    pdf = spark_df.select("date", "Gold", "WTI", "DXY").orderBy("date").toPandas()
    pdf = pdf.set_index("date")

    fig, axes = plt.subplots(len(windows), 1, figsize=(14, 5 * len(windows)), sharex=True)
    if len(windows) == 1:
        axes = [axes]

    results = {}

    for idx, window in enumerate(windows):
        ax = axes[idx]
        roll_dxy = pdf["Gold"].rolling(window).corr(pdf["DXY"])
        roll_wti = pdf["Gold"].rolling(window).corr(pdf["WTI"])

        results[f"Gold_DXY_{window}d"] = roll_dxy
        results[f"Gold_WTI_{window}d"] = roll_wti

        ax.plot(roll_dxy.index, roll_dxy, label="Gold <-> DXY", color="#e74c3c", linewidth=1.5)
        ax.plot(roll_wti.index, roll_wti, label="Gold <-> WTI", color="#2ecc71", linewidth=1.5)
        ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)
        ax.fill_between(roll_dxy.index, roll_dxy, alpha=0.1, color="#e74c3c")
        ax.fill_between(roll_wti.index, roll_wti, alpha=0.1, color="#2ecc71")
        ax.set_ylabel("Correlation", fontsize=11)
        ax.set_title(f"Rolling Correlation - {window}-day window", fontsize=13, fontweight="bold")
        ax.legend(fontsize=11)
        ax.set_ylim(-1, 1)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Date", fontsize=11)
    plt.tight_layout()

    fname = f"{prefix}_rolling_correlation.png" if prefix else "rolling_correlation.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved: {path}")

    return results


def granger_causality(merged_pd, maxlag=10, prefix=""):
    """Kiểm tra Granger Causality: DXY/WTI -> Gold (statsmodels)."""
    print(f"\n  Granger Causality Test (maxlag={maxlag}):")
    print("  " + "-" * 50)

    results = {}
    pairs = [
        ("DXY -> Gold", ["Gold", "DXY"]),
        ("WTI -> Gold", ["Gold", "WTI"]),
    ]

    for name, cols in pairs:
        data = merged_pd[cols].dropna()
        try:
            gc_result = grangercausalitytests(data, maxlag=maxlag, verbose=False)

            best_lag = None
            best_pvalue = 1.0
            lag_results = []

            for lag in range(1, maxlag + 1):
                p_val = gc_result[lag][0]["ssr_ftest"][1]
                lag_results.append({"lag": lag, "p_value": p_val})
                if p_val < best_pvalue:
                    best_pvalue = p_val
                    best_lag = lag

            results[name] = {
                "best_lag": best_lag,
                "best_pvalue": best_pvalue,
                "significant": best_pvalue < 0.05,
                "details": lag_results,
            }

            status = "YES" if best_pvalue < 0.05 else "NO"
            print(f"    {name}: best lag={best_lag}, p={best_pvalue:.6f} -> {status}")

        except Exception as e:
            print(f"    {name}: Error - {e}")
            results[name] = {"error": str(e)}

    # Vẽ biểu đồ
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for idx, (name, cols) in enumerate(pairs):
        ax = axes[idx]
        if name in results and "details" in results[name]:
            details = results[name]["details"]
            lags = [d["lag"] for d in details]
            pvals = [d["p_value"] for d in details]
            ax.bar(lags, pvals, color=["#2ecc71" if p < 0.05 else "#e74c3c" for p in pvals], alpha=0.8)
            ax.axhline(y=0.05, color="red", linestyle="--", linewidth=1.5, label="alpha=0.05")
            ax.set_xlabel("Lag (days)")
            ax.set_ylabel("p-value")
            ax.set_title(f"Granger Causality: {name}", fontsize=13, fontweight="bold")
            ax.legend()
            ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    fname = f"{prefix}_granger_causality.png" if prefix else "granger_causality.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved: {path}")

    return results


def run(spark_df, merged_pd, prefix=""):
    """Pipeline phân tích tương quan với PySpark."""
    print("\n" + "=" * 60)
    print("  PHASE 2: CORRELATION ANALYSIS (PySpark)")
    print("=" * 60)

    os.makedirs(FIGURES_DIR, exist_ok=True)

    # Pearson correlation bằng PySpark
    corr_matrix = static_correlation_spark(spark_df, prefix=prefix)

    # Rolling correlation
    rolling_results = rolling_correlation_spark(spark_df, prefix=prefix)

    # Granger causality (cần pandas)
    granger_results = granger_causality(merged_pd, prefix=prefix)

    return {
        "pearson": corr_matrix,
        "rolling": rolling_results,
        "granger": granger_results,
    }
