"""
So sánh liên giai đoạn: 2014-2019 vs 2020-2025
Phân tích sự thay đổi trong mối quan hệ liên thị trường giữa hai giai đoạn.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "output", "figures")

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "#f8f9fa",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
})


def compare_correlations(corr_old, corr_new):
    """So sánh ma trận Pearson giữa hai giai đoạn."""
    print("\n📊 SO SÁNH TƯƠNG QUAN PEARSON")
    print("-" * 50)

    pairs = [("Gold", "WTI"), ("Gold", "DXY"), ("WTI", "DXY")]
    results = []

    for a, b in pairs:
        old_val = corr_old["pearson"].loc[a, b]
        new_val = corr_new["pearson"].loc[a, b]
        change = new_val - old_val
        direction = "↑" if change > 0 else "↓"

        results.append({
            "Pair": f"{a} ↔ {b}",
            "2014-2019": round(old_val, 4),
            "2020-2025": round(new_val, 4),
            "Change": f"{direction} {abs(change):.4f}",
        })

        print(f"   {a} ↔ {b}: {old_val:.4f} → {new_val:.4f} ({direction}{abs(change):.4f})")

    return pd.DataFrame(results)


def compare_granger(granger_old, granger_new):
    """So sánh kết quả Granger Causality giữa hai giai đoạn."""
    print("\n🔍 SO SÁNH GRANGER CAUSALITY")
    print("-" * 50)

    results = []
    for name in ["DXY → Gold", "WTI → Gold"]:
        old = granger_old.get(name, {})
        new = granger_new.get(name, {})

        old_p = old.get("best_pvalue", float("nan"))
        new_p = new.get("best_pvalue", float("nan"))
        old_sig = "CÓ" if old.get("significant", False) else "KHÔNG"
        new_sig = "CÓ" if new.get("significant", False) else "KHÔNG"

        results.append({
            "Relation": name,
            "2014-2019 p-val": f"{old_p:.6f}",
            "2014-2019": old_sig,
            "2020-2025 p-val": f"{new_p:.6f}",
            "2020-2025": new_sig,
        })

        print(f"   {name}: {old_sig} (p={old_p:.4f}) → {new_sig} (p={new_p:.4f})")

    return pd.DataFrame(results)


def compare_models(metrics_old, metrics_new):
    """So sánh hiệu suất mô hình giữa hai giai đoạn."""
    print("\n🏆 SO SÁNH HIỆU SUẤT MÔ HÌNH")
    print("-" * 60)

    # Chỉ lấy test metrics
    test_old = [m for m in metrics_old if "Test" in m["Model"]]
    test_new = [m for m in metrics_new if "Test" in m["Model"]]

    results = []
    for old_m, new_m in zip(test_old, test_new):
        model_name = old_m["Model"].replace(" (Test)", "")
        for metric in ["MAE", "RMSE", "MAPE (%)"]:
            old_v = old_m[metric]
            new_v = new_m[metric]
            change_pct = ((new_v - old_v) / old_v) * 100 if old_v != 0 else 0
            direction = "↑ Tệ hơn" if change_pct > 0 else "↓ Tốt hơn"

            results.append({
                "Model": model_name,
                "Metric": metric,
                "2014-2019": old_v,
                "2020-2025": new_v,
                "Change": f"{direction} ({abs(change_pct):.1f}%)",
            })

    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    return df


def plot_comparison_charts(corr_df, model_df, merged_old, merged_new):
    """Tạo biểu đồ so sánh giữa hai giai đoạn."""
    os.makedirs(FIGURES_DIR, exist_ok=True)

    # --- 1. So sánh tương quan Pearson ---
    fig, ax = plt.subplots(figsize=(10, 6))
    x = range(len(corr_df))
    width = 0.35
    bars1 = ax.bar([i - width / 2 for i in x], corr_df["2014-2019"], width,
                   label="2014-2019", color="#3498db", alpha=0.85, edgecolor="white")
    bars2 = ax.bar([i + width / 2 for i in x], corr_df["2020-2025"], width,
                   label="2020-2025", color="#e74c3c", alpha=0.85, edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(corr_df["Pair"], fontsize=12)
    ax.set_ylabel("He so tuong quan Pearson", fontsize=11)
    ax.set_title("So sanh Tuong quan Pearson: 2014-2019 vs 2020-2025",
                 fontsize=14, fontweight="bold")
    ax.legend(fontsize=12)
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)
    ax.grid(True, alpha=0.3, axis="y")

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h + 0.01 * (1 if h >= 0 else -1),
                    f"{h:.3f}", ha="center", va="bottom" if h >= 0 else "top",
                    fontsize=10, fontweight="bold")

    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "comparison_pearson.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Pearson comparison: {path}")

    # --- 2. So sánh model metrics ---
    models = model_df["Model"].unique()
    metrics = ["MAE", "RMSE", "MAPE (%)"]

    fig, axes = plt.subplots(1, len(metrics), figsize=(16, 5))
    colors_period = ["#3498db", "#e74c3c"]

    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        subset = model_df[model_df["Metric"] == metric]
        x_pos = range(len(subset))
        width = 0.35

        bars1 = ax.bar([i - width / 2 for i in x_pos], subset["2014-2019"].values, width,
                       label="2014-2019", color=colors_period[0], alpha=0.85, edgecolor="white")
        bars2 = ax.bar([i + width / 2 for i in x_pos], subset["2020-2025"].values, width,
                       label="2020-2025", color=colors_period[1], alpha=0.85, edgecolor="white")

        ax.set_xticks(x_pos)
        ax.set_xticklabels(subset["Model"].values, fontsize=11)
        ax.set_title(metric, fontsize=13, fontweight="bold")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3, axis="y")

        for bars in [bars1, bars2]:
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2, h,
                        f"{h:.1f}", ha="center", va="bottom", fontsize=9, fontweight="bold")

    fig.suptitle("So sanh Hieu suat Mo hinh: 2014-2019 vs 2020-2025 (Test Set)",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "comparison_model_metrics.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Model metrics comparison: {path}")

    # --- 3. Biểu đồ giá Gold hai giai đoạn ---
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))

    axes[0].plot(merged_old.index, merged_old["Gold"], color="#DAA520", linewidth=1.5)
    axes[0].fill_between(merged_old.index, merged_old["Gold"], alpha=0.2, color="#FFD700")
    axes[0].set_title("Gia Vang 2014-2019", fontsize=13, fontweight="bold")
    axes[0].set_ylabel("USD/oz")
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axes[0].xaxis.set_major_locator(mdates.MonthLocator(interval=12))

    axes[1].plot(merged_new.index, merged_new["Gold"], color="#DAA520", linewidth=1.5)
    axes[1].fill_between(merged_new.index, merged_new["Gold"], alpha=0.2, color="#FFD700")
    axes[1].set_title("Gia Vang 2020-2025", fontsize=13, fontweight="bold")
    axes[1].set_ylabel("USD/oz")
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axes[1].xaxis.set_major_locator(mdates.MonthLocator(interval=12))

    fig.suptitle("So sanh Gia Vang giua 2 Giai doan", fontsize=14, fontweight="bold")
    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "comparison_gold_price.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Gold price comparison: {path}")

    # --- 4. Thống kê mô tả ---
    fig, ax = plt.subplots(figsize=(10, 6))

    stats_data = {
        "Metric": ["Mean", "Std", "Min", "Max", "Mean", "Std", "Min", "Max"],
        "Period": ["2014-2019"] * 4 + ["2020-2025"] * 4,
        "Value": [
            merged_old["Gold"].mean(), merged_old["Gold"].std(),
            merged_old["Gold"].min(), merged_old["Gold"].max(),
            merged_new["Gold"].mean(), merged_new["Gold"].std(),
            merged_new["Gold"].min(), merged_new["Gold"].max(),
        ]
    }
    stats_df = pd.DataFrame(stats_data)

    metrics_list = ["Mean", "Std", "Min", "Max"]
    x_pos = range(len(metrics_list))
    old_vals = [stats_df[(stats_df["Metric"] == m) & (stats_df["Period"] == "2014-2019")]["Value"].values[0] for m in metrics_list]
    new_vals = [stats_df[(stats_df["Metric"] == m) & (stats_df["Period"] == "2020-2025")]["Value"].values[0] for m in metrics_list]

    bars1 = ax.bar([i - 0.175 for i in x_pos], old_vals, 0.35,
                   label="2014-2019", color="#3498db", alpha=0.85, edgecolor="white")
    bars2 = ax.bar([i + 0.175 for i in x_pos], new_vals, 0.35,
                   label="2020-2025", color="#e74c3c", alpha=0.85, edgecolor="white")

    ax.set_xticks(x_pos)
    ax.set_xticklabels(metrics_list, fontsize=12)
    ax.set_ylabel("USD/oz", fontsize=11)
    ax.set_title("Thong ke Gia Vang: 2014-2019 vs 2020-2025", fontsize=14, fontweight="bold")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h,
                    f"${h:.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")

    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "comparison_gold_stats.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Gold stats comparison: {path}")

    return stats_df


def run(corr_old, corr_new, metrics_old, metrics_new, merged_old, merged_new):
    """Chạy toàn bộ so sánh liên giai đoạn."""
    print("\n" + "=" * 60)
    print("🔄 SO SÁNH LIÊN GIAI ĐOẠN: 2014-2019 vs 2020-2025")
    print("=" * 60)

    corr_df = compare_correlations(corr_old, corr_new)
    granger_df = compare_granger(corr_old["granger"], corr_new["granger"])
    model_df = compare_models(metrics_old, metrics_new)
    stats_df = plot_comparison_charts(corr_df, model_df, merged_old, merged_new)

    print(f"\n✅ So sánh liên giai đoạn hoàn thành!")

    return {
        "correlation_comparison": corr_df,
        "granger_comparison": granger_df,
        "model_comparison": model_df,
        "stats": stats_df,
    }
