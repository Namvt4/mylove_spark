"""
Visualizations: Tạo biểu đồ tổng hợp cho dự án
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "output", "figures")

# Thiết lập style
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "#f8f9fa",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
})

COLORS = {
    "gold": "#FFD700",
    "gold_dark": "#DAA520",
    "wti": "#2ecc71",
    "dxy": "#3498db",
    "xgboost": "#e74c3c",
    "prophet": "#9b59b6",
    "actual": "#2c3e50",
}


def plot_price_history(merged, prefix=""):
    """Biểu đồ giá lịch sử 3 tài sản."""
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    # Gold
    axes[0].plot(merged.index, merged["Gold"], color=COLORS["gold_dark"], linewidth=1.5)
    axes[0].fill_between(merged.index, merged["Gold"], alpha=0.2, color=COLORS["gold"])
    axes[0].set_ylabel("Giá (USD/oz)", fontsize=11)
    axes[0].set_title("Vàng (Gold Futures - GC=F)", fontsize=13, fontweight="bold")

    # WTI
    axes[1].plot(merged.index, merged["WTI"], color=COLORS["wti"], linewidth=1.5)
    axes[1].fill_between(merged.index, merged["WTI"], alpha=0.2, color=COLORS["wti"])
    axes[1].set_ylabel("Giá (USD/bbl)", fontsize=11)
    axes[1].set_title("Dầu thô WTI (CL=F)", fontsize=13, fontweight="bold")

    # DXY
    axes[2].plot(merged.index, merged["DXY"], color=COLORS["dxy"], linewidth=1.5)
    axes[2].fill_between(merged.index, merged["DXY"], alpha=0.2, color=COLORS["dxy"])
    axes[2].set_ylabel("Chỉ số", fontsize=11)
    axes[2].set_title("Chỉ số USD (DX-Y.NYB)", fontsize=13, fontweight="bold")
    axes[2].set_xlabel("Thời gian", fontsize=11)

    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))

    fig.suptitle("Giá Lịch sử: Gold / WTI / DXY", fontsize=15, fontweight="bold", y=1.02)
    plt.tight_layout()

    fname = f"{prefix}_price_history.png" if prefix else "price_history.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Price history chart: {path}")
    return path


def plot_actual_vs_predicted(xgb_results, prophet_results, prefix=""):
    """Biểu đồ Actual vs Predicted cho cả hai mô hình."""
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # XGBoost
    ax = axes[0]
    xgb_idx = xgb_results["test_index"]
    ax.plot(xgb_idx, xgb_results["y_test"], color=COLORS["actual"],
            linewidth=1.5, label="Actual", alpha=0.8)
    ax.plot(xgb_idx, xgb_results["y_pred_test"], color=COLORS["xgboost"],
            linewidth=1.5, label="XGBoost Prediction", linestyle="--", alpha=0.9)
    ax.fill_between(xgb_idx,
                    xgb_results["y_test"],
                    xgb_results["y_pred_test"],
                    alpha=0.15, color=COLORS["xgboost"])
    ax.set_title("XGBoost: Actual vs Predicted (Test Set)", fontsize=13, fontweight="bold")
    ax.set_ylabel("Giá Vàng (USD)", fontsize=11)
    ax.legend(fontsize=11)

    # Prophet
    ax2 = axes[1]
    prophet_dates = pd.to_datetime(prophet_results["test_dates"])
    ax2.plot(prophet_dates, prophet_results["y_test"], color=COLORS["actual"],
             linewidth=1.5, label="Actual", alpha=0.8)
    ax2.plot(prophet_dates, prophet_results["y_pred_test"], color=COLORS["prophet"],
             linewidth=1.5, label="Prophet Prediction", linestyle="--", alpha=0.9)
    ax2.fill_between(prophet_dates,
                     prophet_results["y_test"],
                     prophet_results["y_pred_test"],
                     alpha=0.15, color=COLORS["prophet"])
    ax2.set_title("Prophet: Actual vs Predicted (Test Set)", fontsize=13, fontweight="bold")
    ax2.set_ylabel("Giá Vàng (USD)", fontsize=11)
    ax2.set_xlabel("Thời gian", fontsize=11)
    ax2.legend(fontsize=11)

    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

    plt.tight_layout()

    fname = f"{prefix}_actual_vs_predicted.png" if prefix else "actual_vs_predicted.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Actual vs Predicted chart: {path}")
    return path


def plot_feature_importance(xgb_results, top_n=15, prefix=""):
    """Biểu đồ Feature Importance từ XGBoost."""
    feat_imp = xgb_results["feature_importance"].head(top_n)

    fig, ax = plt.subplots(figsize=(10, 8))

    colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(feat_imp)))[::-1]
    bars = ax.barh(
        range(len(feat_imp)),
        feat_imp["Importance"].values,
        color=colors,
        edgecolor="white",
        linewidth=0.5,
    )
    ax.set_yticks(range(len(feat_imp)))
    ax.set_yticklabels(feat_imp["Feature"].values, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Importance Score", fontsize=11)
    ax.set_title(f"XGBoost - Top {top_n} Feature Importance\n(Regressor Importance: USD & Oil → Gold)",
                 fontsize=13, fontweight="bold")

    # Thêm giá trị lên bars
    for i, (bar, val) in enumerate(zip(bars, feat_imp["Importance"].values)):
        ax.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height() / 2,
                f"{val:.4f}", va="center", fontsize=9)

    plt.tight_layout()

    fname = f"{prefix}_feature_importance.png" if prefix else "feature_importance.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Feature importance chart: {path}")
    return path


def plot_model_comparison(df_metrics, prefix=""):
    """Biểu đồ so sánh metrics giữa hai mô hình."""
    # Chỉ lấy test metrics
    test_metrics = df_metrics[df_metrics["Model"].str.contains("Test")].copy()

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    metrics = ["MAE", "RMSE", "MAPE (%)"]
    colors_models = [COLORS["xgboost"], COLORS["prophet"]]
    model_names = ["XGBoost", "Prophet"]

    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        values = test_metrics[metric].values
        bars = ax.bar(model_names, values, color=colors_models, alpha=0.85,
                      edgecolor="white", linewidth=1.5, width=0.5)

        # Thêm giá trị
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01 * max(values),
                    f"{val:.4f}", ha="center", va="bottom", fontsize=12, fontweight="bold")

        ax.set_title(metric, fontsize=14, fontweight="bold")
        ax.set_ylabel("Giá trị", fontsize=11)

        # Highlight winner
        winner_idx = np.argmin(values)
        bars[winner_idx].set_edgecolor("#2ecc71")
        bars[winner_idx].set_linewidth(3)

    fig.suptitle("So sánh Mô hình: XGBoost vs Prophet (Test Set)",
                 fontsize=15, fontweight="bold")
    plt.tight_layout()

    fname = f"{prefix}_model_comparison.png" if prefix else "model_comparison.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Model comparison chart: {path}")
    return path


def plot_residuals(xgb_results, prophet_results, prefix=""):
    """Phân tích residuals của hai mô hình."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # XGBoost residuals
    xgb_residuals = np.array(xgb_results["y_test"]) - np.array(xgb_results["y_pred_test"])
    prophet_residuals = np.array(prophet_results["y_test"]) - np.array(prophet_results["y_pred_test"])

    # XGBoost scatter
    axes[0, 0].scatter(xgb_results["y_pred_test"], xgb_residuals, alpha=0.5,
                       color=COLORS["xgboost"], s=15)
    axes[0, 0].axhline(y=0, color="black", linestyle="--", alpha=0.5)
    axes[0, 0].set_title("XGBoost: Residuals vs Predicted", fontsize=12, fontweight="bold")
    axes[0, 0].set_xlabel("Predicted")
    axes[0, 0].set_ylabel("Residual")

    # XGBoost histogram
    axes[0, 1].hist(xgb_residuals, bins=30, color=COLORS["xgboost"], alpha=0.7, edgecolor="white")
    axes[0, 1].axvline(x=0, color="black", linestyle="--", alpha=0.5)
    axes[0, 1].set_title("XGBoost: Phân phối Residuals", fontsize=12, fontweight="bold")
    axes[0, 1].set_xlabel("Residual")
    axes[0, 1].set_ylabel("Frequency")

    # Prophet scatter
    axes[1, 0].scatter(prophet_results["y_pred_test"], prophet_residuals, alpha=0.5,
                       color=COLORS["prophet"], s=15)
    axes[1, 0].axhline(y=0, color="black", linestyle="--", alpha=0.5)
    axes[1, 0].set_title("Prophet: Residuals vs Predicted", fontsize=12, fontweight="bold")
    axes[1, 0].set_xlabel("Predicted")
    axes[1, 0].set_ylabel("Residual")

    # Prophet histogram
    axes[1, 1].hist(prophet_residuals, bins=30, color=COLORS["prophet"], alpha=0.7, edgecolor="white")
    axes[1, 1].axvline(x=0, color="black", linestyle="--", alpha=0.5)
    axes[1, 1].set_title("Prophet: Phân phối Residuals", fontsize=12, fontweight="bold")
    axes[1, 1].set_xlabel("Residual")
    axes[1, 1].set_ylabel("Frequency")

    fig.suptitle("Phân tích Residuals", fontsize=15, fontweight="bold")
    plt.tight_layout()

    fname = f"{prefix}_residuals_analysis.png" if prefix else "residuals_analysis.png"
    path = os.path.join(FIGURES_DIR, fname)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾 Residuals analysis chart: {path}")
    return path


def run(merged, xgb_results, prophet_results, df_metrics, prefix=""):
    """Tạo tất cả visualizations."""
    print("\n" + "=" * 60)
    print("📈 GIAI ĐOẠN 5: VISUALIZATIONS")
    print("=" * 60)

    os.makedirs(FIGURES_DIR, exist_ok=True)

    plot_price_history(merged, prefix=prefix)
    plot_actual_vs_predicted(xgb_results, prophet_results, prefix=prefix)
    plot_feature_importance(xgb_results, prefix=prefix)
    plot_model_comparison(df_metrics, prefix=prefix)
    plot_residuals(xgb_results, prophet_results, prefix=prefix)

    print(f"\n✅ Tất cả biểu đồ đã được lưu vào: {FIGURES_DIR}")
