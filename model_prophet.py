"""
Giai đoạn 3B: Prophet Model
- Prophet với exogenous regressors (WTI, DXY)
- Decomposition & Forecast plots
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from prophet import Prophet
import os
import warnings

warnings.filterwarnings("ignore")

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "output", "figures")


def build_prophet_model(train, test):
    """Xây dựng và huấn luyện Prophet."""
    print("\n  Building Prophet model...")

    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=True,
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10,
    )

    if "WTI" in train.columns:
        model.add_regressor("WTI", mode="multiplicative")
        print("    + Regressor: WTI")
    if "DXY" in train.columns:
        model.add_regressor("DXY", mode="multiplicative")
        print("    + Regressor: DXY")

    model.fit(train)
    print("    Model trained")

    forecast_train = model.predict(train)
    forecast_test = model.predict(test)

    return model, forecast_train, forecast_test


def plot_decomposition(model, forecast_train, train):
    """Prophet decomposition."""
    os.makedirs(FIGURES_DIR, exist_ok=True)
    fig = model.plot_components(forecast_train)
    path = os.path.join(FIGURES_DIR, "prophet_decomposition.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved: {path}")
    return path


def plot_forecast(model, forecast_train, train):
    """Prophet forecast plot."""
    fig = model.plot(forecast_train)
    path = os.path.join(FIGURES_DIR, "prophet_forecast.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved: {path}")
    return path


def run(df_prophet):
    """Pipeline Prophet."""
    print("\n" + "=" * 60)
    print("  PHASE 3B: PROPHET MODEL")
    print("=" * 60)

    os.makedirs(FIGURES_DIR, exist_ok=True)

    # Train/test split (80/20 temporal)
    split_idx = int(len(df_prophet) * 0.8)
    train = df_prophet.iloc[:split_idx].copy()
    test = df_prophet.iloc[split_idx:].copy()

    print(f"\n  Prophet Train/Test Split:")
    print(f"    Train: {len(train)} samples ({train['ds'].iloc[0].date()} -> {train['ds'].iloc[-1].date()})")
    print(f"    Test:  {len(test)} samples ({test['ds'].iloc[0].date()} -> {test['ds'].iloc[-1].date()})")

    model, forecast_train, forecast_test = build_prophet_model(train, test)

    plot_decomposition(model, forecast_train, train)
    plot_forecast(model, forecast_train, train)

    return {
        "model": model,
        "y_train": train["y"].values,
        "y_test": test["y"].values,
        "y_pred_train": forecast_train["yhat"].values,
        "y_pred_test": forecast_test["yhat"].values,
        "test_dates": test["ds"].values,
        "train_dates": train["ds"].values,
    }
