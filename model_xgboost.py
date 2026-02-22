"""
Giai đoạn 3A: XGBoost với Feature Engineering bằng PySpark
- Feature engineering: PySpark Window functions
- Hyperparameter tuning: Optuna
- Training: XGBoost (pandas/numpy)
"""

import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import xgboost as xgb
import optuna
import os
import json
import warnings

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def create_features_spark(spark, spark_df, target="Gold", lags=[1, 2, 3, 5, 7, 10, 14, 21]):
    """Feature engineering bằng PySpark Window functions."""
    print("\n  [PySpark] Feature Engineering with Window functions...")

    df = spark_df.select("date", "Gold", "WTI", "DXY")
    window_date = Window.orderBy("date")

    # 1. Lag features
    for lag in lags:
        df = df.withColumn(
            f"Gold_lag_{lag}",
            F.lag("Gold", lag).over(window_date)
        )
    print(f"    Created {len(lags)} lag features")

    # 2. Rolling statistics (Window functions)
    roll_windows = [5, 10, 21]
    for w in roll_windows:
        win = Window.orderBy("date").rowsBetween(-(w - 1), 0)
        df = df.withColumn(f"Gold_MA_{w}", F.avg("Gold").over(win))
        df = df.withColumn(f"Gold_STD_{w}", F.stddev("Gold").over(win))
        df = df.withColumn(f"Gold_MIN_{w}", F.min("Gold").over(win))
        df = df.withColumn(f"Gold_MAX_{w}", F.max("Gold").over(win))
    print(f"    Created rolling stats for windows {roll_windows}")

    # 3. Returns
    df = df.withColumn("Gold_return_1d",
                       (F.col("Gold") - F.lag("Gold", 1).over(window_date)) / F.lag("Gold", 1).over(window_date))
    df = df.withColumn("Gold_return_5d",
                       (F.col("Gold") - F.lag("Gold", 5).over(window_date)) / F.lag("Gold", 5).over(window_date))
    print("    Created return features")

    # 4. Ratios
    df = df.withColumn("Gold_WTI_ratio", F.col("Gold") / F.col("WTI"))
    df = df.withColumn("Gold_DXY_ratio", F.col("Gold") / F.col("DXY"))
    print("    Created ratio features")

    # 5. Date features
    df = df.withColumn("day_of_week", F.dayofweek("date"))
    df = df.withColumn("month", F.month("date"))
    df = df.withColumn("quarter", F.quarter("date"))
    print("    Created date features")

    # Drop nulls (từ lag/rolling)
    df = df.dropna()

    total_features = len(df.columns) - 2  # trừ date và Gold
    print(f"    Total features: {total_features}")
    print(f"    Records after feature engineering: {df.count()}")

    return df


def spark_features_to_pandas(spark_features_df):
    """Chuyển Spark features DataFrame sang pandas cho XGBoost training."""
    print("  [PySpark] Converting features to pandas for XGBoost...")
    pdf = spark_features_df.toPandas()
    pdf = pdf.set_index("date").sort_index()

    target_col = "Gold"
    feature_cols = [c for c in pdf.columns if c != target_col]

    X = pdf[feature_cols]
    y = pdf[target_col]

    return X, y, feature_cols


def temporal_split(X, y, test_ratio=0.2):
    """Chia train/test theo thời gian."""
    split_idx = int(len(X) * (1 - test_ratio))

    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print(f"\n  Train/Test Split (temporal):")
    print(f"    Train: {len(X_train)} samples ({X_train.index[0].date()} -> {X_train.index[-1].date()})")
    print(f"    Test:  {len(X_test)} samples ({X_test.index[0].date()} -> {X_test.index[-1].date()})")

    return X_train, X_test, y_train, y_test


def optimize_with_optuna(X_train, y_train, n_trials=50):
    """Bayesian Optimization cho XGBoost."""
    print(f"\n  [Optuna] Bayesian Optimization ({n_trials} trials)...")

    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            "gamma": trial.suggest_float("gamma", 1e-8, 5.0, log=True),
        }

        from sklearn.model_selection import TimeSeriesSplit

        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for train_idx, val_idx in tscv.split(X_train):
            X_t, X_v = X_train.iloc[train_idx], X_train.iloc[val_idx]
            y_t, y_v = y_train.iloc[train_idx], y_train.iloc[val_idx]

            model = xgb.XGBRegressor(**params, random_state=42, verbosity=0, n_jobs=-1)
            model.fit(X_t, y_t, eval_set=[(X_v, y_v)], verbose=False)

            pred = model.predict(X_v)
            rmse = np.sqrt(np.mean((y_v - pred) ** 2))
            scores.append(rmse)

        return np.mean(scores)

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_params = study.best_params
    print(f"    Best RMSE (CV): {study.best_value:.4f}")
    print(f"    Best params: {best_params}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "xgboost_best_params.json"), "w") as f:
        json.dump(best_params, f, indent=2)

    return best_params, study


def train_final_model(X_train, X_test, y_train, y_test, best_params):
    """Train XGBoost với best params."""
    print("\n  Training final XGBoost model...")

    model = xgb.XGBRegressor(**best_params, random_state=42, verbosity=0, n_jobs=-1)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    train_rmse = np.sqrt(np.mean((y_train - y_pred_train) ** 2))
    test_rmse = np.sqrt(np.mean((y_test - y_pred_test) ** 2))

    print(f"    Train RMSE: {train_rmse:.4f}")
    print(f"    Test RMSE:  {test_rmse:.4f}")

    # Feature importance
    importance = model.feature_importances_
    feat_imp = pd.DataFrame({
        "Feature": X_train.columns,
        "Importance": importance,
    }).sort_values("Importance", ascending=False).reset_index(drop=True)

    print(f"\n  Top 5 features:")
    for _, row in feat_imp.head(5).iterrows():
        bar = "█" * int(row["Importance"] * 100)
        print(f"    {row['Feature']:30s} {row['Importance']:.4f} {bar}")

    return model, y_pred_train, y_pred_test, feat_imp


def run(spark, spark_df, merged):
    """Pipeline XGBoost với PySpark feature engineering."""
    print("\n" + "=" * 60)
    print("  PHASE 3A: XGBOOST MODEL (PySpark Features)")
    print("=" * 60)

    # 1. Feature engineering bằng PySpark
    features_df = create_features_spark(spark, spark_df)

    # 2. Chuyển sang pandas
    X, y, feature_cols = spark_features_to_pandas(features_df)

    # 3. Split
    X_train, X_test, y_train, y_test = temporal_split(X, y)

    # 4. Optimize
    best_params, study = optimize_with_optuna(X_train, y_train, n_trials=50)

    # 5. Train
    model, y_pred_train, y_pred_test, feat_imp = train_final_model(
        X_train, X_test, y_train, y_test, best_params
    )

    return {
        "model": model,
        "y_train": y_train.values,
        "y_test": y_test.values,
        "y_pred_train": y_pred_train,
        "y_pred_test": y_pred_test,
        "test_index": X_test.index,
        "train_index": X_train.index,
        "feature_importance": feat_imp,
        "best_params": best_params,
        "feature_cols": feature_cols,
    }
