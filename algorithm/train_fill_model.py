import joblib
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

HISTORICAL_DATA_PATH = "historical_fill_data.csv"
MODEL_PATH = "fill_level_model.pkl"


def main():
    df = pd.read_csv(HISTORICAL_DATA_PATH)

    # Drop rows without prediction target
    df = df.dropna(subset=["target_next_fill_pct"]).copy()

    feature_cols = [
        "bin_id",
        "location_type",
        "on_call",
        "eow",
        "service_time_min",
        "bin_type",
        "day_of_week",
        "month",
        "is_weekend",
        "is_game_day",
        "is_move_in",
        "is_move_out",
        "days_since_last_service",
        "service_performed",
        "fill_pct_start_of_day",
        "daily_fill_added_pct",
        "fill_pct",
        "fill_l",
        "fill_pct_lag_1",
        "fill_pct_lag_2",
        "fill_pct_lag_3",
        "rolling_mean_3",
    ]

    target_col = "target_next_fill_pct"

    X = df[feature_cols]
    y = df[target_col]

    categorical_cols = [
        "location_type",
        "bin_type",
        "day_of_week",
    ]

    numeric_cols = [
        "bin_id",
        "on_call",
        "eow",
        "service_time_min",
        "month",
        "is_weekend",
        "is_game_day",
        "is_move_in",
        "is_move_out",
        "days_since_last_service",
        "service_performed",
        "fill_pct_start_of_day",
        "daily_fill_added_pct",
        "fill_pct",
        "fill_l",
        "fill_pct_lag_1",
        "fill_pct_lag_2",
        "fill_pct_lag_3",
        "rolling_mean_3",
    ]

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", RandomForestRegressor(
                n_estimators=300,
                max_depth=12,
                min_samples_split=4,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
            )),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = mean_squared_error(y_test, preds) ** 0.5

    print(f"Training rows: {len(X_train)}")
    print(f"Test rows: {len(X_test)}")
    print(f"MAE:  {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")

    joblib.dump(model, MODEL_PATH)
    print(f"Saved model to {MODEL_PATH}")


if __name__ == "__main__":
    main()