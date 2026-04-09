import pandas as pd
import pytest

import predict_fill_levels as pfl


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def make_history_df():
    return pd.DataFrame(
        [
            {
                "bin_id": 1,
                "date": "2026-04-01",
                "day_of_week": "Tue",
                "month": 4,
                "is_weekend": 0,
                "is_game_day": 0,
                "is_move_in": 0,
                "is_move_out": 0,
                "days_since_last_service": 2,
                "service_performed": 0,
                "fill_pct_start_of_day": 60.0,
                "daily_fill_added_pct": 10.0,
                "fill_pct": 70.0,
                "fill_l": 245.0,
                "fill_pct_lag_1": 55.0,
                "fill_pct_lag_2": 45.0,
                "fill_pct_lag_3": 35.0,
                "rolling_mean_3": 45.0,
            },
            {
                "bin_id": 1,
                "date": "2026-04-03",
                "day_of_week": "Fri",
                "month": 4,
                "is_weekend": 0,
                "is_game_day": 0,
                "is_move_in": 0,
                "is_move_out": 0,
                "days_since_last_service": 4,
                "service_performed": 0,
                "fill_pct_start_of_day": 75.0,
                "daily_fill_added_pct": 8.0,
                "fill_pct": 83.0,
                "fill_l": 290.5,
                "fill_pct_lag_1": 70.0,
                "fill_pct_lag_2": 60.0,
                "fill_pct_lag_3": 50.0,
                "rolling_mean_3": 60.0,
            },
            {
                "bin_id": 21,
                "date": "2026-04-03",
                "day_of_week": "Thu",
                "month": 4,
                "is_weekend": 0,
                "is_game_day": 0,
                "is_move_in": 0,
                "is_move_out": 0,
                "days_since_last_service": 10,
                "service_performed": 0,
                "fill_pct_start_of_day": 100.0,
                "daily_fill_added_pct": 5.0,
                "fill_pct": 100.0,
                "fill_l": 350.0,
                "fill_pct_lag_1": 100.0,
                "fill_pct_lag_2": 100.0,
                "fill_pct_lag_3": 95.0,
                "rolling_mean_3": 98.0,
            },
        ]
    )


def make_current_bins_df():
    return pd.DataFrame(
        [
            {
                "vt_id": 1,
                "location": "West End Market (Dining Hall)",
                "location_type": "dining_hall",
                "on_call": False,
                "eow": False,
                "days_csv": "Tue,Fri",
                "service_time_min": 7,
                "bin_type": "compactor",
                "notes": "Dining hall"
            },
            {
                "vt_id": 21,
                "location": "Kentland Farms (Dairy Barn)",
                "location_type": "agricultural",
                "on_call": False,
                "eow": True,
                "days_csv": "Thu",
                "service_time_min": 15,
                "bin_type": "compactor",
                "notes": "Remote"
            },
            {
                "vt_id": 999,
                "location": "Squires",
                "location_type": None,
                "on_call": False,
                "eow": False,
                "days_csv": "Mon,Wed,Fri",
                "service_time_min": None,
                "bin_type": None,
                "notes": "Missing history row"
            },
        ]
    )


# -------------------------------------------------------------------
# Basic coordinate tests
# -------------------------------------------------------------------

def test_get_coordinates_exact():
    lat, lon = pfl.get_coordinates("Squires")
    assert isinstance(lat, float)
    assert isinstance(lon, float)


def test_get_coordinates_vet_med_fallback():
    lat, lon = pfl.get_coordinates("Vet Med Unknown Location")
    assert (lat, lon) == pfl.VT_LOCATION_COORDS["Vet Med (Straw Beside Bldg)"]


def test_get_coordinates_missing_raises():
    with pytest.raises(ValueError):
        pfl.get_coordinates("Totally Fake Building")


# -------------------------------------------------------------------
# build_prediction_frame tests
# -------------------------------------------------------------------

def test_build_prediction_frame_uses_latest_history_row():
    history_df = make_history_df()
    current_bins_df = make_current_bins_df()

    out = pfl.build_prediction_frame(history_df, current_bins_df)

    row = out[out["bin_id"] == 1].iloc[0]

    # should use the latest row (2026-04-03)
    assert row["fill_pct"] == pytest.approx(83.0)
    assert row["fill_pct_start_of_day"] == pytest.approx(75.0)
    assert row["rolling_mean_3"] == pytest.approx(60.0)


def test_build_prediction_frame_fills_missing_defaults():
    history_df = make_history_df()
    current_bins_df = make_current_bins_df()

    out = pfl.build_prediction_frame(history_df, current_bins_df)

    row = out[out["bin_id"] == 999].iloc[0]

    # defaults from your script
    assert row["location_type"] == "standard"
    assert row["service_time_min"] == 5
    assert row["bin_type"] == "compactor"
    assert row["fill_pct"] == 35
    assert row["fill_pct_start_of_day"] == 25
    assert row["rolling_mean_3"] == 30


def test_build_prediction_frame_adds_coordinates():
    history_df = make_history_df()
    current_bins_df = make_current_bins_df()

    out = pfl.build_prediction_frame(history_df, current_bins_df)

    assert "latitude" in out.columns
    assert "longitude" in out.columns

    row = out[out["bin_id"] == 1].iloc[0]
    expected = pfl.VT_LOCATION_COORDS["West End Market (Dining Hall)"]

    assert row["latitude"] == pytest.approx(expected[0])
    assert row["longitude"] == pytest.approx(expected[1])


# -------------------------------------------------------------------
# selection logic tests
# -------------------------------------------------------------------

def test_add_selection_logic_current_fill():
    df = pd.DataFrame(
        [
            {
                "bin_id": 1,
                "bin_type": "compactor",
                "fill_pct": 85.0,
                "predicted_fill_pct": 50.0,
            }
        ]
    )

    out = pfl.add_selection_logic(df)
    row = out.iloc[0]

    assert row["selection_reason"] == "current_fill_above_threshold"
    assert row["selected_for_pickup"] is True
    assert row["capacity_l"] == 350
    assert row["current_fill_l"] == pytest.approx(297.5)
    assert row["predicted_fill_l"] == pytest.approx(175.0)


def test_add_selection_logic_predicted_fill():
    df = pd.DataFrame(
        [
            {
                "bin_id": 2,
                "bin_type": "standard",
                "fill_pct": 40.0,
                "predicted_fill_pct": 82.0,
            }
        ]
    )

    out = pfl.add_selection_logic(df)
    row = out.iloc[0]

    assert row["selection_reason"] == "predicted_fill_above_threshold"
    assert row["selected_for_pickup"] is True
    assert row["capacity_l"] == 300
    assert row["predicted_fill_l"] == pytest.approx(246.0)


def test_add_selection_logic_not_selected():
    df = pd.DataFrame(
        [
            {
                "bin_id": 3,
                "bin_type": "metal",
                "fill_pct": 20.0,
                "predicted_fill_pct": 40.0,
            }
        ]
    )

    out = pfl.add_selection_logic(df)
    row = out.iloc[0]

    assert row["selection_reason"] == "not_selected"
    assert row["selected_for_pickup"] is False
    assert row["capacity_l"] == 900


def test_add_selection_logic_unknown_bin_type_uses_default_capacity():
    df = pd.DataFrame(
        [
            {
                "bin_id": 4,
                "bin_type": "mystery_bin",
                "fill_pct": 50.0,
                "predicted_fill_pct": 90.0,
            }
        ]
    )

    out = pfl.add_selection_logic(df)
    row = out.iloc[0]

    assert row["capacity_l"] == 350


# -------------------------------------------------------------------
# mocked main() integration test
# -------------------------------------------------------------------

class DummyModel:
    def predict(self, X):
        # predictable fake outputs for 3 rows
        return [81.0, 99.0, 20.0]


def test_main_writes_output(monkeypatch):
    history_df = make_history_df()
    current_bins_df = make_current_bins_df()

    captured = {}

    def fake_read_csv(path):
        if path == pfl.HISTORICAL_DATA_PATH:
            return history_df.copy()
        if path == pfl.CURRENT_BINS_PATH:
            return current_bins_df.copy()
        raise ValueError(f"Unexpected path: {path}")

    def fake_to_csv(self, path, index=False):
        captured["path"] = path
        captured["index"] = index
        captured["df"] = self.copy()

    monkeypatch.setattr(pfl.joblib, "load", lambda path: DummyModel())
    monkeypatch.setattr(pfl.pd, "read_csv", fake_read_csv)
    monkeypatch.setattr(pd.DataFrame, "to_csv", fake_to_csv, raising=False)

    pfl.main()

    assert captured["path"] == pfl.OUTPUT_PATH
    assert captured["index"] is False

    out = captured["df"]
    assert "predicted_fill_pct" in out.columns
    assert "selected_for_pickup" in out.columns
    assert "latitude" in out.columns
    assert "longitude" in out.columns

    # predictions came from DummyModel
    assert out.loc[out["bin_id"] == 1, "predicted_fill_pct"].iloc[0] == pytest.approx(81.0)
    assert out.loc[out["bin_id"] == 21, "predicted_fill_pct"].iloc[0] == pytest.approx(99.0)
    assert out.loc[out["bin_id"] == 999, "predicted_fill_pct"].iloc[0] == pytest.approx(20.0)

    # selection logic should have run
    assert out.loc[out["bin_id"] == 1, "selected_for_pickup"].iloc[0] is True
    assert out.loc[out["bin_id"] == 21, "selected_for_pickup"].iloc[0] is True
    assert out.loc[out["bin_id"] == 999, "selected_for_pickup"].iloc[0] is False