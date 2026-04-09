import pandas as pd
import pytest

import route_optimization_algo as roa


def make_base_df():
    return pd.DataFrame(
        [
            {
                "bin_id": 1,
                "location": "West End Market (Dining Hall)",
                "location_type": "dining_hall",
                "on_call": False,
                "eow": False,
                "days_csv": "Tue,Fri",
                "service_time_min": 7,
                "fill_pct": 82.0,
                "predicted_fill_pct": 40.0,
                "predicted_fill_l": 140.0,
                "latitude": 37.2268,
                "longitude": -80.4265,
            },
            {
                "bin_id": 2,
                "location": "Squires",
                "location_type": "standard",
                "on_call": False,
                "eow": False,
                "days_csv": "Mon,Wed,Fri",
                "service_time_min": 5,
                "fill_pct": 40.0,
                "predicted_fill_pct": 83.0,
                "predicted_fill_l": 120.0,
                "latitude": 37.2294,
                "longitude": -80.4185,
            },
            {
                "bin_id": 3,
                "location": "Civil Engineering",
                "location_type": "academic",
                "on_call": True,
                "eow": False,
                "days_csv": "",
                "service_time_min": 5,
                "fill_pct": 20.0,
                "predicted_fill_pct": 30.0,
                "predicted_fill_l": 80.0,
                "latitude": 37.2290,
                "longitude": -80.4240,
            },
            {
                "bin_id": 21,
                "location": "Kentland Farms (Dairy Barn)",
                "location_type": "agricultural",
                "on_call": False,
                "eow": True,
                "days_csv": "Thu",
                "service_time_min": 15,
                "fill_pct": 100.0,
                "predicted_fill_pct": 100.0,
                "predicted_fill_l": 350.0,
                "latitude": 37.1975,
                "longitude": -80.5805,
            },
        ]
    )


def prep_df(df):
    df = roa.add_estimated_loads(df)
    df = roa.merge_live_data(df)
    return df


def test_safe_float():
    assert roa.safe_float("12.5") == 12.5
    assert roa.safe_float(None, 7.0) == 7.0
    assert roa.safe_float("bad", 3.5) == 3.5


def test_safe_int():
    assert roa.safe_int("12") == 12
    assert roa.safe_int(None, 7) == 7
    assert roa.safe_int("bad", 3) == 3


def test_haversine_km_zero():
    assert roa.haversine_km(37.0, -80.0, 37.0, -80.0) == pytest.approx(0.0)


def test_km_to_minutes():
    assert roa.km_to_minutes(30, speed_kmh=30) == 60


def test_recommended_trucks():
    assert roa.recommended_trucks("Mon") == roa.WEEKDAY_TRUCKS
    assert roa.recommended_trucks("Sun") == roa.WEEKEND_TRUCKS


def test_load_data_success(tmp_path, monkeypatch):
    df = make_base_df()
    csv_path = tmp_path / "MeridianData_with_predictions.csv"
    df.to_csv(csv_path, index=False)

    monkeypatch.setattr(roa, "PREDICTION_OUTPUT", str(csv_path))

    loaded = roa.load_data()
    assert len(loaded) == 4
    assert {"bin_id", "latitude", "longitude", "fill_pct", "predicted_fill_pct", "predicted_fill_l"}.issubset(loaded.columns)


def test_load_data_missing_columns(tmp_path, monkeypatch):
    bad = pd.DataFrame([{"bin_id": 1, "location": "A", "fill_pct": 90}])
    csv_path = tmp_path / "MeridianData_with_predictions.csv"
    bad.to_csv(csv_path, index=False)

    monkeypatch.setattr(roa, "PREDICTION_OUTPUT", str(csv_path))

    with pytest.raises(ValueError):
        roa.load_data()


def test_filter_for_service_day_includes_matching_and_on_call():
    df = make_base_df()
    out = roa.filter_for_service_day(df, "Fri")
    out_ids = set(out["bin_id"].tolist())

    assert 1 in out_ids
    assert 2 in out_ids
    assert 3 in out_ids
    assert 21 not in out_ids


def test_filter_for_service_day_blank_returns_all():
    df = make_base_df()
    out = roa.filter_for_service_day(df, "")
    assert len(out) == len(df)


def test_apply_special_event_flags_game_day():
    df = prep_df(make_base_df())
    out = roa.apply_special_event_flags(df, "game_day")

    dining_before = df.loc[df["bin_id"] == 1, "predicted_fill_pct"].iloc[0]
    dining_after = out.loc[out["bin_id"] == 1, "predicted_fill_pct"].iloc[0]
    assert dining_after == pytest.approx(dining_before + 8)

    academic_before = df.loc[df["bin_id"] == 3, "predicted_fill_pct"].iloc[0]
    academic_after = out.loc[out["bin_id"] == 3, "predicted_fill_pct"].iloc[0]
    assert academic_after == academic_before


def test_apply_special_event_flags_move_in():
    df = make_base_df()
    df.loc[df["bin_id"] == 2, "location_type"] = "residential"
    df = prep_df(df)
    out = roa.apply_special_event_flags(df, "move_in")

    res_before = df.loc[df["bin_id"] == 2, "predicted_fill_pct"].iloc[0]
    res_after = out.loc[out["bin_id"] == 2, "predicted_fill_pct"].iloc[0]
    assert res_after == pytest.approx(res_before + 10)


def test_select_bins_current_fill_predicted_fill_and_urgent():
    df = prep_df(make_base_df())

    out = roa.select_bins(df, urgent_ids=[3])
    out_ids = set(out["bin_id"].tolist())

    assert 1 in out_ids
    assert 2 in out_ids
    assert 3 in out_ids
    assert 21 in out_ids

    reason_1 = out.loc[out["bin_id"] == 1, "selection_reason"].iloc[0]
    reason_2 = out.loc[out["bin_id"] == 2, "selection_reason"].iloc[0]
    reason_3 = out.loc[out["bin_id"] == 3, "selection_reason"].iloc[0]

    assert reason_1 == "current_csv_fill_above_threshold"
    assert reason_2 == "predicted_fill_above_threshold"
    assert reason_3 == "urgent_override"


def test_select_bins_none_selected():
    df = make_base_df()
    df["fill_pct"] = 10.0
    df["predicted_fill_pct"] = 20.0
    df["predicted_fill_l"] = 20.0
    df = prep_df(df)

    out = roa.select_bins(df, urgent_ids=[])
    assert out.empty


def test_split_remote_bins():
    df = prep_df(make_base_df())
    selected = roa.select_bins(df, urgent_ids=[])

    campus_df, remote_df = roa.split_remote_bins(selected)

    campus_ids = set(campus_df["bin_id"].tolist()) if not campus_df.empty else set()
    remote_ids = set(remote_df["bin_id"].tolist()) if not remote_df.empty else set()

    assert 21 in remote_ids
    assert 1 in campus_ids


def test_build_remote_routes():
    df = prep_df(make_base_df())
    selected = roa.select_bins(df, urgent_ids=[])
    remote_df = selected[selected["bin_id"] == 21].copy()

    routes = roa.build_remote_routes(remote_df, start_vehicle_id=4)

    assert len(routes) == 1
    r = routes[0]
    assert r["vehicle_id"] == 4
    assert r["route_labels"] == [roa.DEPOT_NAME, "21", roa.DEPOT_NAME]
    assert r["load_l"] > 0
    assert r["distance_km"] > 0
    assert r["time_min"] > 0


def test_build_haversine_matrix_shapes():
    coords = [
        (37.143069, -80.419532),
        (37.2268, -80.4265),
        (37.2294, -80.4185),
    ]
    dist, tmat = roa.build_haversine_matrix(coords)

    assert len(dist) == 3
    assert len(dist[0]) == 3
    assert len(tmat) == 3
    assert dist[0][0] == 0
    assert tmat[1][1] == 0


def test_fetch_osrm_matrix_failure_falls_back_in_solver(monkeypatch):
    df = prep_df(make_base_df())
    selected = roa.select_bins(df, urgent_ids=[])
    campus_df = selected[selected["bin_id"].isin([1, 2])].copy()

    def fail_osrm(coords):
        raise RuntimeError("OSRM down")

    monkeypatch.setattr(roa, "fetch_osrm_matrix", fail_osrm)

    result = roa.solve_balanced_routes(campus_df, num_trucks=2)
    assert result is not None
    assert result["using_haversine"] is True
    assert len(result["routes"]) >= 1


def test_solve_balanced_routes_basic(monkeypatch):
    df = prep_df(make_base_df())
    selected = roa.select_bins(df, urgent_ids=[])
    campus_df = selected[selected["bin_id"].isin([1, 2])].copy()

    def fake_osrm(coords):
        n = len(coords)
        dist = [[0.0] * n for _ in range(n)]
        tmat = [[0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if i != j:
                    dist[i][j] = 5.0
                    tmat[i][j] = 10
        return dist, tmat

    monkeypatch.setattr(roa, "fetch_osrm_matrix", fake_osrm)

    result = roa.solve_balanced_routes(campus_df, num_trucks=2)
    assert result is not None
    assert "routes" in result
    assert result["total_distance_km"] >= 0
    assert result["total_time_min"] >= 0


def test_end_to_end_selection_and_split():
    df = make_base_df()
    day_filtered = roa.filter_for_service_day(df, "Fri")
    day_filtered = prep_df(day_filtered)
    boosted = roa.apply_special_event_flags(day_filtered, "none")
    selected = roa.select_bins(boosted, urgent_ids=[3])
    campus_df, remote_df = roa.split_remote_bins(selected)

    selected_ids = set(selected["bin_id"].tolist())
    assert selected_ids == {1, 2, 3}
    assert remote_df.empty


def test_dynamic_campus_truck_count_logic():
    campus_df = pd.DataFrame([{"bin_id": 1}, {"bin_id": 2}, {"bin_id": 3}])
    remote_df = pd.DataFrame([{"bin_id": 21}])

    num_trucks = 7
    campus_trucks = min(num_trucks - len(remote_df), len(campus_df))
    campus_trucks = max(1, campus_trucks)

    assert campus_trucks == 3