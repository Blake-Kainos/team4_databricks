from unittest.mock import Mock, patch

import pandas as pd

from src.main import fetch_inspections


def test_fetch_inspections_returns_dataframe() -> None:
    payload = [{"id": "1", "inspection_result": "Pass"}]
    mock_response = Mock()
    mock_response.json.return_value = payload
    mock_response.raise_for_status.return_value = None

    with patch("src.main.requests.get", return_value=mock_response) as mock_get:
        df = fetch_inspections(limit=5)

    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="records") == payload
    mock_get.assert_called_once_with(
        "https://data.cityofchicago.org/resource/4dn8-eb3h.json",
        params={"$limit": 5},
    )


def test_fetch_inspections_uses_default_limit() -> None:
    mock_response = Mock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None

    with patch("src.main.requests.get", return_value=mock_response) as mock_get:
        fetch_inspections()

    mock_get.assert_called_once_with(
        "https://data.cityofchicago.org/resource/4dn8-eb3h.json",
        params={"$limit": 1000},
    )


def test_fetch_inspections_returns_empty_dataframe_for_empty_payload() -> None:
    mock_response = Mock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None

    with patch("src.main.requests.get", return_value=mock_response):
        df = fetch_inspections(limit=10)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_fetch_inspections_raises_for_http_errors() -> None:
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = RuntimeError("boom")

    with patch("src.main.requests.get", return_value=mock_response):
        try:
            fetch_inspections()
            assert False, "Expected RuntimeError"
        except RuntimeError as exc:
            assert str(exc) == "boom"
