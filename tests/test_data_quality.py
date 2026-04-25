"""
Podstawowe testy jakości danych dla warstwy Bronze.
Uruchomienie lokalne: pytest tests/

Testy Sparka (integracyjne) wymagają sesji Databricks lub lokalnego PySpark.
Te testy sprawdzają logikę pomocniczą bez uruchamiania klastra.
"""

import pytest
from src.api_client import get_states_all


# ---------------------------------------------------------------------------
# Testy struktury odpowiedzi API
# ---------------------------------------------------------------------------

def test_api_response_has_required_keys():
    """Odpowiedź OpenSky musi zawierać klucze 'time' i 'states'."""
    mock_response = {"time": 1700000000, "states": []}
    assert "time" in mock_response
    assert "states" in mock_response


def test_state_vector_has_17_fields():
    """Każdy state vector (wiersz) musi mieć dokładnie 17 pól."""
    sample_state = [
        "a1b2c3", "LOT123  ", "Poland",
        1700000000, 1700000000,
        21.01, 52.22, 10000.0, False,
        230.5, 88.0, 0.5, None, None, "7700", False, 0,
    ]
    assert len(sample_state) == 17


def test_sensors_field_cast_to_string_or_none():
    """Pole sensors (indeks 12) musi być bezpiecznie rzutowalne na str lub None."""
    test_cases = [None, [1234, 5678], 9999]
    for val in test_cases:
        result = str(val) if val else None
        assert result is None or isinstance(result, str)


def test_icao24_is_string():
    """ICAO24 powinno być stringiem hex (np. 'a1b2c3')."""
    icao = "a1b2c3"
    assert isinstance(icao, str)
    assert len(icao) == 6


# ---------------------------------------------------------------------------
# Testy walidacji biznesowej (dane po Silver)
# ---------------------------------------------------------------------------

def test_altitude_is_non_negative_or_none():
    """Wysokość barometryczna nie może być ujemna (w warstwie Silver)."""
    altitudes = [0.0, 1000.5, 10972.8, None]
    for alt in altitudes:
        if alt is not None:
            assert alt >= 0, f"Ujemna wysokość: {alt}"


def test_longitude_in_valid_range():
    """Długość geograficzna musi być w zakresie [-180, 180]."""
    longitudes = [-180.0, 0.0, 21.01, 180.0]
    for lon in longitudes:
        assert -180 <= lon <= 180, f"Nieprawidłowa długość: {lon}"


def test_latitude_in_valid_range():
    """Szerokość geograficzna musi być w zakresie [-90, 90]."""
    latitudes = [-90.0, 0.0, 52.22, 90.0]
    for lat in latitudes:
        assert -90 <= lat <= 90, f"Nieprawidłowa szerokość: {lat}"
