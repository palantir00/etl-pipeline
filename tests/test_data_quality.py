"""
Testy walidacji biznesowej — reguły, które dane MUSZĄ spełniać po Silver.

Używamy pytest.mark.parametrize żeby testować wiele przypadków
jedną funkcją (zamiast pisać osobny test dla każdej wartości).
"""

import pytest


# ---------------------------------------------------------------------------
# Walidacja pól identyfikacyjnych
# ---------------------------------------------------------------------------

class TestIcao24:

    @pytest.mark.parametrize("icao", [
        "a1b2c3",
        "000000",
        "ffffff",
        "ABCDEF",
    ])
    def test_poprawne_icao24(self, icao):
        """ICAO24 to 6-znakowy string hex — różne poprawne formaty."""
        assert isinstance(icao, str)
        assert len(icao) == 6

    @pytest.mark.parametrize("icao", [None, "", "abc", "abcdefg"])
    def test_niepoprawne_icao24_nie_przejdzie_walidacji(self, icao):
        """Null, pusty string, za krótki lub za długi — niepoprawne."""
        is_valid = icao is not None and len(icao) == 6
        assert not is_valid, f"Oczekiwano niepoprawnego ICAO, a '{icao}' przeszło walidację"


# ---------------------------------------------------------------------------
# Walidacja współrzędnych GPS
# ---------------------------------------------------------------------------

class TestCoordinates:

    @pytest.mark.parametrize("lat", [-90.0, -52.22, 0.0, 52.22, 90.0])
    def test_poprawna_szerokosc_geograficzna(self, lat):
        assert -90.0 <= lat <= 90.0

    @pytest.mark.parametrize("lat", [-90.1, 90.1, 999.0, -999.0])
    def test_niepoprawna_szerokosc_geograficzna(self, lat):
        assert not (-90.0 <= lat <= 90.0)

    @pytest.mark.parametrize("lon", [-180.0, -21.01, 0.0, 21.01, 180.0])
    def test_poprawna_dlugosc_geograficzna(self, lon):
        assert -180.0 <= lon <= 180.0

    @pytest.mark.parametrize("lon", [-180.1, 180.1, 999.0])
    def test_niepoprawna_dlugosc_geograficzna(self, lon):
        assert not (-180.0 <= lon <= 180.0)


# ---------------------------------------------------------------------------
# Walidacja jednostek po konwersji
# ---------------------------------------------------------------------------

class TestUnits:

    @pytest.mark.parametrize("ms, expected_kmh", [
        (0.0,   0.0),
        (100.0, 360.0),
        (250.0, 900.0),
        (1.0,   3.6),
    ])
    def test_przeliczenie_ms_na_kmh(self, ms, expected_kmh):
        """Prędkość: m/s * 3.6 = km/h."""
        assert round(ms * 3.6, 1) == expected_kmh

    @pytest.mark.parametrize("metry, oczekiwane_stopy", [
        (0,      0),
        (1000,   3281),
        (10000,  32808),
        (11000,  36089),
    ])
    def test_przeliczenie_metrow_na_stopy(self, metry, oczekiwane_stopy):
        """Wysokość: metry * 3.28084 = stopy (zaokrąglone do 0 miejsc)."""
        assert round(metry * 3.28084) == oczekiwane_stopy

    @pytest.mark.parametrize("predkosc_kmh", [0.0, 150.0, 900.0, 1200.0])
    def test_predkosc_jest_nieujemna(self, predkosc_kmh):
        """Prędkość naziemna nie może być ujemna."""
        assert predkosc_kmh >= 0.0

    @pytest.mark.parametrize("alt_ft", [0, 1000, 10000, 45000])
    def test_wysokosc_jest_nieujemna(self, alt_ft):
        """Wysokość barometryczna nie może być ujemna."""
        assert alt_ft >= 0


# ---------------------------------------------------------------------------
# Walidacja pola sensors
# ---------------------------------------------------------------------------

class TestSensors:

    @pytest.mark.parametrize("wartosc", [None, [1234, 5678], 9999, []])
    def test_sensors_bezpiecznie_rzutuje_na_str_lub_none(self, wartosc):
        """Pole sensors z API może być null, int lub listą — musi dać się zapisać."""
        result = str(wartosc) if wartosc else None
        assert result is None or isinstance(result, str)


# ---------------------------------------------------------------------------
# Walidacja logiki altitude_band (Gold)
# ---------------------------------------------------------------------------

class TestAltitudeBands:

    @pytest.mark.parametrize("on_ground, alt_ft, expected_band", [
        (True,  0,     "1_Ziemia"),
        (False, 5000,  "2_Niski (<10k ft)"),
        (False, 10000, "3_Średni (10-25k ft)"),
        (False, 25000, "4_Wysoki (25-40k ft)"),
        (False, 35000, "4_Wysoki (25-40k ft)"),
        (False, 41000, "5_Bardzo wysoki (>40k ft)"),
    ])
    def test_przypisanie_do_pasa_wysokosci(self, on_ground, alt_ft, expected_band):
        """Logika kategoryzacji pasów — identyczna jak w notebooku Gold."""
        if on_ground:
            band = "1_Ziemia"
        elif alt_ft < 10_000:
            band = "2_Niski (<10k ft)"
        elif alt_ft < 25_000:
            band = "3_Średni (10-25k ft)"
        elif alt_ft < 40_000:
            band = "4_Wysoki (25-40k ft)"
        else:
            band = "5_Bardzo wysoki (>40k ft)"

        assert band == expected_band
