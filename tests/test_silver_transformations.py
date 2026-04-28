"""
Testy transformacji Silver — używają lokalnego PySpark (bez Databricks).

Każdy test:
1. Tworzy mały DataFrame z kilkoma wierszami (w tym celowo błędnymi)
2. Wywołuje jedną funkcję transformacji z src/transformations.py
3. Sprawdza czy wynik jest dokładnie taki jak oczekujemy

Fixtures `spark`, `sample_row` i `bronze_schema` są zdefiniowane w conftest.py
i pytest wstrzykuje je automatycznie przez nazwę argumentu.
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import TimestampType
from src import transformations as T


# ---------------------------------------------------------------------------
# Pomocnik — tworzy DataFrame z listy słowników
# ---------------------------------------------------------------------------

def make_df(spark, rows: list[dict], schema):
    """Skrót: lista słowników → DataFrame ze zdefiniowanym schematem."""
    return spark.createDataFrame([Row(**r) for r in rows], schema=schema)


# ---------------------------------------------------------------------------
# filter_required_fields
# ---------------------------------------------------------------------------

class TestFilterRequiredFields:

    def test_usuwa_wiersze_bez_icao24(self, spark, sample_row, bronze_schema):
        row_bez_icao = {**sample_row, "icao24": None}
        df = make_df(spark, [sample_row, row_bez_icao], bronze_schema)

        result = T.filter_required_fields(df)

        assert result.count() == 1
        assert result.first()["icao24"] == "a1b2c3"

    def test_usuwa_wiersze_bez_latitude(self, spark, sample_row, bronze_schema):
        row_bez_lat = {**sample_row, "icao24": "zzzzzz", "latitude": None}
        df = make_df(spark, [sample_row, row_bez_lat], bronze_schema)

        result = T.filter_required_fields(df)

        assert result.count() == 1

    def test_usuwa_wiersze_bez_longitude(self, spark, sample_row, bronze_schema):
        row_bez_lon = {**sample_row, "icao24": "zzzzzz", "longitude": None}
        df = make_df(spark, [sample_row, row_bez_lon], bronze_schema)

        result = T.filter_required_fields(df)

        assert result.count() == 1

    def test_zachowuje_poprawny_wiersz(self, spark, sample_row, bronze_schema):
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.filter_required_fields(df)

        assert result.count() == 1

    def test_pusty_dataframe_pozostaje_pusty(self, spark, bronze_schema):
        df = make_df(spark, [], bronze_schema)

        result = T.filter_required_fields(df)

        assert result.count() == 0


# ---------------------------------------------------------------------------
# filter_valid_coordinates
# ---------------------------------------------------------------------------

class TestFilterValidCoordinates:

    def test_usuwa_latitude_powyzej_90(self, spark, sample_row, bronze_schema):
        bledny = {**sample_row, "icao24": "err001", "latitude": 91.0}
        df = make_df(spark, [sample_row, bledny], bronze_schema)

        result = T.filter_valid_coordinates(df)

        assert result.count() == 1

    def test_usuwa_longitude_ponizej_minus180(self, spark, sample_row, bronze_schema):
        bledny = {**sample_row, "icao24": "err002", "longitude": -181.0}
        df = make_df(spark, [sample_row, bledny], bronze_schema)

        result = T.filter_valid_coordinates(df)

        assert result.count() == 1

    def test_zachowuje_graniczne_wartosci(self, spark, sample_row, bronze_schema):
        # Wartości graniczne (-90, 90, -180, 180) są poprawne
        polnocny_biegun = {**sample_row, "icao24": "aaa001", "latitude": 90.0,  "longitude": 0.0}
        poludniowy_biegun = {**sample_row, "icao24": "aaa002", "latitude": -90.0, "longitude": 0.0}
        df = make_df(spark, [polnocny_biegun, poludniowy_biegun], bronze_schema)

        result = T.filter_valid_coordinates(df)

        assert result.count() == 2


# ---------------------------------------------------------------------------
# clean_callsign
# ---------------------------------------------------------------------------

class TestCleanCallsign:

    def test_przycina_spacje(self, spark, sample_row, bronze_schema):
        # sample_row ma callsign = "LOT123  " (ze spacjami)
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.clean_callsign(df)

        assert result.first()["callsign"] == "LOT123"

    def test_zamienia_na_wielkie_litery(self, spark, sample_row, bronze_schema):
        malymi = {**sample_row, "callsign": "lot123"}
        df = make_df(spark, [malymi], bronze_schema)

        result = T.clean_callsign(df)

        assert result.first()["callsign"] == "LOT123"

    def test_null_callsign_pozostaje_nullem(self, spark, sample_row, bronze_schema):
        bez_callsign = {**sample_row, "callsign": None}
        df = make_df(spark, [bez_callsign], bronze_schema)

        result = T.clean_callsign(df)

        assert result.first()["callsign"] is None

    def test_callsign_tylko_spacje_daje_pusty_string(self, spark, sample_row, bronze_schema):
        # "    " po trim() staje się "" (pusty string, nie null)
        same_spacje = {**sample_row, "callsign": "    "}
        df = make_df(spark, [same_spacje], bronze_schema)

        result = T.clean_callsign(df)

        assert result.first()["callsign"] == ""


# ---------------------------------------------------------------------------
# convert_units
# ---------------------------------------------------------------------------

class TestConvertUnits:

    def test_velocity_ms_na_kmh(self, spark, sample_row, bronze_schema):
        # sample_row ma velocity = 100.0 m/s → 100 * 3.6 = 360.0 km/h
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.convert_units(df)

        assert result.first()["velocity_kmh"] == 360.0

    def test_baro_altitude_metry_na_stopy(self, spark, sample_row, bronze_schema):
        # 10000 m * 3.28084 = 32808.4 → zaokrąglone do 32808
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.convert_units(df)

        assert result.first()["baro_altitude_ft"] == 32808

    def test_null_velocity_pozostaje_nullem(self, spark, sample_row, bronze_schema):
        bez_predkosci = {**sample_row, "velocity": None}
        df = make_df(spark, [bez_predkosci], bronze_schema)

        result = T.convert_units(df)

        assert result.first()["velocity_kmh"] is None

    def test_null_altitude_pozostaje_nullem(self, spark, sample_row, bronze_schema):
        bez_wysokosci = {**sample_row, "baro_altitude": None}
        df = make_df(spark, [bez_wysokosci], bronze_schema)

        result = T.convert_units(df)

        assert result.first()["baro_altitude_ft"] is None


# ---------------------------------------------------------------------------
# convert_timestamps
# ---------------------------------------------------------------------------

class TestConvertTimestamps:

    def test_tworzy_kolumne_time_position_ts(self, spark, sample_row, bronze_schema):
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.convert_timestamps(df)

        assert "time_position_ts" in result.columns
        assert "last_contact_ts" in result.columns
        assert "snapshot_ts" in result.columns

    def test_timestamp_ma_poprawny_typ(self, spark, sample_row, bronze_schema):
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.convert_timestamps(df)

        ts_field = next(f for f in result.schema.fields if f.name == "time_position_ts")
        assert isinstance(ts_field.dataType, TimestampType)

    def test_null_time_daje_null_timestamp(self, spark, sample_row, bronze_schema):
        bez_czasu = {**sample_row, "time_position": None}
        df = make_df(spark, [bez_czasu], bronze_schema)

        result = T.convert_timestamps(df)

        assert result.first()["time_position_ts"] is None


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:

    def test_usuwa_duplikaty_tego_samego_samolotu(self, spark, sample_row, bronze_schema):
        # Dwa identyczne wiersze (ten sam icao24 i snapshot_time)
        df = make_df(spark, [sample_row, sample_row], bronze_schema)

        result = T.deduplicate(df)

        assert result.count() == 1

    def test_zachowuje_rozne_samoloty(self, spark, sample_row, bronze_schema):
        inny = {**sample_row, "icao24": "x9y8z7"}
        df = make_df(spark, [sample_row, inny], bronze_schema)

        result = T.deduplicate(df)

        assert result.count() == 2

    def test_ten_sam_samolot_rozny_snapshot(self, spark, sample_row, bronze_schema):
        # Ten sam samolot, ale dwa różne momenty w czasie → oba zostają
        pozniej = {**sample_row, "snapshot_time": 1700003600}  # godzinę później
        df = make_df(spark, [sample_row, pozniej], bronze_schema)

        result = T.deduplicate(df)

        assert result.count() == 2


# ---------------------------------------------------------------------------
# run_all — test end-to-end całego pipeline'u Silver
# ---------------------------------------------------------------------------

class TestRunAll:

    def test_caly_pipeline_na_mieszcze_danych(self, spark, sample_row, bronze_schema):
        """Poprawny wiersz powinien przejść przez cały pipeline bez utraty."""
        df = make_df(spark, [sample_row], bronze_schema)

        result = T.run_all(df)

        assert result.count() == 1
        row = result.first()
        assert row["callsign"] == "LOT123"      # przycięty
        assert row["velocity_kmh"] == 360.0     # przeliczony
        assert row["baro_altitude_ft"] == 32808 # przeliczony
        assert "time_position_ts" in result.columns

    def test_bledne_wiersze_sa_odfiltrowane(self, spark, sample_row, bronze_schema):
        """Wiersz bez icao24 nie powinien trafić do Silver."""
        bledny = {**sample_row, "icao24": None}
        df = make_df(spark, [sample_row, bledny], bronze_schema)

        result = T.run_all(df)

        assert result.count() == 1
