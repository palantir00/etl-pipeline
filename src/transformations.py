"""
Funkcje transformacji Silver — czyste funkcje DataFrame → DataFrame.

Wyodrębnione z notebooka 02_silver_transform.py żeby można je było:
- testować lokalnie bez Databricks (pytest + lokalny PySpark)
- importować w notebooku zamiast duplikować kod
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType


def filter_required_fields(df: DataFrame) -> DataFrame:
    """Usuwa wiersze, w których brakuje kluczowych pól identyfikacyjnych."""
    return df.filter(
        F.col("icao24").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("origin_country").isNotNull()
    )


def filter_valid_coordinates(df: DataFrame) -> DataFrame:
    """Usuwa wiersze z wartościami GPS poza fizycznym zakresem Ziemi."""
    return df.filter(
        F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
    )


def clean_callsign(df: DataFrame) -> DataFrame:
    """Przycina spacje i zamienia callsign na wielkie litery."""
    return df.withColumn(
        "callsign",
        F.when(
            F.col("callsign").isNotNull(),
            F.upper(F.trim(F.col("callsign")))
        ).otherwise(None)
    )


def convert_timestamps(df: DataFrame) -> DataFrame:
    """Zamienia pola UNIX epoch (int) na kolumny TimestampType."""
    return (
        df
        .withColumn(
            "time_position_ts",
            F.from_unixtime(F.col("time_position")).cast(TimestampType())
        )
        .withColumn(
            "last_contact_ts",
            F.from_unixtime(F.col("last_contact")).cast(TimestampType())
        )
        .withColumn(
            "snapshot_ts",
            F.from_unixtime(F.col("snapshot_time")).cast(TimestampType())
        )
    )


def convert_units(df: DataFrame) -> DataFrame:
    """Przelicza jednostki: m/s → km/h, metry → stopy."""
    return (
        df
        .withColumn("velocity_kmh",      F.round(F.col("velocity") * 3.6, 1))
        .withColumn("baro_altitude_ft",  F.round(F.col("baro_altitude") * 3.28084, 0).cast(IntegerType()))
        .withColumn("geo_altitude_ft",   F.round(F.col("geo_altitude") * 3.28084, 0).cast(IntegerType()))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Usuwa zduplikowane wiersze — ten sam samolot w tym samym snapshоcie."""
    return df.dropDuplicates(["icao24", "snapshot_time"])


def run_all(df: DataFrame) -> DataFrame:
    """Uruchamia wszystkie transformacje Silver w odpowiedniej kolejności."""
    return (
        df
        .transform(filter_required_fields)
        .transform(filter_valid_coordinates)
        .transform(clean_callsign)
        .transform(convert_timestamps)
        .transform(convert_units)
        .transform(deduplicate)
    )
