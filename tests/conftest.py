"""
conftest.py — współdzielone fixtures dla całego katalogu tests/.

pytest automatycznie wczytuje ten plik przed każdym testem.
Fixtures zdefiniowane tutaj są dostępne we wszystkich plikach testowych
bez importowania — wystarczy użyć ich nazwy jako argumentu funkcji testowej.
"""

import pytest
from pyspark.sql import SparkSession


# scope="session" oznacza: utwórz SparkSession RAZ dla całej sesji testowej,
# nie twórz nowej dla każdego testu — uruchamianie Sparka trwa kilka sekund.
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")           # lokalny tryb, 1 wątek — wystarczy do testów
        .appName("etl-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")      # domyślnie 200 — za dużo dla testów
        .config("spark.ui.showConsoleProgress", "false")  # wycisz logi postępu
        .config("spark.ui.enabled", "false")              # wyłącz Spark UI (port 4040)
        .getOrCreate()
    )
    yield session          # yield zamiast return — kod po yield to "teardown"
    session.stop()         # zatrzymaj Spark po zakończeniu WSZYSTKICH testów


@pytest.fixture
def sample_row():
    """Jeden poprawny wiersz w kształcie tabeli Bronze — używany w wielu testach."""
    return {
        "icao24":          "a1b2c3",
        "callsign":        "LOT123  ",   # celowo ze spacjami — test przycinania
        "origin_country":  "Poland",
        "time_position":   1700000000,
        "last_contact":    1700000000,
        "longitude":       21.01,
        "latitude":        52.22,
        "baro_altitude":   10000.0,      # metry
        "on_ground":       False,
        "velocity":        100.0,        # m/s → powinno dać 360.0 km/h
        "true_track":      88.0,
        "vertical_rate":   0.5,
        "sensors":         None,
        "geo_altitude":    10100.0,
        "squawk":          "7700",
        "spi":             False,
        "position_source": 0,
        "ingestion_timestamp": None,
        "snapshot_time":   1700000000,
        "source_api":      "https://opensky-network.org/api/states/all",
    }


@pytest.fixture
def bronze_schema():
    """Schemat tabeli Bronze — identyczny jak w notebooku 01."""
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, BooleanType, LongType,
    )
    return StructType([
        StructField("icao24",              StringType(),  True),
        StructField("callsign",            StringType(),  True),
        StructField("origin_country",      StringType(),  True),
        StructField("time_position",       LongType(),    True),
        StructField("last_contact",        LongType(),    True),
        StructField("longitude",           DoubleType(),  True),
        StructField("latitude",            DoubleType(),  True),
        StructField("baro_altitude",       DoubleType(),  True),
        StructField("on_ground",           BooleanType(), True),
        StructField("velocity",            DoubleType(),  True),
        StructField("true_track",          DoubleType(),  True),
        StructField("vertical_rate",       DoubleType(),  True),
        StructField("sensors",             StringType(),  True),
        StructField("geo_altitude",        DoubleType(),  True),
        StructField("squawk",              StringType(),  True),
        StructField("spi",                 BooleanType(), True),
        StructField("position_source",     LongType(),    True),
        StructField("ingestion_timestamp", StringType(),  True),
        StructField("snapshot_time",       LongType(),    True),
        StructField("source_api",          StringType(),  True),
    ])
