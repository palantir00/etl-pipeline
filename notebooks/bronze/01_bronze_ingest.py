# Databricks notebook source

# MAGIC %md
# MAGIC # 01 · Bronze — Ingestion of Flight State Vectors
# MAGIC
# MAGIC **Layer:** Bronze (raw, unmodified data)
# MAGIC **Source:** [OpenSky Network API](https://opensky-network.org/api/states/all)
# MAGIC **Output:** Delta table `bronze.flight_states_raw`
# MAGIC
# MAGIC ---
# MAGIC ## Co robi ta warstwa?
# MAGIC
# MAGIC To **pierwszy etap** pipeline'u Medallion. Zasada Bronze jest prosta:
# MAGIC zapisz dane **dokładnie tak, jak przyszły ze źródła** — bez transformacji,
# MAGIC bez czyszczenia. Dzięki temu zawsze mamy oryginalny zapis, jeśli coś pójdzie
# MAGIC źle w kolejnych krokach.
# MAGIC
# MAGIC > 💡 **Wzorzec:** Bronze = "data lake landing zone". Traktuj go jak inbox e-mail —
# MAGIC > wrzucasz wszystko, sortujesz później.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Importy i konfiguracja

# COMMAND ----------

# Standardowe biblioteki Pythona
import requests       # wysyłanie żądań HTTP do API
import json           # parsowanie JSON-a
from datetime import datetime, timezone

# PySpark — silnik obliczeniowy Databricks
# SparkSession: punkt wejścia do całego Sparka; w Databricks jest już gotowy jako `spark`
from pyspark.sql import SparkSession

# Funkcje kolumnowe — odpowiedniki funkcji SQL, ale wywoływane z Pythona
from pyspark.sql.functions import col, current_timestamp, lit

# Typy danych — definiujemy schemat jak w CREATE TABLE SQL
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType,
)

# ---------------------------------------------------------------------------
# Konfiguracja — wszystkie "magiczne" stringi w jednym miejscu
# ---------------------------------------------------------------------------

# Ścieżka DBFS (Databricks File System) — coś jak S3 lub Azure Blob
BRONZE_PATH = "dbfs:/user/hive/warehouse/bronze"

# Nazwa tabeli w formacie <baza_danych>.<tabela>
TABLE_NAME = "bronze.flight_states_raw"

# Endpoint OpenSky — zwraca "state vectors" wszystkich śledzonych samolotów
API_URL = "https://opensky-network.org/api/states/all"

# Timeout HTTP w sekundach — po tym czasie żądanie zostanie przerwane
API_TIMEOUT_SEC = 30

print(f"Tabela docelowa : {TABLE_NAME}")
print(f"Ścieżka DBFS   : {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Pobranie surowych danych z OpenSky API
# MAGIC
# MAGIC Endpoint `/api/states/all` zwraca JSON w takiej strukturze:
# MAGIC ```json
# MAGIC {
# MAGIC   "time": 1700000000,
# MAGIC   "states": [
# MAGIC     ["a1b2c3", "UAL123  ", "United States", 1700000000, 1700000000,
# MAGIC      -87.65, 41.97, 10972.8, false, 250.1, 45.2, null, null, null, "1234", false, 0],
# MAGIC     ...
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC Każdy element `states` to lista 17 wartości **bez nazw kolumn** — nadamy je sami
# MAGIC w kroku 3, korzystając z dokumentacji API.

# COMMAND ----------

def fetch_opensky_states(url: str, timeout: int) -> dict:
    """
    Wywołuje REST API OpenSky i zwraca sparsowany JSON.

    Zwraca dict z kluczami:
      - 'time'   : int  — timestamp UNIX snapshotu (kiedy OpenSky zebrał dane)
      - 'states' : list — lista state vectorów (każdy to lista 17 wartości)
    """
    response = requests.get(url, timeout=timeout)

    # raise_for_status() rzuca wyjątek, jeśli HTTP status to 4xx lub 5xx
    # Lepsze niż ciche przetwarzanie strony błędu jak gdyby były danymi
    response.raise_for_status()

    return response.json()


# Wykonujemy właściwe żądanie HTTP — tutaj następuje faktyczne połączenie z API
raw_data = fetch_opensky_states(API_URL, API_TIMEOUT_SEC)

snapshot_time = raw_data.get("time")        # czas snapshotu jako UNIX epoch (int)
states = raw_data.get("states", [])         # lista samolotów; [] jeśli brak danych

print(f"Timestamp snapshotu : {snapshot_time}")
print(f"Liczba samolotów    : {len(states)}")
print(f"\nPrzykładowy rekord (pierwszy samolot):")
print(json.dumps(states[0], indent=2) if states else "API nie zwróciło danych")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Definicja schematu
# MAGIC
# MAGIC ### Dlaczego definiujemy schemat ręcznie zamiast pozwolić Sparkowi go wywnioskować?
# MAGIC
# MAGIC | Powód | Opis |
# MAGIC |-------|------|
# MAGIC | **Szybkość** | Wnioskowanie schematu czyta cały dataset dwukrotnie |
# MAGIC | **Poprawność** | Wartości `null` sprawiają, że Spark zgaduje typ jako `string` |
# MAGIC | **Dokumentacja** | Schemat jest samodokumentujący się |
# MAGIC | **Wczesne wykrywanie błędów** | Zmiana w API → błąd od razu, nie cicho |
# MAGIC
# MAGIC 17 pól pochodzi z [dokumentacji OpenSky State Vector](https://openskynetwork.github.io/opensky-api/rest.html).

# COMMAND ----------

# StructType = definicja tabeli (jak CREATE TABLE w SQL)
# StructField(nazwa, typ_danych, nullable)
#   nullable=True oznacza, że pole może zawierać wartość null/None

STATE_SCHEMA = StructType([
    StructField("icao24",          StringType(),  True),  # unikalny adres ICAO samolotu (hex)
    StructField("callsign",        StringType(),  True),  # znak wywoławczy, np. "LOT123  "
    StructField("origin_country",  StringType(),  True),  # kraj rejestracji
    StructField("time_position",   LongType(),    True),  # ostatnia aktualizacja pozycji (UNIX s)
    StructField("last_contact",    LongType(),    True),  # ostatni odebrany sygnał (UNIX s)
    StructField("longitude",       DoubleType(),  True),  # długość geogr. WGS-84 (stopnie)
    StructField("latitude",        DoubleType(),  True),  # szerokość geogr. WGS-84 (stopnie)
    StructField("baro_altitude",   DoubleType(),  True),  # wysokość barometryczna (metry)
    StructField("on_ground",       BooleanType(), True),  # True = samolot na ziemi
    StructField("velocity",        DoubleType(),  True),  # prędkość naziemna (m/s)
    StructField("true_track",      DoubleType(),  True),  # kurs (stopnie, 0=Północ, zgodnie ze wskazówkami)
    StructField("vertical_rate",   DoubleType(),  True),  # prędkość pionowa, + = wznoszenie (m/s)
    StructField("sensors",         StringType(),  True),  # IDs sensorów (zachowujemy jako string)
    StructField("geo_altitude",    DoubleType(),  True),  # wysokość geometryczna (metry)
    StructField("squawk",          StringType(),  True),  # kod transpondera
    StructField("spi",             BooleanType(), True),  # special purpose indicator
    StructField("position_source", LongType(),    True),  # 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
])

print(f"Zdefiniowano schemat — liczba kolumn: {len(STATE_SCHEMA.fields)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Konwersja list Pythona → PySpark DataFrame
# MAGIC
# MAGIC **SparkSession** (`spark`) to punkt wejścia do PySpark. W Databricks
# MAGIC jest tworzony automatycznie — nie musisz go inicjalizować.
# MAGIC
# MAGIC `spark.createDataFrame(data, schema)` przyjmuje:
# MAGIC - `data`   — listę krotek / list (wiersze tabeli)
# MAGIC - `schema` — StructType zdefiniowany powyżej
# MAGIC
# MAGIC Wynikiem jest **DataFrame** — rozproszona, niemutowalna, **leniwie ewaluowana** tabela.
# MAGIC "Leniwa ewaluacja" oznacza, że Spark **nie wykonuje żadnych obliczeń** aż do wywołania
# MAGIC akcji jak `.show()`, `.count()` lub `.write`. Wcześniej tylko buduje plan.

# COMMAND ----------

# Konwertujemy każdy rekord z API (lista 17 wartości) na krotkę Pythona.
# Pole `sensors` (indeks 12) może być listą intów lub None — rzutujemy na str,
# żeby uniknąć konfliktu typów przy tworzeniu DataFrame.
rows = [
    (
        s[0],                           # icao24
        s[1],                           # callsign
        s[2],                           # origin_country
        s[3],                           # time_position
        s[4],                           # last_contact
        s[5],                           # longitude
        s[6],                           # latitude
        s[7],                           # baro_altitude
        s[8],                           # on_ground
        s[9],                           # velocity
        s[10],                          # true_track
        s[11],                          # vertical_rate
        str(s[12]) if s[12] else None,  # sensors: lista → string lub None
        s[13],                          # geo_altitude
        s[14],                          # squawk
        s[15],                          # spi
        s[16],                          # position_source
    )
    for s in states
]

# Tworzymy DataFrame — to dopiero PLAN, żadne obliczenia jeszcze nie następują
df_raw = spark.createDataFrame(rows, schema=STATE_SCHEMA)

# Dodajemy kolumny metadanych — przydatne do debugowania i ładowań przyrostowych
df_bronze = (
    df_raw
    # current_timestamp() → czas, kiedy MY przeprowadziliśmy ingestion
    .withColumn("ingestion_timestamp", current_timestamp())
    # lit(wartość) → kolumna ze stałą wartością dla każdego wiersza
    .withColumn("snapshot_time", lit(snapshot_time))
    # audit trail: skąd pochodzi ten rekord
    .withColumn("source_api", lit(API_URL))
)

# printSchema() pokazuje drzewo kolumn z typami — NIE czyta danych
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Podgląd danych
# MAGIC
# MAGIC `.show(n)` to **akcja** — dopiero ona wyzwala faktyczne wykonanie planu Sparka.
# MAGIC `.count()` jest też akcją — obie zmuszają Spark do przetworzenia danych.
# MAGIC
# MAGIC > 💡 W PySpark transformacje (`.filter()`, `.withColumn()`, `.select()`) są leniwe.
# MAGIC > Akcje (`.show()`, `.count()`, `.write`) są natychmiastowe.

# COMMAND ----------

row_count = df_bronze.count()
print(f"Liczba wierszy do zapisu: {row_count}")

# truncate=False → nie przycinaj długich wartości w kolumnach
df_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Zapis do Delta Lake — tabela Bronze
# MAGIC
# MAGIC **Delta Lake** to warstwa storage'u open-source, która dodaje do plików Parquet:
# MAGIC
# MAGIC | Cecha | Co to znaczy |
# MAGIC |-------|-------------|
# MAGIC | **ACID transactions** | Zapis jest atomowy — albo wszystko, albo nic |
# MAGIC | **Schema enforcement** | Nie pozwoli zapisać danych o złym kształcie |
# MAGIC | **Time travel** | Możesz zapytać o historyczne wersje: `VERSION AS OF 3` |
# MAGIC | **Upserts (MERGE)** | Aktualizuj pasujące wiersze, wstawiaj nowe — w jednym kroku |
# MAGIC
# MAGIC ### Tryby zapisu
# MAGIC
# MAGIC | Tryb | Zachowanie |
# MAGIC |------|-----------|
# MAGIC | `overwrite` | Zastępuje wszystkie dane (dobre do pierwszego uruchomienia) |
# MAGIC | `append` | Dodaje wiersze do istniejącej tabeli |
# MAGIC | `merge` | Upsert — aktualizuje pasujące, wstawia nowe |
# MAGIC
# MAGIC W produkcji przejdziemy na `append` lub `merge` dla ładowań przyrostowych.

# COMMAND ----------

# Upewnij się, że baza danych (schema) istnieje przed zapisem tabeli
# W Databricks "database" i "schema" to ten sam koncept
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

(
    df_bronze
    .write
    .format("delta")                    # format Delta Lake (nie zwykły Parquet czy CSV)
    .mode("overwrite")                  # zastąp istniejące dane — OK dla pierwszego uruchomienia
    .option("overwriteSchema", "true")  # zastąp też schemat, jeśli się zmienił
    .saveAsTable(TABLE_NAME)            # rejestruj w Hive Metastore — można odpytywać po nazwie
)

print(f"✓ Dane zapisane do {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Weryfikacja — odpytaj tabelę, którą właśnie stworzono

# COMMAND ----------

# spark.sql() pozwala wykonać dowolne SQL na tabelach Spark / Delta
result = spark.sql(f"""
    SELECT
        icao24,
        TRIM(callsign)         AS callsign,
        origin_country,
        ROUND(longitude, 4)    AS lon,
        ROUND(latitude, 4)     AS lat,
        ROUND(baro_altitude)   AS alt_m,
        on_ground,
        ingestion_timestamp
    FROM {TABLE_NAME}
    ORDER BY origin_country
    LIMIT 10
""")

result.show(truncate=False)

# COMMAND ----------

# Szybki sanity check — liczba samolotów według kraju
spark.sql(f"""
    SELECT
        origin_country,
        COUNT(*) AS aircraft_count,
        SUM(CAST(on_ground AS INT)) AS on_ground_count
    FROM {TABLE_NAME}
    GROUP BY origin_country
    ORDER BY aircraft_count DESC
    LIMIT 15
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podsumowanie
# MAGIC
# MAGIC | Krok | Co się wydarzyło |
# MAGIC |------|-----------------|
# MAGIC | **Fetch** | Wywołano OpenSky API → otrzymano JSON snapshot wszystkich śledzonych samolotów |
# MAGIC | **Schema** | Zdefiniowano 17 nazwanych, typowanych kolumn wg dokumentacji API |
# MAGIC | **DataFrame** | Skonwertowano listy Pythona → rozproszony PySpark DataFrame |
# MAGIC | **Metadata** | Dodano kolumny `ingestion_timestamp`, `snapshot_time`, `source_api` |
# MAGIC | **Write** | Zapisano jako tabelę Delta Lake `bronze.flight_states_raw` |
# MAGIC | **Verify** | Potwierdzono liczbę wierszy i dane przez zapytanie SQL |
# MAGIC
# MAGIC **Następny krok →** `02_silver_transform.py` wczyta tę tabelę, wyczyści nulle,
# MAGIC przytnie białe znaki z `callsign`, skonwertuje timestampy i narzuci rygorystyczniejszy schemat.
