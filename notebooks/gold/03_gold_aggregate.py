# Databricks notebook source

# MAGIC %md
# MAGIC # 03 · Gold — Agregacje gotowe do raportowania
# MAGIC
# MAGIC **Wejście:**  Delta tabela `silver.flight_states_clean`
# MAGIC **Wyjście:**  Trzy tabele Gold:
# MAGIC
# MAGIC | Tabela | Co zawiera |
# MAGIC |--------|-----------|
# MAGIC | `gold.flights_by_country` | Ile samolotów per kraj, średnia prędkość i wysokość |
# MAGIC | `gold.altitude_bands` | Rozkład samolotów wg pasa wysokości (ziemia / niski / średni / wysoki) |
# MAGIC | `gold.speed_leaders` | 20 najszybszych samolotów w tym snapshоcie |
# MAGIC
# MAGIC ---
# MAGIC ## Co robi ta warstwa?
# MAGIC
# MAGIC Gold to "gotowy raport dla szefa". Dane są już czyste (Silver to zrobiło),
# MAGIC więc teraz **grupujemy, liczymy i rankingujemy** — żeby odpowiedzieć na
# MAGIC konkretne pytania biznesowe.
# MAGIC
# MAGIC Nowe pojęcia PySpark w tym notebooku:
# MAGIC - `groupBy().agg()` — grupowanie + agregacja (jak pivot table w Excelu)
# MAGIC - `F.count()`, `F.avg()`, `F.max()`, `F.sum()` — funkcje agregujące
# MAGIC - `F.when().when().otherwise()` — wiele warunków IF (kategoryzacja)
# MAGIC - Window functions — ranking wewnątrz grup (jak `RANK()` w SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Importy i konfiguracja

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# Window to specjalny obiekt do funkcji okienkowych — wyjaśniamy go w kroku 5
# Na razie traktuj go jak "definicja grupy do rankingowania"

SOURCE_TABLE = "silver.flight_states_clean"

# Trzy tabele wyjściowe — każda odpowiada na inne pytanie biznesowe
TABLE_BY_COUNTRY   = "gold.flights_by_country"
TABLE_ALT_BANDS    = "gold.altitude_bands"
TABLE_SPEED_LEADERS = "gold.speed_leaders"

print(f"Źródło : {SOURCE_TABLE}")
print(f"Cele   : {TABLE_BY_COUNTRY}, {TABLE_ALT_BANDS}, {TABLE_SPEED_LEADERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Wczytanie danych z Silver

# COMMAND ----------

df_silver = spark.table(SOURCE_TABLE)

row_count = df_silver.count()
print(f"Wierszy w Silver: {row_count}")

# Szybki podgląd — zawsze warto zobaczyć co mamy zanim zaczniesz agregować
df_silver.select(
    "icao24", "callsign", "origin_country",
    "baro_altitude_ft", "velocity_kmh", "on_ground"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Tabela 1 — Loty według kraju (`gold.flights_by_country`)
# MAGIC
# MAGIC **Pytanie biznesowe:** Które kraje mają najwięcej aktywnych samolotów?
# MAGIC Jaka jest ich średnia prędkość i wysokość?
# MAGIC
# MAGIC ---
# MAGIC ### Jak działa `groupBy().agg()`?
# MAGIC
# MAGIC Wyobraź sobie listę zakupów z całego miesiąca. Chcesz wiedzieć ile wydałaś
# MAGIC per kategoria (jedzenie, transport, rozrywka). Robisz pivot table w Excelu:
# MAGIC zaznaczasz "grupuj po kategorii" i "zsumuj kwoty". To dokładnie to samo.
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy("kategoria").agg(F.sum("kwota"))
# MAGIC    ↑                         ↑
# MAGIC  "grupuj po tym polu"    "i zastosuj tę funkcję na pozostałych"
# MAGIC ```
# MAGIC
# MAGIC Funkcje agregujące (działają na grupie wierszy → zwracają jedną wartość):
# MAGIC - `F.count("*")` — ile wierszy w grupie
# MAGIC - `F.avg("kolumna")` — średnia
# MAGIC - `F.max("kolumna")` — maksimum
# MAGIC - `F.min("kolumna")` — minimum
# MAGIC - `F.sum("kolumna")` — suma
# MAGIC
# MAGIC `.alias("nowa_nazwa")` nadaje kolumnie czytelną nazwę — jak "przemianowanie"
# MAGIC kolumny w Excelu.

# COMMAND ----------

df_by_country = (
    df_silver
    .groupBy("origin_country")
    .agg(
        # Łączna liczba samolotów (w powietrzu i na ziemi)
        F.count("*").alias("total_aircraft"),

        # Samoloty w powietrzu: on_ground=False → rzutujemy Boolean na int (0/1) i sumujemy
        F.sum(
            F.when(F.col("on_ground") == False, 1).otherwise(0)
        ).alias("in_air"),

        # Samoloty na ziemi
        F.sum(
            F.when(F.col("on_ground") == True, 1).otherwise(0)
        ).alias("on_ground_count"),

        # Średnia prędkość — tylko dla samolotów w powietrzu (na ziemi prędkość = 0)
        F.round(
            F.avg(F.when(F.col("on_ground") == False, F.col("velocity_kmh"))),
            1
        ).alias("avg_speed_kmh"),

        # Średnia wysokość — tylko dla samolotów w powietrzu
        F.round(
            F.avg(F.when(F.col("on_ground") == False, F.col("baro_altitude_ft"))),
            0
        ).alias("avg_altitude_ft"),

        # Najszybszy samolot z tego kraju
        F.round(F.max("velocity_kmh"), 1).alias("max_speed_kmh"),

        # Czas snapshotu — kiedy te dane były aktualne
        F.max("snapshot_ts").alias("snapshot_ts"),
    )
    # Sortujemy malejąco po liczbie samolotów w powietrzu
    .orderBy(F.col("in_air").desc())
)

print("=== Loty według kraju (Top 15) ===")
df_by_country.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Tabela 2 — Rozkład wg pasa wysokości (`gold.altitude_bands`)
# MAGIC
# MAGIC **Pytanie biznesowe:** Jak wygląda rozkład ruchu lotniczego wg wysokości?
# MAGIC Ile samolotów leci nisko (podejście/odlot), a ile na przelotowej?
# MAGIC
# MAGIC ---
# MAGIC ### `F.when().when().otherwise()` — wiele warunków IF
# MAGIC
# MAGIC W Silver używaliśmy `F.when(warunek, wartość).otherwise(inne)` — to jeden IF.
# MAGIC Teraz robimy **łańcuch warunków** — jak `JEŻELI` zagnieżdżone w Excelu,
# MAGIC ale czytelniejsze:
# MAGIC
# MAGIC ```python
# MAGIC F.when(warunek_1, "A")   # jeśli 1 → A
# MAGIC  .when(warunek_2, "B")   # w przeciwnym razie, jeśli 2 → B
# MAGIC  .when(warunek_3, "C")   # w przeciwnym razie, jeśli 3 → C
# MAGIC  .otherwise("D")         # w każdym innym przypadku → D
# MAGIC ```
# MAGIC
# MAGIC Spark sprawdza warunki **po kolei** i zatrzymuje się przy pierwszym True.
# MAGIC
# MAGIC ---
# MAGIC ### Pasy wysokości w lotnictwie (stopy):
# MAGIC
# MAGIC | Pas | Zakres | Typowe loty |
# MAGIC |-----|--------|-------------|
# MAGIC | Ziemia | 0 (on_ground) | kołowanie, parking |
# MAGIC | Niski | < 10 000 ft | odloty, podejścia, małe samoloty |
# MAGIC | Średni | 10 000 – 25 000 ft | przejściowy, helikoptery, turbopropy |
# MAGIC | Wysoki | 25 000 – 40 000 ft | typowy przelot rejsowy |
# MAGIC | Bardzo wysoki | > 40 000 ft | wojskowe, Concorde-style |

# COMMAND ----------

# Najpierw kategoryzujemy każdy wiersz — dodajemy kolumnę "altitude_band"
df_with_band = df_silver.withColumn(
    "altitude_band",
    F.when(F.col("on_ground") == True,  "1_Ziemia")
     .when(F.col("baro_altitude_ft") < 10_000,              "2_Niski (<10k ft)")
     .when(F.col("baro_altitude_ft") < 25_000,              "3_Średni (10-25k ft)")
     .when(F.col("baro_altitude_ft") < 40_000,              "4_Wysoki (25-40k ft)")
     .when(F.col("baro_altitude_ft").isNotNull(),            "5_Bardzo wysoki (>40k ft)")
     .otherwise("Nieznany")  # baro_altitude_ft jest null i samolot nie jest na ziemi
)

# Teraz grupujemy po kategorii i liczymy
df_alt_bands = (
    df_with_band
    .groupBy("altitude_band")
    .agg(
        F.count("*").alias("aircraft_count"),
        F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
        F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
        F.max("snapshot_ts").alias("snapshot_ts"),
    )
    # Sortujemy po nazwie pasa (zaczyna się od cyfry → alphabetycznie = chronologicznie)
    .orderBy("altitude_band")
)

print("=== Rozkład wg pasa wysokości ===")
df_alt_bands.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Tabela 3 — Ranking najszybszych samolotów (`gold.speed_leaders`)
# MAGIC
# MAGIC **Pytanie biznesowe:** Które samoloty lecą teraz najszybciej?
# MAGIC Jak szybki jest #1 w każdym kraju?
# MAGIC
# MAGIC ---
# MAGIC ### Window functions — ranking wewnątrz grup
# MAGIC
# MAGIC To jeden z najpotężniejszych mechanizmów w PySpark/SQL. Pozwala robić
# MAGIC operacje "per grupa" **bez zmniejszania liczby wierszy** (w odróżnieniu od
# MAGIC `groupBy` które zwraca jeden wiersz na grupę).
# MAGIC
# MAGIC **Analogia:** Masz listę uczniów z wynikami testów z każdej klasy.
# MAGIC Chcesz wiedzieć który uczeń jest #1, #2, #3 **w swojej klasie** —
# MAGIC ale zachować wszystkich uczniów w tabeli. Window function to robi.
# MAGIC
# MAGIC ```python
# MAGIC # Krok 1: zdefiniuj "okno" — jak pogrupować i jak posortować wewnątrz grupy
# MAGIC window_spec = Window.partitionBy("klasa").orderBy(F.col("wynik").desc())
# MAGIC
# MAGIC # Krok 2: zastosuj funkcję rankingującą
# MAGIC df.withColumn("rank", F.rank().over(window_spec))
# MAGIC ```
# MAGIC
# MAGIC - `partitionBy` = "podziel na grupy po tym polu" (jak GROUP BY)
# MAGIC - `orderBy` = "posortuj wewnątrz każdej grupy po tym polu"
# MAGIC - `F.rank()` = nadaj numer porządkowy (1, 2, 3...) wewnątrz każdej grupy
# MAGIC
# MAGIC Różnica między `rank()` a `dense_rank()`:
# MAGIC - `rank()`:       1, 2, 2, **4** (pomija 3 gdy jest remis)
# MAGIC - `dense_rank()`: 1, 2, 2, **3** (nie pomija — "gęsty ranking")

# COMMAND ----------

# Definiujemy okno: rankinguj w ramach każdego kraju po prędkości malejąco
window_by_country = (
    Window
    .partitionBy("origin_country")   # osobny ranking dla każdego kraju
    .orderBy(F.col("velocity_kmh").desc())  # najszybszy = #1
)

# Dodajemy kolumnę z rankingiem wewnątrz kraju
df_ranked = df_silver \
    .filter(F.col("on_ground") == False) \
    .filter(F.col("velocity_kmh").isNotNull()) \
    .withColumn("rank_in_country", F.dense_rank().over(window_by_country))

# Globalne Top 20 najszybszych samolotów (bez ograniczenia do krajów)
window_global = Window.orderBy(F.col("velocity_kmh").desc())

df_speed_leaders = (
    df_ranked
    .withColumn("global_rank", F.dense_rank().over(window_global))
    .filter(F.col("global_rank") <= 20)   # zachowaj tylko Top 20 globalnie
    .select(
        "global_rank",
        "rank_in_country",
        "callsign",
        "origin_country",
        "velocity_kmh",
        "baro_altitude_ft",
        F.round("true_track", 0).alias("heading_deg"),  # kurs w stopniach
        "vertical_rate",                                  # + wznosi się, - opada
        "snapshot_ts",
    )
    .orderBy("global_rank")
)

print("=== Top 20 najszybszych samolotów ===")
df_speed_leaders.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Zapis wszystkich trzech tabel Gold

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Zapisujemy każdą tabelę osobno — ten sam wzorzec co w Bronze i Silver
for df, table_name in [
    (df_by_country,    TABLE_BY_COUNTRY),
    (df_alt_bands,     TABLE_ALT_BANDS),
    (df_speed_leaders, TABLE_SPEED_LEADERS),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"✓ Zapisano: {table_name}  ({df.count()} wierszy)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Weryfikacja — finalne zapytania SQL

# COMMAND ----------

print("=== gold.flights_by_country — Top 10 ===")
spark.sql(f"""
    SELECT
        origin_country,
        total_aircraft,
        in_air,
        on_ground_count,
        avg_speed_kmh,
        avg_altitude_ft,
        max_speed_kmh
    FROM {TABLE_BY_COUNTRY}
    ORDER BY in_air DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

print("=== gold.altitude_bands — pełna tabela ===")
spark.sql(f"""
    SELECT
        altitude_band,
        aircraft_count,
        ROUND(aircraft_count * 100.0 / SUM(aircraft_count) OVER (), 1) AS pct_of_total,
        avg_speed_kmh,
        avg_altitude_ft
    FROM {TABLE_ALT_BANDS}
    ORDER BY altitude_band
""").show(truncate=False)

# COMMAND ----------

print("=== gold.speed_leaders — Top 20 ===")
spark.sql(f"""
    SELECT
        global_rank,
        callsign,
        origin_country,
        velocity_kmh,
        baro_altitude_ft,
        heading_deg,
        vertical_rate
    FROM {TABLE_SPEED_LEADERS}
    ORDER BY global_rank
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podsumowanie całego pipeline'u
# MAGIC
# MAGIC ```
# MAGIC OpenSky API
# MAGIC     │  ~10 000 wierszy, surowy JSON
# MAGIC     ▼
# MAGIC [Bronze]  flight_states_raw      ← dane dokładnie z API, nic nie zmienione
# MAGIC     │
# MAGIC     │  filtrowanie nullów, GPS, dedup, przeliczenie jednostek
# MAGIC     ▼
# MAGIC [Silver]  flight_states_clean    ← dane czyste, poprawnie typowane
# MAGIC     │
# MAGIC     │  groupBy, ranking, kategoryzacja
# MAGIC     ▼
# MAGIC [Gold]    flights_by_country     ← ile samolotów per kraj
# MAGIC           altitude_bands         ← rozkład wg wysokości
# MAGIC           speed_leaders          ← Top 20 najszybszych
# MAGIC ```
# MAGIC
# MAGIC ### Nowe pojęcia PySpark z tego notebooka
# MAGIC
# MAGIC | Pojęcie | Do czego służy |
# MAGIC |---------|---------------|
# MAGIC | `groupBy().agg()` | Grupowanie + agregacja (jak pivot table) |
# MAGIC | `F.count()`, `F.avg()`, `F.max()`, `F.sum()` | Funkcje agregujące |
# MAGIC | `F.when().when().otherwise()` | Łańcuch warunków IF |
# MAGIC | `Window.partitionBy().orderBy()` | Definicja grupy do rankingowania |
# MAGIC | `F.rank()`, `F.dense_rank()` | Numer pozycji w rankingu |
# MAGIC | `.over(window_spec)` | Zastosowanie funkcji okienkowej |
# MAGIC | Zapis w pętli `for` | Zapisanie wielu tabel tym samym kodem |
