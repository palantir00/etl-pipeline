# Databricks notebook source

# MAGIC %md
# MAGIC # 02 · Silver — Czyszczenie i transformacja danych
# MAGIC
# MAGIC **Wejście:**  Delta tabela `bronze.flight_states_raw`
# MAGIC **Wyjście:**  Delta tabela `silver.flight_states_clean`
# MAGIC
# MAGIC ---
# MAGIC ## Co robi ta warstwa?
# MAGIC
# MAGIC Silver to "pralnia" danych. Bierzemy surowe dane z Bronze i:
# MAGIC
# MAGIC | Problem w Bronze | Co robimy w Silver |
# MAGIC |------------------|--------------------|
# MAGIC | Brakujące wartości (null) w kluczowych polach | Usuwamy takie wiersze |
# MAGIC | `callsign` ma spacje na końcu: `"LOT123  "` | Przycinamy do `"LOT123"` |
# MAGIC | Czas jako liczba całkowita: `1700000000` | Zamieniamy na czytelny timestamp |
# MAGIC | Prędkość w m/s (mało intuicyjne) | Przeliczamy na km/h |
# MAGIC | Ten sam samolot może być dwa razy w danych | Usuwamy duplikaty |
# MAGIC | Współrzędne poza zakresem Ziemi | Filtrujemy błędne wartości |
# MAGIC
# MAGIC > 💡 **Zasada Silver:** dane są wiarygodne, poprawnie typowane, bez śmieci.
# MAGIC > Nie agregujemy — to zadanie Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Importy i konfiguracja

# COMMAND ----------

from pyspark.sql import functions as F
# Importujemy cały moduł functions jako F — zamiast pisać col(), trim(), itp.
# piszemy F.col(), F.trim() — czytelniej gdy używamy dużo funkcji naraz

from pyspark.sql.types import TimestampType, DoubleType, IntegerType

# Skąd czytamy i dokąd piszemy
SOURCE_TABLE = "bronze.flight_states_raw"
TARGET_TABLE = "silver.flight_states_clean"

# Minimalna liczba wierszy — jeśli Bronze ma mniej, coś jest nie tak z pipeline'em
MIN_EXPECTED_ROWS = 100

print(f"Źródło  : {SOURCE_TABLE}")
print(f"Cel     : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Wczytanie danych z Bronze
# MAGIC
# MAGIC `spark.table("nazwa")` to najprostszy sposób wczytania tabeli Delta do DataFrame.
# MAGIC Alternatywa: `spark.read.format("delta").load("dbfs:/ścieżka/")` — przydatna
# MAGIC gdy nie masz tabeli zarejestrowanej w metastore, tylko sam plik.
# MAGIC
# MAGIC Pamiętaj: wczytanie tabeli to nadal **leniwa operacja** — Spark czyta tylko
# MAGIC metadane (ile wierszy, jakie kolumny). Dane trafią do pamięci dopiero gdy
# MAGIC wywołasz akcję jak `.count()` lub `.show()`.

# COMMAND ----------

df_bronze = spark.table(SOURCE_TABLE)

# Szybki podgląd tego z czym zaczynamy
row_count_bronze = df_bronze.count()
print(f"Wiersze w Bronze: {row_count_bronze}")

if row_count_bronze < MIN_EXPECTED_ROWS:
    raise ValueError(
        f"Bronze ma tylko {row_count_bronze} wierszy — sprawdź czy 01_bronze_ingest.py "
        f"był uruchomiony poprawnie."
    )

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Eksploracja — co jest nie tak w surowych danych?
# MAGIC
# MAGIC Zanim cokolwiek wyczyścimy, musimy wiedzieć **ile jest problemów**.
# MAGIC To ważny krok — nie czyścisz na ślepo, tylko świadomie.

# COMMAND ----------

print("=== Liczba wartości NULL w kluczowych kolumnach ===\n")

# F.col("kolumna").isNull() zwraca True/False dla każdego wiersza
# F.sum() traktuje True jako 1, False jako 0 — w efekcie liczymy nulle
df_bronze.select(
    F.sum(F.col("icao24").isNull().cast("int")).alias("null_icao24"),
    F.sum(F.col("callsign").isNull().cast("int")).alias("null_callsign"),
    F.sum(F.col("latitude").isNull().cast("int")).alias("null_latitude"),
    F.sum(F.col("longitude").isNull().cast("int")).alias("null_longitude"),
    F.sum(F.col("baro_altitude").isNull().cast("int")).alias("null_baro_altitude"),
    F.sum(F.col("velocity").isNull().cast("int")).alias("null_velocity"),
    F.sum(F.col("origin_country").isNull().cast("int")).alias("null_origin_country"),
).show()

# COMMAND ----------

# Sprawdzamy przykłady callsign ze spacjami — typowy problem w danych lotniczych
print("=== Przykłady callsign przed przycinaniem ===")
df_bronze.select("callsign") \
    .filter(F.col("callsign").isNotNull()) \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Krok 1 — Usunięcie wierszy z brakującymi kluczowymi danymi
# MAGIC
# MAGIC `icao24` to unikalny identyfikator samolotu (jak numer rejestracyjny auta).
# MAGIC Bez niego nie możemy wiedzieć **który** to samolot — wiersz jest bezużyteczny.
# MAGIC
# MAGIC `latitude` i `longitude` to pozycja GPS. Bez nich nie możemy umieścić samolotu
# MAGIC na mapie — też bezużyteczne.
# MAGIC
# MAGIC ### Jak działa `.filter()` w PySpark?
# MAGIC
# MAGIC Wyobraź sobie tabelę jako sito. `.filter()` przepuszcza tylko te wiersze,
# MAGIC dla których warunek jest **True**.
# MAGIC
# MAGIC ```
# MAGIC df.filter(F.col("icao24").isNotNull())
# MAGIC          ↑
# MAGIC   "zostawiam tylko wiersze gdzie icao24 NIE jest null"
# MAGIC ```
# MAGIC
# MAGIC `&` to operator "I" (AND) — oba warunki muszą być spełnione jednocześnie.

# COMMAND ----------

df_no_nulls = df_bronze.filter(
    F.col("icao24").isNotNull()       # musi być identyfikator samolotu
    & F.col("latitude").isNotNull()   # musi być pozycja GPS (szerokość)
    & F.col("longitude").isNotNull()  # musi być pozycja GPS (długość)
    & F.col("origin_country").isNotNull()  # musi być kraj rejestracji
)

removed_nulls = row_count_bronze - df_no_nulls.count()
print(f"Usunięto wierszy z nullami: {removed_nulls}")
print(f"Pozostało wierszy         : {df_no_nulls.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Krok 2 — Filtrowanie błędnych współrzędnych GPS
# MAGIC
# MAGIC Ziemia ma określony zakres współrzędnych:
# MAGIC - Szerokość (latitude):  od -90° (biegun pd.) do +90° (biegun pn.)
# MAGIC - Długość (longitude): od -180° (data line) do +180°
# MAGIC
# MAGIC Jeśli API zwróci `latitude = 999.0` — to błąd w danych. Filtrujemy takie wiersze.
# MAGIC
# MAGIC `.between(a, b)` to skrót dla `col >= a AND col <= b`.

# COMMAND ----------

df_valid_coords = df_no_nulls.filter(
    F.col("latitude").between(-90.0, 90.0)
    & F.col("longitude").between(-180.0, 180.0)
)

removed_coords = df_no_nulls.count() - df_valid_coords.count()
print(f"Usunięto wierszy z błędnymi współrzędnymi: {removed_coords}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Krok 3 — Przycinanie białych znaków z callsign
# MAGIC
# MAGIC API OpenSky zwraca callsign jako pole o stałej długości 8 znaków, uzupełniane
# MAGIC spacjami: `"LOT123  "`. Dla analizy potrzebujemy `"LOT123"`.
# MAGIC
# MAGIC ### Nowe funkcje PySpark:
# MAGIC
# MAGIC - `F.trim(col)` — usuwa spacje z początku i końca stringa
# MAGIC - `F.upper(col)` — zamienia na wielkie litery
# MAGIC - `.withColumn("nazwa", wyrażenie)` — **dodaje nową kolumnę** lub **zastępuje istniejącą**
# MAGIC   jeśli podasz tę samą nazwę co istniejąca kolumna
# MAGIC
# MAGIC Użyjemy też `F.when().otherwise()` — to odpowiednik `IF` z Excela:
# MAGIC ```
# MAGIC F.when(warunek, wartość_jeśli_true).otherwise(wartość_jeśli_false)
# MAGIC ```

# COMMAND ----------

df_callsign_clean = df_valid_coords.withColumn(
    "callsign",
    # Jeśli callsign istnieje → przytnij spacje i zamień na wielkie litery
    # Jeśli callsign to null → zostaw null (otherwise(None) = zostaw null)
    F.when(
        F.col("callsign").isNotNull(),
        F.upper(F.trim(F.col("callsign")))
    ).otherwise(None)
)

# Sprawdź efekt — callsign nie powinien już mieć spacji na końcu
print("=== Callsign po przycinaniu ===")
df_callsign_clean.select("callsign").filter(F.col("callsign").isNotNull()).limit(10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Krok 4 — Konwersja czasów: liczba → czytelny timestamp
# MAGIC
# MAGIC Czas w Bronze jest zapisany jako liczba całkowita, np. `1700000000`.
# MAGIC To "UNIX timestamp" — liczba sekund, które minęły od 1 stycznia 1970 roku.
# MAGIC Komputery to lubią, ludzie nie.
# MAGIC
# MAGIC `F.from_unixtime(col)` zamienia tę liczbę na czytelny string: `"2023-11-14 22:13:20"`.
# MAGIC `.cast(TimestampType())` zamienia string na prawdziwy typ Timestamp — wtedy
# MAGIC możemy robić operacje czasowe (np. "daj mi dane z ostatniej godziny").

# COMMAND ----------

df_timestamps = df_callsign_clean \
    .withColumn(
        "time_position_ts",
        # from_unixtime zamienia sekundy UNIX na string z datą i godziną
        # cast(TimestampType()) zamienia ten string na prawdziwy typ daty/czasu
        F.from_unixtime(F.col("time_position")).cast(TimestampType())
    ) \
    .withColumn(
        "last_contact_ts",
        F.from_unixtime(F.col("last_contact")).cast(TimestampType())
    ) \
    .withColumn(
        "snapshot_ts",
        F.from_unixtime(F.col("snapshot_time")).cast(TimestampType())
    )

# Sprawdź jak wyglądają nowe kolumny z timestampem
print("=== Porównanie: liczba vs timestamp ===")
df_timestamps.select(
    "time_position", "time_position_ts",
    "last_contact",  "last_contact_ts"
).limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Krok 5 — Przeliczenie jednostek
# MAGIC
# MAGIC API zwraca prędkość w **m/s** (metry na sekundę). W lotnictwie używa się węzłów
# MAGIC (knots) albo km/h. Przeliczamy na **km/h** żeby dane były intuicyjne.
# MAGIC
# MAGIC Wzory:
# MAGIC - `km/h = m/s × 3.6`
# MAGIC - `stopy = metry × 3.28084`
# MAGIC
# MAGIC `F.round(col, n)` zaokrągla do `n` miejsc po przecinku.

# COMMAND ----------

df_units = df_timestamps \
    .withColumn(
        "velocity_kmh",
        # Mnożenie kolumny przez stałą — PySpark obsługuje standardowe operatory: * / + -
        F.round(F.col("velocity") * 3.6, 1)
    ) \
    .withColumn(
        "baro_altitude_ft",
        F.round(F.col("baro_altitude") * 3.28084, 0).cast(IntegerType())
    ) \
    .withColumn(
        "geo_altitude_ft",
        F.round(F.col("geo_altitude") * 3.28084, 0).cast(IntegerType())
    )

print("=== Prędkość i wysokość przed i po przeliczeniu ===")
df_units.select(
    "velocity", "velocity_kmh",
    "baro_altitude", "baro_altitude_ft"
).filter(F.col("velocity").isNotNull()).limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Krok 6 — Usunięcie duplikatów
# MAGIC
# MAGIC Ten sam samolot (`icao24`) może pojawić się dwukrotnie w jednym snapshоcie
# MAGIC (np. gdy dwa sensory go odebrały w tej samej chwili). Duplikaty zaburzają
# MAGIC liczniki — np. "ile samolotów lata teraz" byłoby zawyżone.
# MAGIC
# MAGIC `dropDuplicates(["kolumna1", "kolumna2"])` zatrzymuje tylko jeden wiersz
# MAGIC na unikalną kombinację podanych kolumn. Resztę odrzuca.
# MAGIC
# MAGIC Wybieramy kombinację `icao24 + snapshot_time` bo:
# MAGIC - `icao24` = który samolot
# MAGIC - `snapshot_time` = o której godzinie (jeden snapshot = jedna chwila w czasie)

# COMMAND ----------

df_deduped = df_units.dropDuplicates(["icao24", "snapshot_time"])

removed_dupes = df_units.count() - df_deduped.count()
print(f"Usunięto duplikatów: {removed_dupes}")
print(f"Wierszy po dedupl.  : {df_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Wybór finalnych kolumn
# MAGIC
# MAGIC Teraz wybieramy które kolumny trafiają do Silver. Usuwamy stare kolumny
# MAGIC z surową liczbą (np. `time_position`) bo zastąpiliśmy je czytelniejszymi
# MAGIC wersjami (`time_position_ts`). Usuwamy też `source_api` — w Silver już
# MAGIC wiemy skąd są dane.
# MAGIC
# MAGIC `.select("kol1", "kol2", ...)` działa jak `SELECT` w SQL — wybierasz tylko
# MAGIC te kolumny, które chcesz zatrzymać.

# COMMAND ----------

df_silver = df_deduped.select(
    # --- Identyfikacja samolotu ---
    "icao24",
    "callsign",
    "origin_country",

    # --- Pozycja ---
    "latitude",
    "longitude",
    "baro_altitude",        # oryginał w metrach — zostawiamy dla precyzji
    "baro_altitude_ft",     # przeliczony na stopy — dla intuicyjności
    "geo_altitude",
    "geo_altitude_ft",
    "on_ground",

    # --- Ruch ---
    "velocity",             # oryginał w m/s
    "velocity_kmh",         # przeliczony na km/h
    "true_track",
    "vertical_rate",

    # --- Transponder ---
    "squawk",
    "spi",
    "position_source",

    # --- Czas (czytelne timestampy zamiast liczb UNIX) ---
    "time_position_ts",
    "last_contact_ts",
    "snapshot_ts",

    # --- Metadata pipeline'u ---
    "ingestion_timestamp",
)

print(f"Finalny schemat Silver — liczba kolumn: {len(df_silver.columns)}")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Podsumowanie czyszczenia — raport jakości

# COMMAND ----------

row_count_silver = df_silver.count()

print("=" * 50)
print("RAPORT JAKOŚCI DANYCH")
print("=" * 50)
print(f"Bronze (wejście)          : {row_count_bronze:>8} wierszy")
print(f"Po usunięciu nullów        : {row_count_bronze - removed_nulls:>8} wierszy  (-{removed_nulls})")
print(f"Po filtrze współrzędnych   : {row_count_bronze - removed_nulls - removed_coords:>8} wierszy  (-{removed_coords})")
print(f"Po usunięciu duplikatów    : {row_count_silver + removed_dupes:>8} wierszy  (przed dedupl.)")
print(f"Silver (wyjście)           : {row_count_silver:>8} wierszy  (-{removed_dupes} duplikatów)")
print("-" * 50)
total_removed = row_count_bronze - row_count_silver
pct_kept = (row_count_silver / row_count_bronze * 100) if row_count_bronze > 0 else 0
print(f"Łącznie odfiltrowano       : {total_removed:>8} wierszy")
print(f"Zachowano                  : {pct_kept:.1f}% danych")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Zapis do Silver — tabela Delta
# MAGIC
# MAGIC Używamy tego samego wzorca co w Bronze: `format("delta").saveAsTable()`.
# MAGIC
# MAGIC W przyszłości, gdy Bronze będzie dostawać nowe dane co 10 minut, zamienimy
# MAGIC `overwrite` na **merge** (upsert) — wtedy Silver będzie aktualizować istniejące
# MAGIC wiersze i dodawać tylko nowe. Na razie `overwrite` jest najprostszy do nauki.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ Dane zapisane do {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · Weryfikacja końcowa

# COMMAND ----------

# Sprawdzamy zawartość tabeli Silver SQL-em
spark.sql(f"""
    SELECT
        icao24,
        callsign,
        origin_country,
        ROUND(latitude, 4)   AS lat,
        ROUND(longitude, 4)  AS lon,
        baro_altitude_ft     AS alt_ft,
        velocity_kmh,
        on_ground,
        snapshot_ts
    FROM {TARGET_TABLE}
    ORDER BY velocity_kmh DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Top 10 krajów wg liczby aktywnych samolotów (w powietrzu)
print("=== Top 10 krajów — samoloty w powietrzu ===")
spark.sql(f"""
    SELECT
        origin_country,
        COUNT(*)                        AS total_aircraft,
        SUM(CASE WHEN on_ground = false
                 THEN 1 ELSE 0 END)     AS in_air,
        ROUND(AVG(velocity_kmh), 1)     AS avg_speed_kmh,
        ROUND(AVG(baro_altitude_ft), 0) AS avg_altitude_ft
    FROM {TARGET_TABLE}
    WHERE on_ground = false
    GROUP BY origin_country
    ORDER BY in_air DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podsumowanie
# MAGIC
# MAGIC | Krok | Co zrobiliśmy | Nowe pojęcie PySpark |
# MAGIC |------|--------------|---------------------|
# MAGIC | 1 | Wczytanie Bronze | `spark.table()` |
# MAGIC | 2 | Eksploracja nullów | `F.sum()`, `isNull()` |
# MAGIC | 3 | Usunięcie nullów w kluczowych polach | `.filter()`, `isNotNull()`, `&` |
# MAGIC | 4 | Filtrowanie błędnych GPS | `.between()` |
# MAGIC | 5 | Przycinanie callsign | `F.trim()`, `F.upper()`, `F.when().otherwise()` |
# MAGIC | 6 | Konwersja czasu | `F.from_unixtime()`, `.cast(TimestampType())` |
# MAGIC | 7 | Przeliczenie jednostek | Operatory `*`, `F.round()` |
# MAGIC | 8 | Dedupl. | `dropDuplicates()` |
# MAGIC | 9 | Wybór kolumn | `.select()` |
# MAGIC | 10 | Zapis | `.write.format("delta").saveAsTable()` |
# MAGIC
# MAGIC **Następny krok →** `03_gold_aggregate.py` weźmie tę czystą tabelę i stworzy
# MAGIC gotowe do raportowania agregacje: ile samolotów per kraj, najszybsze trasy, itd.
