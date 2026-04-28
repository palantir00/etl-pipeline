# Architecture — Medallion Pattern

## Overview

This pipeline follows the **Medallion (Multi-hop) Architecture** popularised by Databricks.
Data flows through three layers, each progressively adding quality and structure.

## Layers

### Bronze — Raw Ingestion
- **What:** Exact copy of source data, stored as-received, never mutated
- **Format:** Delta Lake (columnar Parquet + transaction log)
- **Schema:** Loose — nullable fields, no business logic applied
- **Retention:** Forever (audit trail / reprocessing safety net)
- **Write pattern:** `append` (every 10 minutes from scheduler)

### Silver — Cleansed & Typed
- **What:** Bronze data after cleaning, deduplication, and type casting
- **Format:** Delta Lake
- **Schema:** Strict — key fields NOT NULL, enums validated
- **Typical transforms:** trim strings, filter nulls, epoch → timestamp, unit conversions

### Gold — Business-Ready
- **What:** Aggregated, joined, and enriched tables ready for BI/ML
- **Format:** Delta Lake
- **Schema:** Denormalised, query-optimised
- **Typical transforms:** GROUP BY, window functions, JOINs with reference data

## Data Flow

```
OpenSky REST API  (https://opensky-network.org/api/states/all)
        │
        │  JSON  ~10 000 aircraft rows per snapshot
        ▼
┌─────────────────────────────────────────┐
│  BRONZE  bronze.flight_states_raw       │  append-only
│  · 17 raw positional fields             │  every 10 min
│  · ingestion_timestamp, snapshot_time   │
└───────────────────┬─────────────────────┘
                    │  read, parse, validate
                    ▼
┌─────────────────────────────────────────┐
│  SILVER  silver.flight_states_clean     │  merge / upsert on icao24
│  · typed schema (no nulls on key cols)  │  every 10 min
│  · callsign trimmed, altitude in feet   │
│  · snapshot_time as TimestampType       │
└──────┬──────────────────────────────────┘
       │                    │
       │  daily agg         │  rolling 1-hour window
       ▼                    ▼
┌──────────────┐   ┌────────────────────┐
│  GOLD        │   │  GOLD              │
│  flights_by_ │   │  active_routes     │
│  country     │   │  (origin→dest est) │
└──────────────┘   └────────────────────┘
```

## Scheduling

| Job                   | Trigger                         | Typical duration |
|-----------------------|---------------------------------|-----------------|
| `01_bronze_ingest`    | Every 10 min                    | ~30 s            |
| `02_silver_transform` | Every 10 min (after Bronze)     | ~60 s            |
| `03_gold_aggregate`   | Every hour                      | ~2 min           |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over plain Parquet | ACID guarantees, time travel, schema enforcement |
| Explicit schema in Bronze | Catches upstream API changes early; faster than inference |
| Append in Bronze, merge in Silver | Bronze = immutable log; Silver = current state |
| Metadata columns on every layer | Enables debugging, SLA monitoring, and reprocessing |
