# Hospital Patient Vital Signs Monitoring — Kappa Architecture Pipeline

Mini-project for Applied Big Data Engineering (EC8203). Use Case 2.

## Architecture Summary

**Chosen architecture: Kappa** (see `docs/architecture_decision.md` for full justification
and the rejected Lambda alternative — this belongs in the report's 20-mark section).

```
                 ┌────────────────────┐
 Bedside          │  Kafka             │
 Monitors  ─────▶ │  topic:            │
 (producer)       │  vitals-stream     │──┐
                 └────────────────────┘  │
                                          │      ┌─────────────────────────┐      ┌────────────┐
                 ┌────────────────────┐  ├────▶ │ Spark Structured        │────▶ │ PostgreSQL │
 Pathology Lab    │  Kafka             │  │      │ Streaming               │      │ (serving   │
 (daily batch     │  topic:            │──┘      │ - clean/validate        │      │  store)    │
  producer)       │  lab-results       │          │ - windowed vitals trend │      └─────┬──────┘
                 └────────────────────┘          │ - threshold alerts      │            │
                                                  │ - stream-stream join    │            │
                                                  └─────────────────────────┘            │
                                                                                          ▼
                                          ┌───────────────────────┐            ┌──────────────────┐
                                          │ Airflow DAG (5-min)   │───────────▶│ FastAPI serving   │
                                          │ - freshness check     │            │ /ward/live        │
                                          │ - daily risk report   │            │ /reports/daily/N  │
                                          └───────────────────────┘            │ /alerts/active    │
                                                                                └──────────────────┘
```

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Apache Kafka | Decouples producers/consumers; partitioned `vitals-stream` scales per-patient throughput; retained topics enable Kappa-style replay |
| Stream processing | Spark Structured Streaming | Native Kafka source, windowing + watermarking for trend detection, `foreachBatch` for JDBC sink + alerting logic |
| Orchestration | Apache Airflow | Schedules the periodic (simulated-daily) risk-reconciliation job and pipeline freshness check over the serving store |
| Storage/Serving | PostgreSQL | Relational joins (trend + labs + alerts) needed for the risk report; simple to query from FastAPI |
| Serving API | FastAPI | Lightweight REST layer for the "live dashboard" requirement |

## Simulated Clock

1 simulated **day** = **300 seconds (5 minutes)**, configurable via `SIMULATED_DAY_SECONDS`.
The Airflow DAG's `schedule_interval` must be kept in sync with this value. See
`docs/simulated_clock.md`.

## Repository Layout

```
hospital-vitals-pipeline/
├── docker-compose.yml          # Kafka, Postgres, Spark, Airflow, API, producers
├── data-sources/               # Simulated streaming + daily-batch sources
├── processing/jobs/            # Spark Structured Streaming job
├── storage/init/               # Postgres schema (DDL)
├── serving/                    # FastAPI serving layer
├── airflow/dags/               # Daily risk report + freshness-check DAG
├── observability/
│   ├── logging/                # Shared JSON structured logging
│   └── alerts/                 # Standalone no-data / error-rate health-check daemon
├── docker/                     # Per-service Dockerfiles
├── tests/                      # Unit tests
└── docs/                       # Architecture decision, simulated clock notes
```

## Running the Pipeline

Prerequisites: Docker + Docker Compose.

```bash
docker compose up --build
```

This starts, in order: Zookeeper → Kafka (+ topic creation) → Postgres (schema auto-applied
from `storage/init/`) → vitals & lab producers → Spark streaming job → FastAPI → Airflow.

- **Live dashboard**: `http://localhost:8000/dashboard/` — patient monitor grid, active alerts feed, daily risk report table (polls the API every 5–15s, no build step required)
- Ward live dashboard data (raw JSON): `curl http://localhost:8000/ward/live`
- Active alerts: `curl http://localhost:8000/alerts/active`
- Daily risk report (day 1): `curl http://localhost:8000/reports/daily/1`
- Airflow UI: `http://localhost:8080`

To stop and wipe state:

```bash
docker compose down -v
```

## Reproducing Results for the Report

1. Let the stack run for at least 2–3 simulated days (≈15 min) so the Airflow DAG has
   produced multiple `patient_risk_report` rows.
2. Screenshot `/ward/live` and `/reports/daily/{N}` responses (or wire up a simple
   frontend / Postgres GUI) for the report's Results section.
3. Kill the `vitals-producer` container mid-run to demonstrate the no-data alert firing
   — capture this for the Observability section.

## Assumptions & Simplifications

- Fixed roster of 10 simulated patients (`NUM_PATIENTS`), shared between both producers.
- JDBC sink writes use `append` mode rather than a true upsert for windowed aggregates;
  a production system would use `MERGE` semantics.
- Risk-scoring weights in the Airflow DAG are illustrative, not clinically validated.
