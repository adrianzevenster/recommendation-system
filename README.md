# Simulated Real-Time Recommendation System

A local, open-source movie/TV recommendation platform that mirrors a production design

## What it includes

- **Streaming ingestion** with **Redpanda** (Kafka-compatible)
- **Raw event storage** in **MinIO**
- **Operational database** in **PostgreSQL**
- **Online/session features** in **Redis**
- **Hybrid recommender**:
  - collaborative item-item co-visitation
  - content-based similarity using TF-IDF over metadata
  - session recency boost
  - trending fallback by region
- **Serving API** with **FastAPI**
- **Observability**:
  - metrics in **Prometheus**
  - dashboards in **Grafana**
  - traces in **Jaeger**
  - structured JSON logs to stdout
- **Synthetic event generator** to simulate user behavior

## Architecture

```text
Synthetic users -> Redpanda -> Stream Processor -> Postgres + Redis + MinIO
                                      \-> Trainer -> candidate tables
Frontend/API client -> Recommendation API -> Redis + Postgres -> ranked recommendations
Prometheus scrapes services -> Grafana dashboards
OpenTelemetry traces -> Jaeger
```

## Directory structure

```text
recommendation_system_sim/
├── common/
│   ├── config.py
│   ├── db.py
│   ├── logging_utils.py
│   ├── metrics.py
│   ├── models.py
│   ├── redis_client.py
│   └── telemetry.py
├── services/
│   ├── seeder/
│   │   ├── app.py
│   │   └── Dockerfile
│   ├── event_generator/
│   │   ├── app.py
│   │   └── Dockerfile
│   ├── stream_processor/
│   │   ├── app.py
│   │   └── Dockerfile
│   ├── trainer/
│   │   ├── app.py
│   │   └── Dockerfile
│   └── recommendation_api/
│       ├── app.py
│       └── Dockerfile
├── observability/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       │   └── recommendation-overview.json
│       └── provisioning/
│           ├── dashboards/dashboard.yml
│           └── datasources/datasource.yml
├── scripts/
│   └── query_demo.sh
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Core services

### 1) Seeder
Creates the demo catalog and users in PostgreSQL.

### 2) Event generator
Publishes synthetic user behavior events:
- impression
- click
- play_start
- watch_progress
- complete
- watchlist_add

### 3) Stream processor
Consumes events and:
- writes raw JSON events to MinIO
- persists interactions in PostgreSQL
- updates Redis online features:
  - recent watched items
  - watched set
  - genre affinity
  - regional popularity

### 4) Trainer
Runs every 60 seconds and materializes:
- collaborative neighbors from item co-occurrence
- content neighbors from TF-IDF similarity
- trending titles by region
- model version records

### 5) Recommendation API
Serves `GET /recommendations/{user_id}`.

Ranking formula:

```text
0.35 collaborative
+ 0.25 content
+ 0.20 session
+ 0.10 trending
+ 0.05 freshness
+ 0.05 genre affinity
```

Guardrails:
- filters watched titles
- filters unavailable regions
- filters maturity mismatches
- simple genre diversity cap

## Running locally

### Start the full stack

```bash
docker compose up --build
```

### Call the recommendation API

```bash
curl http://localhost:8000/recommendations/u001 | python -m json.tool
```

or

```bash
./scripts/query_demo.sh
```

## Observability endpoints

- Recommendation API: `http://localhost:8000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Jaeger: `http://localhost:16686`
- MinIO console: `http://localhost:9001` (`minio` / `minio123`)

## Example API response

```json
{
  "user_id": "u001",
  "context": "home",
  "model_version": "hybrid-20260325094500",
  "recent_items": ["m001", "m007", "m010"],
  "recommendations": [
    {
      "item_id": "m006",
      "title": "Orbit Nine",
      "genres": ["sci-fi", "thriller"],
      "score": 0.8213,
      "reason": "content",
      "components": {
        "collaborative": 0.61,
        "content": 0.94,
        "session": 0.0,
        "trending": 0.40,
        "freshness": 1.0,
        "genre_bonus": 0.8
      }
    }
  ],
  "latency_ms": 12.84
}
```