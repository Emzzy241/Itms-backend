# ITMS Backend

## Overview

This repository contains the backend implementation for the Event-Driven Intelligent Transaction Monitoring System (ITMS). The solution is composed of microservices that ingest transactions, apply intelligence scoring, generate compliance alerts, and synchronize data to Firebase.

## Components

- `IngestionService`: HTTP API for receiving transaction payloads and publishing them to Kafka.
- `IntelligenceService`: Kafka consumer that enriches raw transactions with AI risk scoring.
- `ComplianceService`: Kafka consumer that inspects scored transactions and publishes alerts for flagged events.
- `SyncService`: Kafka consumer that writes scored transactions and alerts into Firestore.

## Architecture

- `IngestionService` publishes raw transaction events to Kafka topic `raw_transactions`.
- `IntelligenceService` consumes `raw_transactions`, scores each event, then publishes to `scored_transactions`.
- `ComplianceService` consumes `scored_transactions`, generates alerts for flagged transactions, and publishes to `alerts`.
- `SyncService` consumes both `scored_transactions` and `alerts` and syncs them to Firebase Firestore.

## Prerequisites

- .NET 8 SDK
- Docker
- Docker Compose
- Firebase service account key (optional for `SyncService`)

## Quick Start with Docker

1. Clone the repository.
2. Create Firebase key file if using sync:

```bash
copy SyncService\firebase-key.example.json SyncService\firebase-key.json
```

3. Start the full stack:

```bash
docker compose up --build
```

4. The ingestion API is available at:

```text
http://localhost:5024/api/v1/transaction/ingest
```

## Running Services Individually

From the repository root, run any service directly:

```bash
dotnet run --project IngestionService/IngestionService.csproj
```

Replace the project path for the desired service.

### Service Details

`IngestionService`
- Endpoint: `POST /api/v1/transaction/ingest`
- Publishes incoming transaction payloads to Kafka topic `raw_transactions`.

`IntelligenceService`
- Consumes `raw_transactions` from Kafka.
- Posts feature requests to the configured AI endpoint `http://localhost:8000/predict`.
- Produces enriched records to `scored_transactions`.

`ComplianceService`
- Consumes `scored_transactions` from Kafka.
- Filters flagged transactions and produces alerts to `alerts`.
- Exposes a HTTP GET endpoint at `GET /api/v1/resultformat/latest` to view the latest event list.

`SyncService`
- Consumes both `scored_transactions` and `alerts`.
- Writes documents to Firestore collections `transactions` and `alerts`.

## Configuration Notes

- Kafka bootstrap servers are currently configured in code as `localhost:9092` for the Intelligence, Compliance, and Sync services.
- Docker Compose uses the service hostname `kafka:9092`; if you run services in containers, update their Kafka bootstrap configuration or use Docker networking accordingly.
- `SyncService` sets `GOOGLE_APPLICATION_CREDENTIALS` to `firebase-key.json`.
- The AI endpoint used by `IntelligenceService` is `http://localhost:8000/predict`.

## Example Transaction Payload

```json
{
  "TransactionId": "txn-123",
  "SenderId": "USER-999",
  "Amount": 125.50,
  "TimeStamp": "2026-06-09T12:00:00Z"
}
```

## Data Flow

1. Client posts transaction to `IngestionService`.
2. Kafka stores raw transaction in `raw_transactions`.
3. `IntelligenceService` consumes and scores it.
4. Scored event is published to `scored_transactions`.
5. `ComplianceService` generates alerts for flagged records.
6. `SyncService` persists scored transactions and alerts to Firestore.

## Troubleshooting

- If Docker Compose cannot connect to Kafka, verify that Kafka is running and accessible at `localhost:9092`.
- If `SyncService` fails, confirm `SyncService/firebase-key.json` exists and Firestore credentials are valid.
- For local AI scoring, ensure the XGBoost API service is running and available at `http://localhost:8000/predict`.

## Project Structure

- `ComplianceService/`
- `IngestionService/`
- `IntelligenceService/`
- `SyncService/`
- `docker-compose.yml`
- `ITMS.sln`

## Notes

- Services are implemented using .NET 8.
- Kafka topics used by the system are `raw_transactions`, `scored_transactions`, and `alerts`.
- `SyncService` writes to Firestore collections `transactions` and `alerts`.

