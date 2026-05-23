# Payment Intelligence Platform

An end-to-end production-grade data and ML system that predicts payment failure probability and optimizes gateway routing decisions for digital payment platforms.

## Architecture

- **Ingestion**: Simulated transaction stream via Kafka
- **Pipeline**: PySpark Structured Streaming → S3 (Bronze/Silver/Gold) using Apache Iceberg
- **Orchestration**: Apache Airflow with Dataset-aware scheduling
- **ML**: LSTM sequence model for failure prediction (PyTorch)
- **RL**: Contextual Bandit → DQN for gateway routing optimization
- **Serving**: AWS Lambda + Redshift

## Project Structure

├── ingestion/          # Transaction simulator, Kafka producer
├── pipeline/           # PySpark jobs for bronze/silver/gold layers
├── models/             # DNN baseline, LSTM, RL agent
├── orchestration/      # Airflow DAGs
├── infrastructure/     # Docker configs, IaC
├── monitoring/         # Data quality, model monitoring
├── tests/              # Unit and integration tests
└── docs/               # Architecture, data models, ADRs
## Setup

Coming soon.

## Design Decisions

See [docs/adr](docs/adr) for Architecture Decision Records.
