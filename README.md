# Data Platform Lakehouse Project

A modern, maintainable, monitored, and scalable data platform built with best-of-breed open-source tools. This system provides storage, organization, analysis, and enrichment of structured and unstructured data (tables, text, images) in a lakehouse architecture using Apache Iceberg on PostgreSQL catalog and MinIO for object storage.

## Overview

This repository represents a complete end-to-end data platform designed for both development and production-like deployment using Docker Swarm. Key features include:

- **FastAPI** backend with typed, production-ready Python (3.12)
- **Apache Iceberg** lakehouse tables (via PyIceberg) for versioned, ACID-compliant data
- **PostgreSQL** as metadata catalog and relational store
- **MinIO** as S3-compatible object storage
- **DuckDB** for ad-hoc SQL analytics and business rules
- **Polars** for high-performance DataFrame operations
- **Ollama** + Llama3 for local LLM-powered data querying and analysis
- Multimodal enrichment: NLP (spaCy), OCR (EasyOCR) on images stored in Iceberg
- Hierarchical data lake summary (Iceberg namespaces/tables + MinIO buckets/objects)
- Comprehensive monitoring stack: Prometheus, Grafana, cAdvisor, Alertmanager, Uptime Kuma
- Performance metrics logged to PostgreSQL with historical tracking
- Antifragile monitor service that learns baselines and auto-scales/restarts services
- Authentication (JWT), health checks, structured logging
- Error resilience via `@error_handler` decorator with retries and alerting
- Escalation alerts to Slack/Discord

## Architecture

```
FastAPI ↔ PostgreSQL (catalog + users/metrics)
        ↔ MinIO (data lake storage)
        ↔ Ollama (LLM enrichment)
        ↔ Iceberg Tables (versioned data with snapshots)

Monitoring:
  Prometheus → Grafana
  cAdvisor → Container metrics
  Alertmanager → Slack/Discord alerts
  Antifragile Monitor → Auto-scale & heal via Docker API
```

## Getting Started

### Prerequisites
- Docker & Docker Swarm (`docker swarm init`)
- Python 3.12 (for local dev with Metaflow, not required in container)

### Deployment (Docker Swarm)

```bash
# Clone repo
git clone https://github.com/yourusername/data-platform-lakehouse.git
cd data-platform-lakehouse

# Set your webhook in .env
echo "ALERT_WEBHOOK=https://your-slack-or-discord-webhook" >> .env

# Deploy main stack
docker stack deploy -c docker-compose.yml data_platform

# Deploy monitoring stack
docker stack deploy -c docker-compose-monitoring.yml monitoring

# (Optional) Deploy Kafka
docker stack deploy -c docker-compose-kafka.yml kafka_stack
```

Services will be available at:
- API: `http://localhost:8000`
- MinIO Console: `http://localhost:9001`
- Grafana: `http://localhost:3000`
- Uptime Kuma: `http://localhost:3001`
- pgAdmin: `http://localhost:8080`

### Local Development
- Use Metaflow for workflow orchestration (install locally)
- Run services via `docker-compose` (non-swarm) if preferred

## Personal Note

This project is deeply personal to me. It represents the culmination of nearly 10 years of hobbyist exploration in data engineering, systems design, and programming language curiosity.

Over the years, I’ve rebuilt versions of this platform many times — sometimes in Python, sometimes experimenting with different storage engines, query layers, or orchestration tools. Each iteration was a learning journey: understanding trade-offs, performance characteristics, and how real-world systems behave under load or failure.

Like many of us, I get excited when a new technology appears. I’ll dive into Rust or Haskell each December to solve [Advent of Code](https://adventofcode.com/) puzzles — not because I need to, but because the elegance and rigor recharge my thinking. When something like LanceDB, DuckDB, or Iceberg gains traction, I tinker, break things, and try to deeply understand why it matters.

This version feels different. It’s more complete, more resilient, and more intentional than past attempts. I’m currently in between professional adventures, and it felt like the right moment to formalize years of private experimentation into a coherent, organized public repository.

This isn’t just code — it’s a reflection of curiosity sustained over time, of learning through building (and rebuilding), and of finding joy in systems that are robust, observable, and a little bit antifragile.

I hope it’s useful to someone else on their own journey.

— January 2026