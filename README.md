# DataSurge

## Overview

DataSurge is a scalable, production-ready data pipeline for real-time financial transaction processing, anomaly detection, and business metrics aggregation. The system leverages Apache Kafka for event streaming, Apache Spark for distributed processing, and integrates with Prometheus and Grafana for monitoring and visualization.

## Architecture

- **Kafka Cluster**: Multi-broker, multi-controller setup for high availability and fault tolerance.
- **Producer (`main.py`)**: Simulates and streams financial transactions to Kafka topics.
- **Spark Processor (`jobs/spark_processor.py`)**: Consumes transactions, performs aggregations, detects anomalies, and writes results to dedicated topics.
- **Monitoring**: Prometheus scrapes application and system metrics; Grafana provides dashboards for business, Kafka, and system metrics.
- **Alerting**: Prometheus alert rules for business and system health.

## Features

- Real-time transaction ingestion and processing
- Anomaly detection for fraud and unusual activity
- Aggregated business metrics
- Scalable, containerized deployment
- Comprehensive monitoring and alerting

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Java (for Spark)
- Recommended: 8GB+ RAM

## Setup

1. **Clone the Repository**
	 ```sh
	 git clone https://github.com/nrnavaneet/datasurge.git
	 cd datasurge
	 ```

2. **Install Python Dependencies**
	 ```sh
	 pip install -r requirements.txt
	 ```

3. **Start the System**
	 ```sh
	 docker-compose up --build
	 ```

	 This will start Kafka brokers, controllers, Spark master/worker, Prometheus, and Grafana.

## Usage

### Producer

- Run `main.py` to start producing financial transactions to Kafka:
	```sh
	python main.py
	```

### Spark Processor

- The Spark job is mounted in the container and can be run as follows:
	```sh
	docker exec -it datasurge-spark-master-1 spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/bitnami/spark/jobs/spark_processor.py
	```

### Monitoring

- **Prometheus**: Accessible at `http://localhost:9090`
- **Grafana**: Accessible at `http://localhost:3000`
	- Dashboards for business metrics, Kafka metrics, and system overview are pre-configured.

## Configuration

- **Kafka**: Configured in `docker-compose.yml` and Python scripts.
- **Spark**: Check `jobs/spark_processor.py` for checkpoint and state directory settings.
- **Prometheus**: Scrape configs in `monitoring/prometheus.yml`.
- **Alert Rules**: Defined in `monitoring/alert-rules.yml`.
- **Grafana Dashboards**: JSON files in `monitoring/grafana/dashboards/`.

## Metrics & Alerts

- **Business Metrics**: Transaction rates, anomaly rates, large transaction spikes.
- **System Metrics**: CPU, memory, disk usage.
- **Kafka Metrics**: Broker status, topic message rates.
- **Alerts**: Triggered for no transactions, high anomaly rates, spikes in large transactions, and high system resource usage.

## Directory Structure

```
jobs/                 # Spark processing jobs
monitoring/           # Prometheus, Grafana, alert configs
mnt/                  # Checkpoints and state for Spark
logs/                 # Kafka and controller logs
volumes/              # Persistent data for brokers/controllers
main.py               # Kafka producer
requirements.txt      # Python dependencies
docker-compose.yml    # Container orchestration
README.md             # Project documentation
```

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please contact the repository owner.
