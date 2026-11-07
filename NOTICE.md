# Attribution and project notes

UPIGuard is a customized educational/portfolio adaptation inspired by the upstream project:

- **Upstream:** [iitravindra/streaming_fraud_detection](https://github.com/iitravindra/streaming_fraud_detection)
- **Original license:** MIT; the upstream `LICENSE` file is retained in this repository.

This repository reworks the project for a simulated UPI transaction use case. Changes include UPI-oriented transaction fields and synthetic producers, model-training workflow, Spark Structured Streaming inference, TimescaleDB persistence, Grafana provisioning, Docker Compose configuration, documentation, and a renamed dashboard.

The upstream project and its author retain their original attribution. This notice does not claim ownership of upstream work. The generated transaction stream and labels are synthetic and are not bank or UPI network data.
