# Build and verification notes

## Automated checks

- Unit tests: `python -m pytest -q` (4 tests passed during release preparation).
- Runtime checks: a full Docker Compose startup and end-to-end Kafka-to-Grafana
  run are not covered by the unit tests and have not been verified here.

## Data and artifacts

- Transaction records and labels are synthetic; they are not bank or UPI
  network data.
- The generated training CSV is retained so model training is reproducible.
- The trained model and generated metrics are reproducible build outputs and
  are excluded from Git. Run `python model_build/ml_model.py` to regenerate
  them after installing the dependencies in `model_build/requirements.txt`.
- `docs/reference/` retains the supplied synthetic dataset and upstream visual
  references for traceability. The upstream attribution and MIT license are
  preserved in `NOTICE.md` and `LICENSE`.

The provisioned dashboard definition is
`monitoring/grafana/dashboards/upiguard-risk-analytics.json`. The separate
dashboard screenshot note in `docs/images/` explains how to add a screenshot
from a running UPIGuard instance.
