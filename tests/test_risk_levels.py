from data_processor.risk import classify_probability
import pytest


def test_risk_thresholds():
    assert classify_probability(0.0) == "LOW"
    assert classify_probability(0.2999) == "LOW"
    assert classify_probability(0.30) == "MEDIUM"
    assert classify_probability(0.6999) == "MEDIUM"
    assert classify_probability(0.70) == "HIGH"
    assert classify_probability(1.0) == "HIGH"


def test_probability_out_of_range_is_rejected():
    with pytest.raises(ValueError):
        classify_probability(1.1)
