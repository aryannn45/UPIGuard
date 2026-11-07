"""Risk band helpers shared by streaming logic and tests."""

def classify_probability(probability: float) -> str:
    """Map a model fraud probability to the configured demo risk band."""
    value = float(probability)
    if not 0.0 <= value <= 1.0:
        raise ValueError("probability must be between 0 and 1")
    if value >= 0.70:
        return "HIGH"
    if value >= 0.30:
        return "MEDIUM"
    return "LOW"
