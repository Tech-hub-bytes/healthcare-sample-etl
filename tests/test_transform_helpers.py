"""Unit tests for healthcare claims transform helpers (local, no Spark)."""


def claim_type_label(claim_type: str) -> str:
    if claim_type == "P":
        return "professional"
    if claim_type == "I":
        return "institutional"
    return "unknown"


def age_band(age: int | None) -> str:
    if age is None:
        return "unknown"
    if age < 18:
        return "0-17"
    if age < 35:
        return "18-34"
    if age < 50:
        return "35-49"
    if age < 65:
        return "50-64"
    if age >= 65:
        return "65+"
    return "unknown"


def test_claim_type_labels():
    assert claim_type_label("P") == "professional"
    assert claim_type_label("I") == "institutional"
    assert claim_type_label("X") == "unknown"


def test_age_bands():
    assert age_band(10) == "0-17"
    assert age_band(25) == "18-34"
    assert age_band(40) == "35-49"
    assert age_band(55) == "50-64"
    assert age_band(70) == "65+"
    assert age_band(None) == "unknown"
