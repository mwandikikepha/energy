"""
Individual validation rule functions.
Each rule takes a single record and returns (passed: bool, reason: str).
"""

REQUIRED_FIELDS = ["country", "product_type", "price", "currency", "unit", "source", "reporting_date"]


def check_nulls(record: dict) -> tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if not record.get(field):
            return False, f"missing required field: {field}"
    return True, ""


def check_price_type(record: dict) -> tuple[bool, str]:
    try:
        float(record["price"])
        return True, ""
    except (ValueError, TypeError):
        return False, f"price not numeric: {record.get('price')}"


def check_negative_price(record: dict) -> tuple[bool, str]:
    try:
        if float(record["price"]) < 0:
            return False, f"negative price: {record.get('price')}"
        return True, ""
    except (ValueError, TypeError):
        return True, ""  # already caught by check_price_type


def check_valid_product(record: dict) -> tuple[bool, str]:
    valid = {"gasoline", "diesel", "lpg", "electricity"}
    product = record.get("product_type", "")
    if product not in valid:
        return False, f"unknown product_type: {product}"
    return True, ""