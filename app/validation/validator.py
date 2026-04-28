from app.validation.rules import (
    check_nulls,
    check_price_type,
    check_negative_price,
    check_valid_product,
)

RULES = [
    ("null_check",     check_nulls),
    ("price_type",     check_price_type),
    ("negative_price", check_negative_price),
    ("valid_product",  check_valid_product),
]


def validate(records: list[dict]) -> dict:
   
    valid_records = []
    failures = {name: {"count": 0, "samples": []} for name, _ in RULES}

    for record in records:
        passed_all = True

        for rule_name, rule_fn in RULES:
            passed, reason = rule_fn(record)
            if not passed:
                failures[rule_name]["count"] += 1
                if len(failures[rule_name]["samples"]) < 3:
                    failures[rule_name]["samples"].append({
                        "country":      record.get("country"),
                        "product_type": record.get("product_type"),
                        "price":        record.get("price"),
                        "reason":       reason,
                    })
                passed_all = False
                break  

        if passed_all:
            valid_records.append(record)

    return {
        "total":         len(records),
        "valid_count":   len(valid_records),
        "invalid_count": len(records) - len(valid_records),
        "valid_records": valid_records,
        "failures":      failures,
    }
