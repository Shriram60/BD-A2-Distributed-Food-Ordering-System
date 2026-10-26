"""Task A2: inspect raw_orders.jsonl before writing Spark Job 1 (throwaway)."""
import json
import re
import sys
from collections import Counter
from pathlib import Path

FIELDS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
          "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
          "expected_delivery_timestamp", "delivered_timestamp",
          "distance_km", "order_amount", "delivery_fee", "status"]
TS_FIELDS = [f for f in FIELDS if f.endswith("_timestamp")]
NUM_FIELDS = ["distance_km", "order_amount", "delivery_fee"]


def shape(v):
    """Turn a value into a pattern: digits -> 9, letters -> a."""
    if not isinstance(v, str):
        return f"<{type(v).__name__}>"
    return re.sub(r"[A-Za-z]", "a", re.sub(r"\d", "9", v))


def invalid(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def to_num(v):
    if isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main(path):
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    print(f"file: {path}\ntotal lines: {len(lines)}")

    bad_json, not_object, blank = [], [], 0
    missing = Counter()
    invalid_field = Counter()
    ts_shapes = Counter()
    num_types = Counter()
    status = Counter()
    nonpos = Counter()
    nonnum = Counter()
    extra_keys = Counter()
    row_issues = Counter()   # first-failing reason per row, in A3 order

    seen_lines = Counter(lines)
    for n, line in enumerate(lines, 1):
        if not line.strip():
            blank += 1
            row_issues["blank line"] += 1
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            bad_json.append((n, str(e), line[:120]))
            row_issues["malformed json"] += 1
            continue
        if not isinstance(rec, dict):
            not_object.append((n, line[:120]))
            row_issues["json not an object"] += 1
            continue

        for k in rec:
            if k not in FIELDS:
                extra_keys[k] += 1
        for f in FIELDS:
            if f not in rec:
                missing[f] += 1
            elif invalid(rec[f]):
                invalid_field[f] += 1
        for f in TS_FIELDS:
            if f in rec:
                ts_shapes[shape(rec[f])] += 1
        for f in NUM_FIELDS:
            if f in rec:
                num_types[(f, type(rec[f]).__name__)] += 1
                x = to_num(rec[f])
                if x is None and not invalid(rec[f]):
                    nonnum[(f, repr(rec[f]))] += 1
                elif x is not None and x <= 0:
                    nonpos[(f, x)] += 1
        status[repr(rec.get("status", "<missing>"))] += 1

        # first reason this row would be dropped, mirroring the A3 step order
        if any(f not in rec or invalid(rec[f]) for f in FIELDS):
            reason = "missing/empty field"
        elif any(to_num(rec[f]) is None for f in NUM_FIELDS):
            reason = "non-numeric number field"
        elif any(not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", str(rec[f]))
                 for f in TS_FIELDS):
            reason = "bad timestamp (vs yyyy-MM-dd HH:mm:ss)"
        elif to_num(rec["distance_km"]) <= 0:
            reason = "distance_km <= 0"
        elif to_num(rec["order_amount"]) <= 0:
            reason = "order_amount <= 0"
        elif rec["status"] != "DELIVERED":
            reason = "status != DELIVERED"
        else:
            reason = "valid (before dedup)"
        row_issues[reason] += 1

    print(f"blank lines: {blank}")
    print(f"malformed JSON: {len(bad_json)}")
    for n, err, txt in bad_json:
        print(f"  line {n}: {err} | {txt}")
    print(f"valid JSON but not an object: {len(not_object)}")
    for n, txt in not_object:
        print(f"  line {n}: {txt}")
    print(f"missing keys: {dict(missing) or 'none'}")
    print(f"null/empty values: {dict(invalid_field) or 'none'}")
    print(f"unexpected extra keys: {dict(extra_keys) or 'none'}")
    print("timestamp shapes (9=digit, a=letter):")
    for s, c in ts_shapes.most_common():
        print(f"  {c:4d}  {s!r}")
    print(f"numeric JSON types: {dict(num_types)}")
    print(f"non-numeric number values: {dict(nonnum) or 'none'}")
    print(f"<= 0 number values: {dict(nonpos) or 'none'}")
    print(f"status values: {dict(status)}")
    dups = {l: c for l, c in seen_lines.items() if c > 1 and l.strip()}
    print(f"exact duplicate lines: {len(dups)} distinct lines repeated, "
          f"{sum(c - 1 for c in dups.values())} extra copies")
    for l, c in dups.items():
        print(f"  x{c}: {l[:100]}")
    print("first drop reason per row (A3 step order):")
    for r, c in row_issues.most_common():
        print(f"  {c:4d}  {r}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(Path.home() / "bd-a2/data/raw_orders.jsonl"))
