"""Task A3 validation: build tests/fixtures_job1.jsonl, run spark_job1 on it,
and compare each case with hand-computed expected values.

Usage (venv active): python tests/check_job1.py
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FIXTURE = os.path.join(HERE, "fixtures_job1.jsonl")
OUTPUT = os.path.join(HERE, "_out_job1.jsonl")
STATS = os.path.expanduser("~/bd-a2/stats/spark_job1_stats.json")

D = "2026-09-01 "
BASE = {
    "customer_id": "C1", "restaurant_id": "R1", "delivery_partner_id": "D1", "city": "Pune",
    "order_timestamp": D + "12:00:00", "accepted_timestamp": D + "12:02:00",
    "picked_up_timestamp": D + "12:12:00", "expected_delivery_timestamp": D + "12:40:00",
    "delivered_timestamp": D + "12:40:00",
    "distance_km": 7.0, "order_amount": 500, "delivery_fee": 30, "status": "DELIVERED",
}


def rec(oid, drop=(), **over):
    r = {"order_id": oid, **BASE, **over}
    for k in drop:
        r.pop(k)
    return json.dumps(r)


# Base record metrics: prep 10, delivery 28, fulfillment 40, speed 7/(28/60)=15, delay 0.
OK = dict(prep=10.0, deliv=28.0, fulfil=40.0, speed=15.0, delay=0.0,
          status="ON_TIME", cat="ON_TIME", amt=500.0, fee=30.0, dist=7.0)

# (case id, description, fixture lines, expected: drop-counter name or metric dict)
CASES = [
    ("F01", "malformed JSON", ['{"order_id": "F01", "city": '], "malformed"),
    ("F02", "valid JSON, empty object", ["{}"], "malformed"),
    ("F03", "JSON array, not an object", ["[1, 2]"], "malformed"),
    ("F04", "blank line", [""], "malformed"),
    ("F05", "missing key (city)", [rec("F05", drop=["city"])], "missing_or_invalid_field"),
    ("F06", "empty string customer_id", [rec("F06", customer_id="")], "missing_or_invalid_field"),
    ("F07", "whitespace-only city", [rec("F07", city="   ")], "missing_or_invalid_field"),
    ("F08", "null city", [rec("F08", city=None)], "missing_or_invalid_field"),
    ("F09", '"abc" as distance_km', [rec("F09", distance_km="abc")], "missing_or_invalid_field"),
    ("F10", "bad timestamp", [rec("F10", delivered_timestamp="2026-13-45 99:00:00")], "invalid_timestamp"),
    ("F11", "timestamp in another shape (ISO T/Z)",
     [rec("F11", order_timestamp="2026-09-01T12:00:00Z")], "invalid_timestamp"),
    ("F12", "distance_km 0", [rec("F12", distance_km=0)], "nonpositive_distance"),
    ("F13", "order_amount -5", [rec("F13", order_amount=-5)], "nonpositive_amount"),
    ("F14", "status CANCELLED", [rec("F14", status="CANCELLED")], "not_delivered"),
    ("F15", 'status "delivered" (case differs, D6)', [rec("F15", status="delivered")], "not_delivered"),
    ("F16", "exact duplicate pair (one kept)", [rec("F16"), rec("F16")], dict(OK, _dup=1)),
    ("F17", "delivery 0 min -> speed null (D2)",
     [rec("F17", picked_up_timestamp=D + "12:12:00", delivered_timestamp=D + "12:12:00")],
     dict(OK, deliv=0.0, fulfil=12.0, speed=None)),
    ("F18", "delay exactly 0", [rec("F18")], OK),
    ("F19", "delay 15 -> SLIGHT", [rec("F19", expected_delivery_timestamp=D + "12:25:00")],
     dict(OK, delay=15.0, status="DELAYED", cat="SLIGHT_DELAY")),
    ("F20", "delay 15.5 -> MODERATE (D3)", [rec("F20", expected_delivery_timestamp=D + "12:24:30")],
     dict(OK, delay=15.5, status="DELAYED", cat="MODERATE_DELAY")),
    ("F21", "delay 30 -> MODERATE", [rec("F21", expected_delivery_timestamp=D + "12:10:00")],
     dict(OK, delay=30.0, status="DELAYED", cat="MODERATE_DELAY")),
    ("F22", "delay 31 -> SEVERE", [rec("F22", expected_delivery_timestamp=D + "12:09:00")],
     dict(OK, delay=31.0, status="DELAYED", cat="SEVERE_DELAY")),
    ("F23", "early delivery -> delay clamped to 0",
     [rec("F23", expected_delivery_timestamp=D + "12:50:00")], OK),
    ("F24", "delivery_fee 0 is kept (D9)", [rec("F24", delivery_fee=0)], dict(OK, fee=0.0)),
    ("F25", "negative prep time kept (D8)", [rec("F25", picked_up_timestamp=D + "12:01:00")],
     dict(OK, prep=-1.0, deliv=39.0, speed=10.77)),  # 7 / (39/60) = 10.769...
    ("F26", "numbers given as JSON strings", [rec("F26", distance_km="7.0", order_amount="500",
                                                  delivery_fee="30")], OK),
    ("F27", "rounding: 7 min delivery, 20 s delay",
     [rec("F27", distance_km=10, picked_up_timestamp=D + "12:12:00",
          delivered_timestamp=D + "12:19:00", expected_delivery_timestamp=D + "12:18:40")],
     dict(OK, dist=10.0, deliv=7.0, fulfil=19.0, speed=85.71, delay=0.33,
          status="DELAYED", cat="SLIGHT_DELAY")),  # 10/(7/60)=85.714; 20/60=0.333
    ("F28", "extra unknown key ignored", [json.dumps({**json.loads(rec("F28")), "note": "x"})], OK),
]

FIELD_MAP = dict(prep="preparation_time_min", deliv="delivery_time_min",
                 fulfil="fulfillment_time_min", speed="delivery_speed_kmph", delay="delay_min",
                 status="delivery_status", cat="delay_category", amt="order_amount",
                 fee="delivery_fee", dist="distance_km")
CONTRACT_KEYS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_amount", "delivery_fee", "distance_km", "preparation_time_min", "delivery_time_min",
    "fulfillment_time_min", "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category",
]


def main():
    lines = [l for _, _, ls, _ in CASES for l in ls]
    with open(FIXTURE, "w") as fh:
        fh.write("\n".join(lines) + "\n")

    r = subprocess.run([sys.executable, os.path.join(REPO, "vm4/spark_job1.py"),
                        "--input", FIXTURE, "--output", OUTPUT], capture_output=True, text=True)
    print(r.stdout)
    if r.returncode:
        print(r.stderr[-3000:])
        sys.exit("spark_job1 failed")

    with open(OUTPUT) as fh:
        raw_out = [l for l in fh if l.strip()]
    out = {}
    for l in raw_out:
        o = json.loads(l)
        out.setdefault(o["order_id"], []).append(o)
    stats = json.load(open(STATS))

    fails = 0
    print(f"{'case':5} {'description':42} {'expected':48} actual")
    for cid, desc, _, exp in CASES:
        got = out.get(cid, [])
        if isinstance(exp, str):
            exp_s, ok = f"dropped ({exp})", not got
            act_s = "dropped" if not got else f"KEPT {len(got)} row(s)"
        else:
            want = {FIELD_MAP[k]: v for k, v in exp.items() if not k.startswith("_")}
            exp_s = "kept: " + ", ".join(f"{k}={v}" for k, v in exp.items()
                                         if not k.startswith("_") and v != OK.get(k))
            if exp == OK:
                exp_s = "kept: base values (prep 10, deliv 28, speed 15, ON_TIME)"
            if len(got) != 1:
                ok, act_s = False, f"{len(got)} rows"
            else:
                diffs = {k: got[0].get(k) for k, v in want.items() if got[0].get(k) != v}
                ok = not diffs
                act_s = "match" if ok else f"DIFF {diffs}"
        fails += not ok
        print(f"{'PASS' if ok else 'FAIL'} {cid:4} {desc:42} {exp_s[:48]:48} {act_s}")

    # Drop counters vs expected.
    exp_counts = {}
    for _, _, ls, exp in CASES:
        if isinstance(exp, str):
            exp_counts[exp] = exp_counts.get(exp, 0) + len(ls)
        elif exp.get("_dup"):
            exp_counts["duplicates"] = exp_counts.get("duplicates", 0) + exp["_dup"]
    print("\ndrop counters:")
    for k, v in exp_counts.items():
        a = stats[f"dropped_{k}"]
        fails += a != v
        print(f"  {'PASS' if a == v else 'FAIL'} dropped_{k}: expected {v}, actual {a}")
    exp_out = sum(1 for _, _, _, e in CASES if not isinstance(e, str))
    ok = stats["output_records"] == exp_out and stats["total_lines"] == len(lines)
    fails += not ok
    print(f"  {'PASS' if ok else 'FAIL'} total_lines {stats['total_lines']} (expected {len(lines)}), "
          f"output_records {stats['output_records']} (expected {exp_out})")

    # Output format: key order and explicit null.
    bad_order = [l for l in raw_out if list(json.loads(l).keys()) != CONTRACT_KEYS]
    null_kept = any('"delivery_speed_kmph":null' in l for l in raw_out)
    fails += bool(bad_order) + (not null_kept)
    print(f"  {'PASS' if not bad_order else 'FAIL'} all {len(raw_out)} rows have the 15 keys in contract order")
    print(f"  {'PASS' if null_kept else 'FAIL'} null speed written as JSON null")

    os.remove(OUTPUT)
    print(f"\n{'ALL PASS' if not fails else f'{fails} FAILURE(S)'}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
