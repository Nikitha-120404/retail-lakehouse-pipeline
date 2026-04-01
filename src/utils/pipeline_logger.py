"""
pipeline_logger.py
──────────────────
Tracks pipeline run metadata: which layer ran, when, row counts, duration.
Writes a JSON log to logs/pipeline_runs.jsonl for observability.
"""
from __future__ import annotations
import json
import time
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

_LOG_FILE = Path(__file__).resolve().parents[2] / "logs" / "pipeline_runs.jsonl"
_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def track_step(layer: str, table: str):
    """Context manager — wraps a pipeline step and logs duration + status."""
    start = time.time()
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "layer":     layer,
        "table":     table,
        "status":    "running",
        "duration_s": None,
        "error":      None,
    }
    try:
        yield record
        record["status"]    = "success"
        record["duration_s"] = round(time.time() - start, 2)
        print(f"[{layer.upper()}] ✓ {table} ({record['duration_s']}s)")
    except Exception as e:
        record["status"]    = "failed"
        record["duration_s"] = round(time.time() - start, 2)
        record["error"]     = str(e)
        print(f"[{layer.upper()}] ✗ {table} FAILED: {e}")
        raise
    finally:
        with open(_LOG_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")


def print_run_history(last_n: int = 20) -> None:
    """Print last N pipeline run records from the JSONL log."""
    if not _LOG_FILE.exists():
        print("No pipeline runs logged yet.")
        return
    lines = _LOG_FILE.read_text().strip().split("\n")
    records = [json.loads(l) for l in lines if l.strip()][-last_n:]
    print(f"\n{'TIME':<22} {'LAYER':<12} {'TABLE':<30} {'STATUS':<10} {'DURATION':>10}")
    print("-" * 90)
    for r in records:
        ts   = r.get("timestamp","")[:19]
        icon = "✓" if r["status"] == "success" else "✗"
        dur  = f"{r['duration_s']}s" if r['duration_s'] else "-"
        print(f"{ts:<22} {r['layer']:<12} {r['table']:<30} {icon} {r['status']:<8} {dur:>10}")
    print()
