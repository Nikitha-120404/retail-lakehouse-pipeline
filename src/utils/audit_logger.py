"""
audit_logger.py
───────────────
Structured audit logging for the NexCart Lakehouse pipeline.
Events are persisted to a PostgreSQL table: audit_events.

Connection is configured via environment variables (or .env):
    NEXCART_PG_DSN       postgresql://user:pass@host:5432/dbname  (takes priority)
    NEXCART_PG_HOST      default: localhost
    NEXCART_PG_PORT      default: 5432
    NEXCART_PG_DB        default: nexcart_audit
    NEXCART_PG_USER      default: postgres
    NEXCART_PG_PASSWORD  (required when not using DSN)

The audit_events table is created automatically on first run.

Event types recorded
────────────────────
  pipeline_start   – top of a pipeline run
  pipeline_end     – end of a pipeline run (with total duration)
  step_start       – a layer/table step is about to begin
  step_complete    – step finished successfully (with duration)
  step_failed      – step raised an exception (error details + duration)
  table_stats      – row count recorded for a Parquet output table
  table_stats_error– row-count attempt failed (soft warning)
  quality_result   – one DQ rule outcome from DataQualityChecker

Useful queries
──────────────
    -- Full run history
    SELECT run_id, event_type, layer, table_name, status, duration_s, ts
    FROM   audit_events
    ORDER  BY ts DESC;

    -- All steps for a specific run
    SELECT layer, table_name, event_type, duration_s, ts
    FROM   audit_events
    WHERE  run_id = '<run_id>'
    ORDER  BY ts;

    -- Failed steps across all runs
    SELECT run_id, layer, table_name, error_type, error_message, ts
    FROM   audit_events
    WHERE  event_type = 'step_failed'
    ORDER  BY ts DESC;

    -- Row counts per layer per run
    SELECT run_id, layer, table_name, row_count, ts
    FROM   audit_events
    WHERE  event_type = 'table_stats'
    ORDER  BY run_id, layer, ts;

    -- DQ failures
    SELECT run_id, table_name, rule_name, column_name, metric, threshold, message, ts
    FROM   audit_events
    WHERE  event_type = 'quality_result' AND status = 'FAIL'
    ORDER  BY ts DESC;

Typical usage
─────────────
    from src.utils.audit_logger import AuditLogger

    audit = AuditLogger()
    audit.log_pipeline_start("full")

    with audit.track_step("bronze", "customers"):
        BronzeIngestor().ingest_table("customers")

    audit.log_table_stats(spark, "bronze", "customers", get_path("bronze") / "customers")
    audit.log_pipeline_end("full", status="success")
    audit.print_summary()
    audit.close()
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator, Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv()  # load .env if present

# ── DDL ────────────────────────────────────────────────────────────────────────
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id             BIGSERIAL         PRIMARY KEY,
    run_id         VARCHAR(16)       NOT NULL,
    event_type     VARCHAR(64)       NOT NULL,
    ts             TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    pipeline       VARCHAR(128),
    layer          VARCHAR(64),
    table_name     VARCHAR(128),
    status         VARCHAR(32),
    duration_s     DOUBLE PRECISION,
    row_count      BIGINT,
    path           TEXT,
    rule_name      VARCHAR(128),
    column_name    VARCHAR(128),
    metric         DOUBLE PRECISION,
    threshold      DOUBLE PRECISION,
    message        TEXT,
    error_type     VARCHAR(128),
    error_message  TEXT
);

CREATE INDEX IF NOT EXISTS audit_events_run_id_idx
    ON audit_events (run_id);

CREATE INDEX IF NOT EXISTS audit_events_ts_idx
    ON audit_events (ts DESC);

CREATE INDEX IF NOT EXISTS audit_events_event_type_idx
    ON audit_events (event_type);
"""

_INSERT_SQL = """
INSERT INTO audit_events (
    run_id, event_type, ts,
    pipeline, layer, table_name, status,
    duration_s, row_count, path,
    rule_name, column_name, metric, threshold, message,
    error_type, error_message
) VALUES (
    %(run_id)s, %(event_type)s, %(ts)s,
    %(pipeline)s, %(layer)s, %(table_name)s, %(status)s,
    %(duration_s)s, %(row_count)s, %(path)s,
    %(rule_name)s, %(column_name)s, %(metric)s, %(threshold)s, %(message)s,
    %(error_type)s, %(error_message)s
)
"""


# ══════════════════════════════════════════════════════════════════════════════
# Connection helper
# ══════════════════════════════════════════════════════════════════════════════

def _build_dsn() -> str:
    """
    Build a libpq DSN string from environment variables.

    Priority:
      1. NEXCART_PG_DSN  (full DSN — used as-is)
      2. Individual NEXCART_PG_* variables
    """
    dsn = os.getenv("NEXCART_PG_DSN")
    if dsn:
        return dsn
    host     = os.getenv("NEXCART_PG_HOST",     "localhost")
    port     = os.getenv("NEXCART_PG_PORT",     "5432")
    dbname   = os.getenv("NEXCART_PG_DB",       "nexcart_audit")
    user     = os.getenv("NEXCART_PG_USER",     "postgres")
    password = os.getenv("NEXCART_PG_PASSWORD", "")
    return (
        f"host={host} port={port} dbname={dbname}"
        f" user={user} password={password}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# AuditLogger
# ══════════════════════════════════════════════════════════════════════════════

class AuditLogger:
    """
    Persists structured audit events to PostgreSQL (audit_events table).

    One connection is held open per AuditLogger instance.  Call close()
    when the pipeline finishes to release it.

    If the PostgreSQL connection cannot be established — or any individual
    INSERT fails — the error is printed to stderr and the pipeline continues
    unaffected.  Events are always kept in-memory so summary() works
    regardless of DB availability.

    Parameters
    ----------
    run_id : str, optional
        Caller-supplied identifier.  Auto-generated (8-char hex) when omitted.
    """

    def __init__(self, run_id: Optional[str] = None) -> None:
        self.run_id         = run_id or uuid.uuid4().hex[:8]
        self._events:       list[dict] = []
        self._session_start = datetime.now(timezone.utc)
        self._conn          = self._connect()

        print(
            f"[Audit] Logger ready  | run_id={self.run_id}"
            f" | db={'connected' if self._conn else 'unavailable (events in-memory only)'}"
        )

    # ── Connection ────────────────────────────────────────────────────────────

    def _connect(self):
        """Open the PostgreSQL connection and ensure the table exists."""
        try:
            conn = psycopg2.connect(_build_dsn())
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
            conn.commit()
            return conn
        except Exception as exc:
            print(
                f"[Audit] WARNING: Cannot connect to PostgreSQL: {exc}\n"
                f"         Events will be kept in-memory only.",
                file=sys.stderr,
            )
            return None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _record(self, event_type: str, **fields: Any) -> dict:
        """Build an event dict, store it in-memory, and write it to PostgreSQL."""
        event: dict = {
            "run_id":     self.run_id,
            "event_type": event_type,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        self._events.append(event)
        self._pg_insert(event)
        return event

    def _pg_insert(self, event: dict) -> None:
        """Map the event dict to the audit_events columns and INSERT."""
        if self._conn is None:
            return
        params = {
            "run_id":        event["run_id"],
            "event_type":    event["event_type"],
            "ts":            event["timestamp"],
            "pipeline":      event.get("pipeline"),
            "layer":         event.get("layer"),
            "table_name":    event.get("table"),
            "status":        event.get("status"),
            # pipeline_end uses total_duration_s; step events use duration_s
            "duration_s":    event.get("duration_s") or event.get("total_duration_s"),
            "row_count":     event.get("row_count"),
            "path":          event.get("path"),
            "rule_name":     event.get("rule_name"),
            "column_name":   event.get("column"),
            "metric":        event.get("metric"),
            "threshold":     event.get("threshold"),
            "message":       event.get("message"),
            "error_type":    event.get("error_type"),
            "error_message": event.get("error_message"),
        }
        try:
            with self._conn.cursor() as cur:
                cur.execute(_INSERT_SQL, params)
            self._conn.commit()
        except Exception as exc:
            print(
                f"[Audit] WARNING: PostgreSQL INSERT failed: {exc}",
                file=sys.stderr,
            )
            try:
                self._conn.rollback()
            except Exception:
                pass

    # ── Pipeline-level ────────────────────────────────────────────────────────

    def log_pipeline_start(self, pipeline_name: str) -> None:
        """Record the start of a named pipeline (e.g. "full", "bronze")."""
        self._record("pipeline_start", pipeline=pipeline_name)
        print(f"[Audit] pipeline_start  -> {pipeline_name}")

    def log_pipeline_end(
        self, pipeline_name: str, status: str = "success"
    ) -> None:
        """
        Record the end of a pipeline run.

        Parameters
        ----------
        status : "success" | "failed" | "partial"
        """
        elapsed = round(
            (datetime.now(timezone.utc) - self._session_start).total_seconds(), 2
        )
        self._record(
            "pipeline_end",
            pipeline=pipeline_name,
            status=status,
            total_duration_s=elapsed,
        )
        print(
            f"[Audit] pipeline_end    -> {pipeline_name}"
            f" | status={status} | elapsed={elapsed}s"
        )

    # ── Step-level ────────────────────────────────────────────────────────────

    def log_step_start(self, layer: str, table: str) -> None:
        self._record("step_start", layer=layer, table=table)

    def log_step_complete(
        self, layer: str, table: str, duration_s: float
    ) -> None:
        self._record(
            "step_complete", layer=layer, table=table, duration_s=duration_s
        )

    def log_step_failed(
        self,
        layer:         str,
        table:         str,
        duration_s:    float,
        error_type:    str,
        error_message: str,
    ) -> None:
        self._record(
            "step_failed",
            layer=layer,
            table=table,
            duration_s=duration_s,
            error_type=error_type,
            error_message=error_message,
        )
        print(
            f"[Audit] FAILED  {layer}.{table}"
            f" | {error_type}: {error_message}"
        )

    # ── Table statistics ──────────────────────────────────────────────────────

    def log_table_stats(
        self,
        spark,
        layer: str,
        table: str,
        path:  Any,
    ) -> int:
        """
        Read *path* as Parquet, record the row count in PostgreSQL, and
        return it.

        This is a soft operation — if the read fails the error is recorded
        but no exception is raised.

        Returns the row count, or -1 on failure.
        """
        from src.utils.parquet_utils import read_parquet  # avoid circular import
        try:
            df = read_parquet(spark, path)
            n  = df.count()
            self._record(
                "table_stats",
                layer=layer,
                table=table,
                path=str(path),
                row_count=n,
            )
            print(f"[Audit] table_stats     {layer}.{table} -> {n:,} rows")
            return n
        except Exception as exc:
            self._record(
                "table_stats_error",
                layer=layer,
                table=table,
                path=str(path),
                message=str(exc),
            )
            print(f"[Audit] table_stats WARN {layer}.{table}: {exc}")
            return -1

    # ── Data quality ──────────────────────────────────────────────────────────

    def log_quality_result(
        self,
        table:      str,
        rule_name:  str,
        column:     str,
        status:     str,
        metric:     float,
        threshold:  float,
        message:    str,
    ) -> None:
        """Forward one DQResult from DataQualityChecker into audit_events."""
        self._record(
            "quality_result",
            table=table,
            rule_name=rule_name,
            column=column,
            status=status,
            metric=metric,
            threshold=threshold,
            message=message,
        )

    def log_quality_results_bulk(self, results: list) -> None:
        """
        Convenience wrapper — log every DQResult in *results* in one call.

        Compatible with the list returned by run_silver_quality_checks().
        """
        for r in results:
            self.log_quality_result(
                r.table, r.rule_name, r.column,
                r.status, r.metric, r.threshold, r.message,
            )
        failed = sum(1 for r in results if r.status == "FAIL")
        print(
            f"[Audit] quality_results -> {len(results)} rules logged"
            f" | {failed} FAIL(s)"
        )

    # ── Context manager ───────────────────────────────────────────────────────

    @contextmanager
    def track_step(
        self, layer: str, table: str
    ) -> Generator[None, None, None]:
        """
        Wrap a pipeline step with audit events.

        Records step_start immediately, then step_complete or step_failed
        depending on whether the body raises.  The exception is always
        re-raised so the caller's error handling is unaffected.

        Example
        -------
            with audit.track_step("silver", "orders"):
                transformer.run_orders()
        """
        self.log_step_start(layer, table)
        t0 = time.monotonic()
        try:
            yield
            self.log_step_complete(layer, table, round(time.monotonic() - t0, 2))
        except Exception as exc:
            self.log_step_failed(
                layer, table,
                round(time.monotonic() - t0, 2),
                type(exc).__name__,
                str(exc),
            )
            raise

    # ── Summary ───────────────────────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        """Return a lightweight in-memory summary dict for this run."""
        steps      = [e for e in self._events
                      if e["event_type"] in ("step_complete", "step_failed")]
        ok         = [e for e in steps if e["event_type"] == "step_complete"]
        failed     = [e for e in steps if e["event_type"] == "step_failed"]
        stats_evts = [e for e in self._events if e["event_type"] == "table_stats"]
        dq_evts    = [e for e in self._events if e["event_type"] == "quality_result"]

        total_rows = sum(e.get("row_count", 0) for e in stats_evts)
        total_dur  = sum(e.get("duration_s", 0.0) for e in steps)
        dq_failed  = sum(1 for e in dq_evts if e.get("status") == "FAIL")

        return {
            "run_id":                self.run_id,
            "steps_ok":              len(ok),
            "steps_failed":          len(failed),
            "table_stats_collected": len(stats_evts),
            "total_rows_observed":   total_rows,
            "total_step_duration_s": round(total_dur, 2),
            "quality_rules_logged":  len(dq_evts),
            "quality_rules_failed":  dq_failed,
            "total_events":          len(self._events),
            "pg_connected":          self._conn is not None,
        }

    def print_summary(self) -> None:
        """Print a formatted summary of this run to stdout."""
        s = self.summary()
        width = 55
        print("\n" + "─" * width)
        print("  Audit Run Summary")
        print("─" * width)
        print(f"  run_id               : {s['run_id']}")
        print(f"  steps ok             : {s['steps_ok']}")
        print(f"  steps failed         : {s['steps_failed']}")
        print(f"  table stats recorded : {s['table_stats_collected']}")
        print(f"  total rows observed  : {s['total_rows_observed']:,}")
        print(f"  total step duration  : {s['total_step_duration_s']}s")
        print(f"  DQ rules logged      : {s['quality_rules_logged']}")
        print(f"  DQ rules failed      : {s['quality_rules_failed']}")
        print(f"  total events         : {s['total_events']}")
        print(f"  postgresql           : {'connected' if s['pg_connected'] else 'unavailable'}")
        print("─" * width + "\n")

    def close(self) -> None:
        """Commit any pending work and close the PostgreSQL connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        print(
            f"[Audit] Closed | {len(self._events)} events persisted"
            f" | run_id={self.run_id}"
        )
