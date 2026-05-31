import os
import re
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _escape_label(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _metric(name, value, labels):
    label_text = ",".join(f'{k}="{_escape_label(v)}"' for k, v in labels.items())
    return f"{name}{{{label_text}}} {float(value)}"


def _parse_progress_ts(value):
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _safe_query_name(query):
    return query.name or "unnamed"


def _collect_streaming_metrics(spark, job_name):
    lines = [
        "# HELP nyc_spark_stream_exporter_up 1 if custom Spark streaming exporter is running.",
        "# TYPE nyc_spark_stream_exporter_up gauge",
        _metric("nyc_spark_stream_exporter_up", 1, {"job": job_name}),
        "# HELP nyc_spark_stream_active 1 if a Structured Streaming query is active.",
        "# TYPE nyc_spark_stream_active gauge",
        "# HELP nyc_spark_stream_input_rows Number of input rows in the last trigger.",
        "# TYPE nyc_spark_stream_input_rows gauge",
        "# HELP nyc_spark_stream_input_rows_per_second Input rows per second in the last trigger.",
        "# TYPE nyc_spark_stream_input_rows_per_second gauge",
        "# HELP nyc_spark_stream_processed_rows_per_second Processed rows per second in the last trigger.",
        "# TYPE nyc_spark_stream_processed_rows_per_second gauge",
        "# HELP nyc_spark_stream_batch_duration_ms Trigger execution duration in milliseconds.",
        "# TYPE nyc_spark_stream_batch_duration_ms gauge",
        "# HELP nyc_spark_stream_operation_duration_ms Operation duration in milliseconds.",
        "# TYPE nyc_spark_stream_operation_duration_ms gauge",
        "# HELP nyc_spark_stream_state_rows_total Total rows held in state operators.",
        "# TYPE nyc_spark_stream_state_rows_total gauge",
        "# HELP nyc_spark_stream_last_progress_timestamp Unix timestamp of last query progress.",
        "# TYPE nyc_spark_stream_last_progress_timestamp gauge",
    ]

    active_queries = list(spark.streams.active)
    if not active_queries:
        lines.append(
            _metric("nyc_spark_stream_active", 0, {"job": job_name, "query": "none"})
        )
        return "\n".join(lines) + "\n"

    for query in active_queries:
        query_name = _safe_query_name(query)
        labels = {
            "job": job_name,
            "query": query_name,
            "query_id": str(query.id),
            "run_id": str(query.runId),
        }
        lines.append(_metric("nyc_spark_stream_active", 1, labels))

        progress = query.lastProgress
        if not progress:
            continue

        progress_labels = dict(labels)
        batch_id = progress.get("batchId")
        if batch_id is not None:
            progress_labels["batch_id"] = batch_id

        lines.append(
            _metric(
                "nyc_spark_stream_input_rows",
                progress.get("numInputRows", 0),
                progress_labels,
            )
        )
        lines.append(
            _metric(
                "nyc_spark_stream_input_rows_per_second",
                progress.get("inputRowsPerSecond", 0.0),
                progress_labels,
            )
        )
        lines.append(
            _metric(
                "nyc_spark_stream_processed_rows_per_second",
                progress.get("processedRowsPerSecond", 0.0),
                progress_labels,
            )
        )
        lines.append(
            _metric(
                "nyc_spark_stream_last_progress_timestamp",
                _parse_progress_ts(progress.get("timestamp")),
                progress_labels,
            )
        )

        duration_ms = progress.get("durationMs") or {}
        trigger_ms = duration_ms.get("triggerExecution", 0)
        lines.append(
            _metric("nyc_spark_stream_batch_duration_ms", trigger_ms, progress_labels)
        )
        for operation, duration in duration_ms.items():
            op_labels = dict(progress_labels)
            op_labels["operation"] = re.sub(r"[^A-Za-z0-9_:-]", "_", operation)
            lines.append(
                _metric("nyc_spark_stream_operation_duration_ms", duration, op_labels)
            )

        state_rows = 0
        for state in progress.get("stateOperators") or []:
            state_rows += int(state.get("numRowsTotal") or 0)
        lines.append(
            _metric("nyc_spark_stream_state_rows_total", state_rows, progress_labels)
        )

    return "\n".join(lines) + "\n"


def start_streaming_metrics_exporter(spark, job_name, port=None):
    port = int(port or os.getenv("STREAMING_METRICS_PORT", "0"))
    if port <= 0:
        print(
            f"[streaming_metrics] disabled for job={job_name}; STREAMING_METRICS_PORT is not set",
            flush=True,
        )
        return None

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/metrics", "/health"):
                self.send_response(404)
                self.end_headers()
                return
            if self.path == "/health":
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = _collect_streaming_metrics(spark, job_name).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(
        f"[streaming_metrics] job={job_name} listening on http://0.0.0.0:{port}/metrics",
        flush=True,
    )
    return server
