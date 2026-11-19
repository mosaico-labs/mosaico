"""End-to-end check of the local deployment telemetry stack.

Sends Flight calls to mosaicod, then waits until each signal reaches its backend:
metrics in Mimir, logs in Loki and traces in Tempo. Finally it asks the three backends to
flush, which they otherwise do only every few hours, and checks that their blocks land in
their buckets.

    uv run --with pyarrow docker/deployment-telemetry/check.py
"""

import datetime
import json
import sys
import time
import urllib.parse
import urllib.request
import uuid

import pyarrow as pa
import pyarrow.flight as flight
from pyarrow import fs

MOSAICOD = "grpc://127.0.0.1:6726"
MIMIR = "http://127.0.0.1:9009"
LOKI = "http://127.0.0.1:3100"
TEMPO = "http://127.0.0.1:3200"
STORE = "http://127.0.0.1:7091"  # the telemetry store
TIMEOUT_S = 120
# The volumes outlive previous runs, so every check only counts what this run produced:
# the unknown action carries RUN_ID into the error log, and objects and traces must be
# newer than STARTED_AT.
RUN_ID = uuid.uuid4().hex[:12]
STARTED_AT = datetime.datetime.now(datetime.timezone.utc)


def get_json(base, path, params):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def send_traffic():
    """Makes a few Flight calls. Error responses are fine: they are measured too."""
    client = flight.connect(MOSAICOD)
    calls = {
        "ListFlights": lambda: list(client.list_flights()),
        "ListActions": lambda: list(client.list_actions()),
        "DoAction (unknown action)": lambda: list(
            client.do_action(flight.Action(f"deployment_telemetry_check_{RUN_ID}", b""))
        ),
    }
    for name, call in calls.items():
        try:
            result = call()
            print(f"mosaicod: {name} returned {len(result)} items")
        except pa.ArrowException as e:
            print(f"mosaicod: {name} returned an error ({type(e).__name__})")
    client.close()


def do_action_calls():
    """Total `DoAction` calls Mimir has seen, 0 if none yet."""
    query = 'sum(rpc_server_call_duration_count{job="mosaicod", rpc_method="DoAction"})'
    result = get_json(MIMIR, "/prometheus/api/v1/query", {"query": query})["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0


def metrics_in_mimir(baseline):
    def check():
        calls = do_action_calls()
        return [f"DoAction calls: {baseline:g} before this run, {calls:g} now"] if calls > baseline else []

    return check


def logs_in_loki():
    now_ns = time.time_ns()
    data = get_json(
        LOKI,
        "/loki/api/v1/query_range",
        {
            "query": f'{{service_name="mosaicod"}} |= "{RUN_ID}"',
            "start": int(STARTED_AT.timestamp()) * 10**9,
            "end": now_ns,
            "limit": 3,
        },
    )["data"]["result"]
    return [line for stream in data for _, line in stream["values"]]


def traces_in_tempo():
    params = {
        "tags": "service.name=mosaicod",
        "start": int(STARTED_AT.timestamp()),
        "end": int(time.time()) + 1,
        "limit": 5,
    }
    traces = get_json(TEMPO, "/api/search", params).get("traces", [])
    return [f"{t['traceID']} {t.get('rootTraceName', '')}" for t in traces]


def post(url):
    req = urllib.request.Request(url, data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status


FLUSH = {
    "Mimir": f"{MIMIR}/ingester/flush",
    "Loki": f"{LOKI}/flush",
    "Tempo": f"{TEMPO}/flush",
}

STORE_FS = fs.S3FileSystem(
    access_key="telemetry",
    secret_key="telemetry-password",
    endpoint_override=STORE,
    region="us-east-1",
)


def objects_in(bucket, prefix, suffix):
    """Lists the objects of `bucket` under `prefix` whose key ends with `suffix`.

    Each backend also writes a cluster seed file at startup, so a non-empty bucket alone
    does not prove that any data was flushed. Objects older than this run are ignored.
    """

    def check():
        selector = fs.FileSelector(f"{bucket}/{prefix}", recursive=True, allow_not_found=True)
        return [
            f"{i.path} ({i.size} bytes)"
            for i in STORE_FS.get_file_info(selector)
            if i.type == fs.FileType.File
            and i.path.endswith(suffix)
            and i.mtime is not None
            and i.mtime >= STARTED_AT
        ]

    return check


BUCKET_CHECKS = {
    # Mimir without multi-tenancy uses the `anonymous` tenant, one directory per block.
    "Mimir blocks in obs-metrics": objects_in("obs-metrics", "blocks/anonymous", "/meta.json"),
    # Loki without auth uses the `fake` tenant.
    "Loki chunks in obs-logs": objects_in("obs-logs", "fake", ""),
    "Tempo blocks in obs-traces": objects_in("obs-traces", "single-tenant", "/data.parquet"),
}


def wait_for(checks):
    """Runs every check until it finds something, and returns the names that never did."""
    pending = dict(checks)
    deadline = time.monotonic() + TIMEOUT_S
    while pending and time.monotonic() < deadline:
        for name, check in list(pending.items()):
            try:
                found = check()
            except Exception as e:  # The backend may still be starting.
                found, error = [], e
            else:
                error = None
            if found:
                print(f"OK   {name}")
                for item in found[:3]:
                    print(f"     {item[:160]}")
                del pending[name]
            elif error is not None:
                print(f"...  {name}: {error}")
        if pending:
            time.sleep(5)

    for name in pending:
        print(f"FAIL {name}: nothing after {TIMEOUT_S}s")
    return list(pending)


def main():
    print(f"run {RUN_ID}")
    try:
        baseline = do_action_calls()
    except OSError:  # Mimir still starting: any call counts.
        baseline = 0.0
    send_traffic()

    failed = wait_for(
        {
            "metrics in Mimir": metrics_in_mimir(baseline),
            "logs in Loki": logs_in_loki,
            "traces in Tempo": traces_in_tempo,
        }
    )

    # Flush only once this run's data is in every backend, otherwise there is nothing new
    # to write and no new object appears.

    for name, url in FLUSH.items():
        print(f"flush: {name} answered {post(url)}")
    failed += wait_for(BUCKET_CHECKS)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
