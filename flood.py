#!/usr/bin/env python3
"""Bounded HTTP load test with a configurable target (host/port via CLI args).

Adaptado del script que nos dio el profesor (comparación nginx protegido vs.
sin protección). Aquí ya NO está hardcodeado a "victim-protected" /
"victim-unprotected": recibe --host y --port para poder apuntarlo a cualquiera
de nuestros 3 servicios (frontend, backend, ldap-api), corriendo FUERA de sus
imágenes de Docker (en la máquina del host).
"""

from __future__ import annotations

import argparse
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

REQUEST_TIMEOUT_SECONDS = 2
REPORT_INTERVAL_SECONDS = 3
MAX_DURATION_SECONDS = 300
MAX_CONCURRENCY = 500
MAX_REQUESTS_PER_SECOND = 500


def bounded_int(name: str, value: int, maximum: int) -> int:
    """Validate a bounded positive integer argument."""
    if not 1 <= value <= maximum:
        raise SystemExit(f"--{name} must be between 1 and {maximum}; got {value}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prueba de carga HTTP acotada contra un host:puerto propio."
    )
    parser.add_argument("--host", required=True, help="Host/IP del servicio a probar (ej. localhost)")
    parser.add_argument("--port", required=True, type=int, help="Puerto del servicio a probar (ej. 3000)")
    parser.add_argument("--path", default="/", help="Ruta HTTP a solicitar (default: /)")
    parser.add_argument(
        "--duration", type=int, default=20,
        help=f"Segundos que dura la prueba (1-{MAX_DURATION_SECONDS}, default 20)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=40,
        help=f"Workers en paralelo (1-{MAX_CONCURRENCY}, default 40)",
    )
    parser.add_argument(
        "--requests-per-second", type=int, default=50,
        help=f"Límite agregado de peticiones nuevas por segundo (1-{MAX_REQUESTS_PER_SECOND}, default 50)",
    )
    args = parser.parse_args()

    bounded_int("duration", args.duration, MAX_DURATION_SECONDS)
    bounded_int("concurrency", args.concurrency, MAX_CONCURRENCY)
    bounded_int("requests-per-second", args.requests_per_second, MAX_REQUESTS_PER_SECOND)
    return args


def request_status(opener: urllib.request.OpenerDirector, url: str) -> str:
    """Perform one GET and return its HTTP status, or 000 on network failure."""
    request = urllib.request.Request(url, method="GET")
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return str(response.status)
    except urllib.error.HTTPError as error:
        return str(error.code)
    except (urllib.error.URLError, TimeoutError, OSError):
        return "000"


class RateLimiter:
    """Limit aggregate request starts across all workers."""

    def __init__(self, requests_per_second: int) -> None:
        self.interval = 1 / requests_per_second
        self.next_request_at = 0.0
        self.lock = threading.Lock()

    def wait_for_turn(self, stop_event: threading.Event) -> bool:
        with self.lock:
            now = time.monotonic()
            scheduled_at = max(now, self.next_request_at)
            self.next_request_at = scheduled_at + self.interval
        return not stop_event.wait(max(0.0, scheduled_at - now))


def worker(
    url: str,
    deadline: float,
    stop_event: threading.Event,
    rate_limiter: RateLimiter,
    counts: Counter[str],
    counts_lock: threading.Lock,
) -> None:
    """Send requests until the deadline or an interrupt."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while not stop_event.is_set() and time.monotonic() < deadline:
        if not rate_limiter.wait_for_turn(stop_event):
            break
        if time.monotonic() >= deadline:
            break
        code = request_status(opener, url)
        with counts_lock:
            counts[code] += 1


def snapshot(counts: Counter[str], counts_lock: threading.Lock) -> Counter[str]:
    with counts_lock:
        return counts.copy()


def report(label: str, counts: Counter[str], final: bool = False) -> None:
    total = sum(counts.values())
    ok = counts["200"]
    failed = total - ok
    tag = "RESULT" if final else f"[{datetime.now():%H:%M:%S}] requests so far"
    print(f"  {tag} for {label}: total={total} ok(200)={ok} blocked/failed={failed}", flush=True)
    if final and total:
        print(f"  -> {failed * 100 // total}% of responses were non-200", flush=True)


def run(url: str, concurrency: int, duration: int, requests_per_second: int) -> bool:
    """Run one bounded test; return False if interrupted by Ctrl-C."""
    print(
        f"\n--- Target: {url} ({concurrency} workers, "
        f"{requests_per_second} requests/s max, {duration}s) ---",
        flush=True,
    )
    counts: Counter[str] = Counter()
    counts_lock = threading.Lock()
    stop_event = threading.Event()
    rate_limiter = RateLimiter(requests_per_second)
    deadline = time.monotonic() + duration
    interrupted = False

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(worker, url, deadline, stop_event, rate_limiter, counts, counts_lock)
            for _ in range(concurrency)
        ]
        try:
            while time.monotonic() < deadline:
                time.sleep(min(REPORT_INTERVAL_SECONDS, deadline - time.monotonic()))
                report(url, snapshot(counts, counts_lock))
        except KeyboardInterrupt:
            interrupted = True
            stop_event.set()
            print("\nStopping workers after Ctrl-C...", flush=True)
        finally:
            stop_event.set()

        for future in futures:
            future.result()

    report(url, snapshot(counts, counts_lock), final=True)
    return not interrupted


def main() -> None:
    args = parse_args()
    url = f"http://{args.host}:{args.port}{args.path}"

    print(
        "=== Bounded HTTP load test: "
        f"{args.concurrency} workers, up to {args.requests_per_second} requests/s, "
        f"{args.duration}s ===",
        flush=True,
    )
    run(url, args.concurrency, args.duration, args.requests_per_second)

    print("\n=== Test finished; only the target you specified was contacted. ===")
    print("A non-200 result can be a Fail2Ban ban, an HTTP error, or a connection timeout.")


if __name__ == "__main__":
    main()
