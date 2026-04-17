import argparse
import json
import os
import time
import traceback
from datetime import datetime
from pathlib import Path


DEFAULT_SLEEP_SECONDS = 1800
DEFAULT_TARGET_ARTICLES = 35
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cache"
LATEST_PATH = CACHE_DIR / "latest.json"
OUTPUT_PATH = LATEST_PATH
LOCK_PATH = CACHE_DIR / "run.lock"
LEGACY_LOCK_PATH = CACHE_DIR / ".scheduler.lock"
RUN_LOG_PATH = CACHE_DIR / "run_log.txt"
ERROR_LOG_PATH = CACHE_DIR / "error_log.txt"
STALE_LOCK_SECONDS = 6 * 60 * 60


def _save_json(news: list) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    backup_name = datetime.now().strftime("%Y-%m-%d_%H-%M.json")
    backup_path = CACHE_DIR / backup_name

    print("Saving JSON...")
    payload = json.dumps(news, ensure_ascii=False, indent=2)
    LATEST_PATH.write_text(payload, encoding="utf-8")
    backup_path.write_text(payload, encoding="utf-8")
    print("Done")


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def _from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _append_json_line(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False) + os.linesep
    fd = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY)
    try:
        os.write(fd, line.encode("utf-8", errors="replace"))
    finally:
        os.close(fd)


def _is_pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _read_lock_data(lock_path: Path) -> dict:
    try:
        lock_raw = lock_path.read_text(encoding="utf-8").strip()
        lock_data = json.loads(lock_raw) if lock_raw else {}
        return lock_data if isinstance(lock_data, dict) else {}
    except Exception:
        return {}


def _read_lock_start_ts(lock_path: Path) -> float:
    try:
        lock_data = _read_lock_data(lock_path)
        return float(lock_data.get("timestamp", 0.0) or 0.0)
    except Exception:
        return 0.0


def _read_lock_pid(lock_path: Path) -> int | None:
    data = _read_lock_data(lock_path)
    try:
        pid = int(data.get("pid"))
        return pid if pid > 0 else None
    except Exception:
        return None


def _is_stale_lock(lock_path: Path) -> tuple[bool, int | None]:
    pid = _read_lock_pid(lock_path)
    if pid is not None and not _is_pid_alive(pid):
        return True, pid

    # Fallback for malformed/legacy lock data with missing pid.
    started = _read_lock_start_ts(lock_path)
    if started and (time.time() - started) > STALE_LOCK_SECONDS:
        return True, pid

    return False, pid


def _log_run(started_at: datetime, ended_at: datetime, status: str, *, error: str | None = None) -> None:
    duration_seconds = max(0.0, (ended_at - started_at).total_seconds())
    payload = {
        "start_timestamp": _to_iso(started_at),
        "end_timestamp": _to_iso(ended_at),
        "status": status,
        "duration_seconds": round(duration_seconds, 3),
        "pid": os.getpid(),
    }
    if error:
        payload["error"] = error
    _append_json_line(RUN_LOG_PATH, payload)


def _log_error(exc: Exception) -> None:
    payload = {
        "timestamp": _to_iso(datetime.now()),
        "pid": os.getpid(),
        "error": str(exc),
        "traceback": traceback.format_exc(),
    }
    _append_json_line(ERROR_LOG_PATH, payload)


def _log_stale_lock(lock_path: Path, pid: int | None) -> None:
    _append_json_line(
        RUN_LOG_PATH,
        {
            "event": "STALE_LOCK",
            "timestamp": _to_iso(datetime.now()),
            "path": str(lock_path),
            "pid": pid,
        },
    )


def _lock_owner_info(lock_path: Path) -> tuple[int | None, datetime | None]:
    lock_data = _read_lock_data(lock_path)
    pid: int | None
    try:
        pid = int(lock_data.get("pid"))
        if pid <= 0:
            pid = None
    except Exception:
        pid = None

    started_at = _from_iso(lock_data.get("started_at"))
    if started_at is None:
        ts = _read_lock_start_ts(lock_path)
        if ts:
            started_at = datetime.fromtimestamp(ts)
    return pid, started_at


def _claim_or_clear_lock(lock_path: Path) -> bool:
    if not lock_path.exists():
        return True

    stale, pid = _is_stale_lock(lock_path)
    if stale:
        _log_stale_lock(lock_path, pid)
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            return False
        return True

    return False


def _acquire_run_lock() -> bool:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    lock_body = json.dumps(
        {
            "pid": os.getpid(),
            "timestamp": time.time(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
        },
        ensure_ascii=False,
    )

    # Migration safety: avoid overlap with old scheduler versions.
    if not _claim_or_clear_lock(LEGACY_LOCK_PATH):
        return False

    if not _claim_or_clear_lock(LOCK_PATH):
        return False

    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        if not _claim_or_clear_lock(LOCK_PATH):
            return False
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False

    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(lock_body)

    # Keep legacy file for older readers/automation during rollout.
    try:
        LEGACY_LOCK_PATH.write_text(lock_body, encoding="utf-8")
    except Exception:
        pass
    return True


def _release_run_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        LEGACY_LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _read_last_json_line(path: Path, *, filter_status_only: bool = False) -> dict | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return None

    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if filter_status_only and payload.get("status") not in {"SUCCESS", "FAILED"}:
            continue
        return payload
    return None


def _read_output_metrics(output_path: Path) -> dict:
    if not output_path.exists():
        return {
            "output_last_updated": None,
            "output_size": 0,
            "articles_count": 0,
        }

    try:
        stats = output_path.stat()
        output_last_updated = _to_iso(datetime.fromtimestamp(stats.st_mtime))
        output_size = int(stats.st_size)
    except Exception:
        output_last_updated = None
        output_size = 0

    articles_count = 0
    try:
        raw = json.loads(output_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            articles_count = len(raw)
        elif isinstance(raw, dict):
            items = raw.get("articles")
            if isinstance(items, list):
                articles_count = len(items)
    except Exception:
        articles_count = 0

    return {
        "output_last_updated": output_last_updated,
        "output_size": output_size,
        "articles_count": articles_count,
    }


def get_pipeline_status() -> dict:
    selected_lock = LOCK_PATH if LOCK_PATH.exists() else LEGACY_LOCK_PATH
    lock_exists = selected_lock.exists()
    pid, started_at = _lock_owner_info(selected_lock) if lock_exists else (None, None)
    pid_alive = _is_pid_alive(pid)
    stale_lock = lock_exists and not pid_alive

    last_run = _read_last_json_line(RUN_LOG_PATH, filter_status_only=True) or {}
    last_error = _read_last_json_line(ERROR_LOG_PATH) or {}
    output_metrics = _read_output_metrics(OUTPUT_PATH)

    error_message = None
    if stale_lock:
        error_message = f"stale lock detected: {selected_lock} (pid={pid})"
    elif isinstance(last_error.get("error"), str) and last_error.get("error"):
        error_message = last_error["error"]

    return {
        "is_running": bool(lock_exists and pid_alive),
        "pid": pid if pid_alive else None,
        "started_at": _to_iso(started_at) if pid_alive else None,
        "last_run_status": last_run.get("status"),
        "last_run_time": last_run.get("end_timestamp"),
        "output_last_updated": output_metrics["output_last_updated"],
        "output_size": output_metrics["output_size"],
        "articles_count": output_metrics["articles_count"],
        "error": error_message,
    }


def _run_once(target_articles: int) -> None:
    if not _acquire_run_lock():
        print("Another run is already in progress. Skipping this trigger.")
        return

    started_at = datetime.now()
    try:
        from news_fetcher import fetch_valid_news

        print("Fetching news...")
        news = fetch_valid_news(target=target_articles)
        print(f"Fetched {len(news)} articles")
        _save_json(news)
        _log_run(started_at, datetime.now(), "SUCCESS")
    except Exception as exc:
        _log_error(exc)
        _log_run(started_at, datetime.now(), "FAILED", error=str(exc))
        raise
    finally:
        _release_run_lock()


def _run_loop(interval_seconds: int, target_articles: int) -> None:
    while True:
        started_at = time.time()
        try:
            _run_once(target_articles=target_articles)
        except Exception as exc:
            print(f"Error: {exc}")

        print("Sleeping for 30 minutes..." if interval_seconds == 1800 else f"Sleeping for {interval_seconds} seconds...")
        elapsed = time.time() - started_at
        remaining = interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch and cache news into cache/latest.json.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run once and exit (recommended for server cron).")
    mode.add_argument("--loop", action="store_true", help="Run continuously and sleep between runs.")
    mode.add_argument("--status", action="store_true", help="Show pipeline status and exit.")
    parser.add_argument("--interval", type=int, default=DEFAULT_SLEEP_SECONDS, help="Loop interval in seconds.")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET_ARTICLES, help="Target number of articles.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.status:
        print(json.dumps(get_pipeline_status(), ensure_ascii=False, indent=2))
        return 0

    run_loop = args.loop
    try:
        if run_loop:
            _run_loop(interval_seconds=max(1, int(args.interval)), target_articles=max(1, int(args.target)))
        else:
            _run_once(target_articles=max(1, int(args.target)))
    except KeyboardInterrupt:
        print("Stopped by user.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
