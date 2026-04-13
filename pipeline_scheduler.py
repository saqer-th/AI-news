import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path


DEFAULT_SLEEP_SECONDS = 1800
DEFAULT_TARGET_ARTICLES = 35
CACHE_DIR = Path("cache")
LATEST_PATH = CACHE_DIR / "latest.json"
LOCK_PATH = CACHE_DIR / ".scheduler.lock"
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


def _read_lock_start_ts() -> float:
    try:
        lock_raw = LOCK_PATH.read_text(encoding="utf-8").strip()
        lock_data = json.loads(lock_raw) if lock_raw else {}
        return float(lock_data.get("timestamp", 0.0) or 0.0)
    except Exception:
        return 0.0


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

    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        started = _read_lock_start_ts()
        if started and (time.time() - started) > STALE_LOCK_SECONDS:
            try:
                LOCK_PATH.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                return False
        else:
            return False

    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(lock_body)
    return True


def _release_run_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _run_once(target_articles: int) -> None:
    if not _acquire_run_lock():
        print("Another run is already in progress. Skipping this trigger.")
        return

    try:
        from news_fetcher import fetch_valid_news

        print("Fetching news...")
        news = fetch_valid_news(target=target_articles)
        print(f"Fetched {len(news)} articles")
        _save_json(news)
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
    parser.add_argument("--interval", type=int, default=DEFAULT_SLEEP_SECONDS, help="Loop interval in seconds.")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET_ARTICLES, help="Target number of articles.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

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
