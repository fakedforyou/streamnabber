import os
import re
import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from flask import Flask, flash, redirect, render_template, request, url_for
from yt_dlp import YoutubeDL


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", BASE_DIR / "downloads"))
DB_PATH = Path(os.getenv("DB_PATH", DATA_DIR / "monitor.db"))

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "10"))
RECHECK_INTERVAL = int(os.getenv("RECHECK_INTERVAL", "60"))
RETRY_INTERVAL = int(os.getenv("RETRY_INTERVAL", "60"))
STALE_INTERVAL = int(os.getenv("STALE_INTERVAL", "180"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "replace-me")

executor = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_DOWNLOADS,
    thread_name_prefix="yt-dlp"
)
active_downloads = {}
active_lock = threading.Lock()
stop_event = threading.Event()


def now():
    return datetime.now(timezone.utc)


def iso(dt=None):
    return (dt or now()).isoformat()


def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                model_name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'waiting',
                progress REAL NOT NULL DEFAULT 0,
                speed TEXT,
                eta TEXT,
                filename TEXT,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                last_download_at TEXT,
                heartbeat_at TEXT,
                next_check_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sources_ready
                ON sources(enabled, status, next_check_at);
        """)


def recover_after_restart():
    # No worker from a previous container/process can still belong to this process.
    with db() as conn:
        conn.execute("""
            UPDATE sources
               SET status='waiting',
                   heartbeat_at=NULL,
                   next_check_at=?,
                   updated_at=?,
                   error=CASE
                       WHEN status='downloading'
                       THEN 'Application restarted while download was active; source re-queued.'
                       ELSE error
                   END
             WHERE status='downloading'
        """, (iso(), iso()))


def valid_url(value):
    try:
        p = urlparse(value)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def derive_model_name(url):
    """
    Reasonable generic default: use the last meaningful URL path component.
    The user can override it in the UI.
    """
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    if parts:
        name = parts[-1]
        name = re.sub(r"\.[A-Za-z0-9]{2,5}$", "", name)
        return name[:100]
    return p.netloc[:100]


def human_speed(value):
    if not value:
        return None
    value = float(value)
    units = ("B/s", "KB/s", "MB/s", "GB/s")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024


def human_eta(value):
    if value is None:
        return None
    try:
        sec = int(value)
    except Exception:
        return None
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m" if h else (f"{m}m {s}s" if m else f"{s}s")


def update_source(source_id, **fields):
    if not fields:
        return
    fields["updated_at"] = iso()
    clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [source_id]
    with db() as conn:
        conn.execute(f"UPDATE sources SET {clause} WHERE id=?", values)


def claim_source(source_id):
    """
    Atomic DB-level lock. Only one caller can change a ready source to
    'downloading'; all other callers get rowcount == 0.
    """
    stamp = iso()
    with db() as conn:
        cur = conn.execute("""
            UPDATE sources
               SET status='downloading',
                   attempts=attempts+1,
                   progress=0,
                   speed=NULL,
                   eta=NULL,
                   error=NULL,
                   started_at=?,
                   heartbeat_at=?,
                   updated_at=?
             WHERE id=?
               AND enabled=1
               AND status IN ('waiting', 'retry_wait')
               AND (next_check_at IS NULL OR next_check_at <= ?)
        """, (stamp, stamp, stamp, source_id, stamp))
        return cur.rowcount == 1


def progress_hook(source_id):
    last_write = {"at": 0.0}

    def hook(data):
        # Throttle DB updates while preserving a current heartbeat.
        if data.get("status") == "downloading":
            t = time.monotonic()
            if t - last_write["at"] < 1:
                return
            last_write["at"] = t

        downloaded = data.get("downloaded_bytes") or 0
        total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
        percent = (downloaded / total * 100) if total else 0

        filename = data.get("filename")
        if filename:
            try:
                filename = str(Path(filename).resolve().relative_to(DOWNLOAD_DIR.resolve()))
            except Exception:
                filename = Path(filename).name

        update_source(
            source_id,
            progress=round(percent, 2),
            speed=human_speed(data.get("speed")),
            eta=human_eta(data.get("eta")),
            heartbeat_at=iso(),
            filename=filename,
        )

    return hook


def download_source(source_id, url, model_name):
    try:
        target_dir = DOWNLOAD_DIR / re.sub(r"[^A-Za-z0-9._ -]+", "_", model_name)
        target_dir.mkdir(parents=True, exist_ok=True)

        opts = {
            "paths": {"home": str(target_dir)},
            # Epoch allows the same persistent/live URL to produce a new file later.
            "outtmpl": "%(title).160B [%(id)s] [%(epoch)s].%(ext)s",
            "continuedl": True,
            "overwrites": False,
            "retries": 10,
            "fragment_retries": 10,
            "file_access_retries": 3,
            "progress_hooks": [progress_hook(source_id)],
            "quiet": True,
            "no_warnings": False,
        }

        with YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

        # Keep the URL in the database. It becomes eligible again after
        # RECHECK_INTERVAL so persistent/live URLs are monitored continuously.
        update_source(
            source_id,
            status="waiting",
            progress=100,
            speed=None,
            eta=None,
            last_download_at=iso(),
            heartbeat_at=iso(),
            next_check_at=iso(now() + timedelta(seconds=RECHECK_INTERVAL)),
            error=None,
        )

    except Exception as exc:
        update_source(
            source_id,
            status="retry_wait",
            speed=None,
            eta=None,
            heartbeat_at=iso(),
            next_check_at=iso(now() + timedelta(seconds=RETRY_INTERVAL)),
            error=f"{type(exc).__name__}: {exc}",
        )
        traceback.print_exc()

    finally:
        with active_lock:
            active_downloads.pop(source_id, None)


def submit(source):
    source_id = source["id"]
    with active_lock:
        current = active_downloads.get(source_id)
        if current and not current.done():
            return False

        if not claim_source(source_id):
            return False

        future = executor.submit(
            download_source,
            source_id,
            source["url"],
            source["model_name"],
        )
        active_downloads[source_id] = future
        return True


def recover_stale():
    cutoff = iso(now() - timedelta(seconds=STALE_INTERVAL))

    with active_lock:
        live_ids = {
            source_id for source_id, future in active_downloads.items()
            if not future.done()
        }

    with db() as conn:
        rows = conn.execute("""
            SELECT id FROM sources
             WHERE status='downloading'
               AND (heartbeat_at IS NULL OR heartbeat_at < ?)
        """, (cutoff,)).fetchall()

        for row in rows:
            if row["id"] not in live_ids:
                conn.execute("""
                    UPDATE sources
                       SET status='waiting',
                           heartbeat_at=NULL,
                           next_check_at=?,
                           updated_at=?,
                           error='Lost download heartbeat; automatically re-queued.'
                     WHERE id=? AND status='downloading'
                """, (iso(), iso(), row["id"]))


def start_ready_sources():
    with active_lock:
        running = sum(1 for f in active_downloads.values() if not f.done())
    slots = max(0, MAX_CONCURRENT_DOWNLOADS - running)
    if not slots:
        return

    stamp = iso()
    with db() as conn:
        rows = conn.execute("""
            SELECT id, url, model_name
              FROM sources
             WHERE enabled=1
               AND status IN ('waiting', 'retry_wait')
               AND (next_check_at IS NULL OR next_check_at <= ?)
             ORDER BY COALESCE(next_check_at, created_at), created_at
             LIMIT ?
        """, (stamp, slots)).fetchall()

    for source in rows:
        submit(source)


def supervisor():
    while not stop_event.is_set():
        try:
            recover_stale()
            start_ready_sources()
        except Exception:
            traceback.print_exc()
        stop_event.wait(CHECK_INTERVAL)


@app.template_filter("localtime")
def localtime(value):
    if not value:
        return "Never"
    try:
        return datetime.fromisoformat(value).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value


@app.get("/")
def index():
    with db() as conn:
        sources = conn.execute("""
            SELECT * FROM sources
            ORDER BY enabled DESC,
                     CASE status
                       WHEN 'downloading' THEN 0
                       WHEN 'retry_wait' THEN 1
                       ELSE 2
                     END,
                     model_name COLLATE NOCASE
        """).fetchall()
    return render_template(
        "index.html",
        sources=sources,
        check_interval=CHECK_INTERVAL,
        recheck_interval=RECHECK_INTERVAL,
        max_concurrent=MAX_CONCURRENT_DOWNLOADS,
    )


@app.post("/sources")
def add_source():
    url = request.form.get("url", "").strip()
    model_name = request.form.get("model_name", "").strip()

    if not valid_url(url):
        flash("Enter a valid http:// or https:// URL.", "error")
        return redirect(url_for("index"))

    if not model_name:
        model_name = derive_model_name(url)

    stamp = iso()
    try:
        with db() as conn:
            conn.execute("""
                INSERT INTO sources
                    (url, model_name, enabled, status, created_at, updated_at, next_check_at)
                VALUES (?, ?, 1, 'waiting', ?, ?, ?)
            """, (url, model_name[:100], stamp, stamp, stamp))
        flash("URL added and scheduled for download.", "success")
    except sqlite3.IntegrityError:
        flash("That URL is already being monitored.", "error")

    return redirect(url_for("index"))


@app.post("/sources/<int:source_id>/toggle")
def toggle_source(source_id):
    with db() as conn:
        row = conn.execute("SELECT enabled, status FROM sources WHERE id=?", (source_id,)).fetchone()
        if row:
            new_value = 0 if row["enabled"] else 1
            conn.execute("""
                UPDATE sources
                   SET enabled=?,
                       status=CASE
                           WHEN ?=1 AND status!='downloading' THEN 'waiting'
                           ELSE status
                       END,
                       next_check_at=CASE WHEN ?=1 THEN ? ELSE next_check_at END,
                       updated_at=?
                 WHERE id=?
            """, (new_value, new_value, new_value, iso(), iso(), source_id))
    return redirect(url_for("index"))


@app.post("/sources/<int:source_id>/retry")
def retry_source(source_id):
    update_source(source_id, status="waiting", error=None, next_check_at=iso())
    return redirect(url_for("index"))


@app.post("/sources/<int:source_id>/delete")
def delete_source(source_id):
    with active_lock:
        future = active_downloads.get(source_id)
        if future and not future.done():
            flash("This URL is actively downloading. Disable it first; remove it after the current yt-dlp run ends.", "error")
            return redirect(url_for("index"))

    with db() as conn:
        conn.execute("DELETE FROM sources WHERE id=?", (source_id,))
    flash("URL removed.", "success")
    return redirect(url_for("index"))


@app.get("/health")
def health():
    return {"status": "ok"}, 200


init_db()
recover_after_restart()
threading.Thread(target=supervisor, name="supervisor", daemon=True).start()


if __name__ == "__main__":
    app.run(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "5000")),
        threaded=True,
        debug=False,
    )
