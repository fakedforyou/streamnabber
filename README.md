# streamnabber

Dockerized Flask/SQLite application that persistently monitors saved URLs
and starts yt-dlp whenever an enabled URL is not already being downloaded.

## Features

- Add URLs from a web interface.
- Optional model name; when omitted, a name is derived from the URL.
- SQLite persistence.
- One active yt-dlp worker per URL.
- Automatic periodic checking.
- Automatic retry after errors.
- Re-check after successful downloads, useful for persistent/live URLs.
- Progress, speed, ETA, status, attempt count and last successful download.
- Enable/disable URLs.
- Remove URLs.
- Stale-worker watchdog.
- Container restart recovery.
- Persistent `/data` and `/downloads` Docker volumes.

## Start

```bash
docker compose up -d --build
```

Open:

```text
http://SERVER-IP:5000
```

The Compose file publishes container port 5000 as host port 5000.

## Stop

```bash
docker compose down
```

## Storage

- Database: `./data/monitor.db`
- Downloads: `./downloads/<model name>/`

Both directories are bind-mounted, so their contents survive container replacement.

## Monitoring logic

Every `CHECK_INTERVAL` seconds the supervisor finds enabled URLs whose
`next_check_at` has arrived. Before starting yt-dlp it atomically changes
the SQLite state to `downloading`. If the same URL is already claimed,
the DB update affects zero rows, preventing a duplicate worker.

After a successful yt-dlp run, `last_download_at` is updated and the URL
returns to `waiting`. It becomes eligible again after `RECHECK_INTERVAL`.

After a failure, the URL enters `retry_wait` and is retried after
`RETRY_INTERVAL`.

`STALE_INTERVAL` is a watchdog threshold. A DB record that says
`downloading` but is no longer owned by a live worker will be re-queued.

## Environment settings

| Setting | Default | Meaning |
|---|---:|---|
| `CHECK_INTERVAL` | 10 | Main supervisor polling interval, seconds |
| `RECHECK_INTERVAL` | 60 | Delay after a successful download before trying the URL again |
| `RETRY_INTERVAL` | 60 | Delay after a yt-dlp error |
| `STALE_INTERVAL` | 180 | Lost-heartbeat threshold |
| `MAX_CONCURRENT_DOWNLOADS` | 100 | Maximum different URLs downloading concurrently. This can be changed to suit your environment.  |

## Important behavior

* To lock the resolution edit app.py. For example, to set it to something other than 720p or less modify the download_sources area
  
* This is a persistent monitor, not a one-shot queue. An enabled URL remains
in the database after a successful download and will be tried again later.

* Disabling a URL prevents future starts. It does not forcibly terminate an
yt-dlp process/thread that is already running.

* Keep Gunicorn at one worker unless the supervisor is moved into a separate
service or replaced by a distributed job queue.

* If exposing this beyond a trusted LAN, put it behind authentication and TLS.

* Only download content you are authorized to download.

* You can edit the list offline by using a DB Browser for SQLite or similar

* This application is useful for Bongocam and Chaturbate. 

* Only use this app for streams you are authorized to capture.
