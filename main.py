"""
================================================================================
 Neura Stream — Prime-style streaming platform (single file, zero API keys)
 Cinematic browse UI: hero banner, Prime-style shelves, YouTube content via
 keyless ytsearch, reels (YouTube Shorts + Instagram/Facebook/X links),
 own uploads, multi-platform extraction and an AI co-pilot.
 Built to run on Render's free tier (512 MB RAM).
================================================================================

 DEPLOYMENT (Render):
   Build command : pip install -r requirements.txt
   Start command : python main.py
   Instance size  : Free (512 MB)

 requirements.txt
 ----------------
    fastapi==0.115.6
    uvicorn==0.32.1
    yt-dlp>=2025.1.1
    duckduckgo-search>=7.0.0
    httpx>=0.27.0
    python-multipart>=0.0.9

 NOTE ON STORAGE (free tier): disk is ephemeral — uploads + SQLite live in the
 app folder and reset on redeploy/restart. Everything else keeps working.

 OPTIONAL (free): set YOUTUBE_API_KEY (YouTube Data API v3, free 10k units/day)
 from console.cloud.google.com and the browse shelves upgrade to the official
 API with zero code changes. Without it, everything stays 100% keyless.

 UPLOADS: local disk by default (ephemeral). Set SUPABASE_URL +
 SUPABASE_SERVICE_KEY (+ SUPABASE_BUCKET, default "uploads") and videos up to
 500 MB are stored in a free Supabase cloud bucket — permanent, restart-safe.

 NO PAID SERVICES. NO BUILD STEP. ONE FILE.
================================================================================
"""

import asyncio
import base64
import hashlib
import mimetypes
import os
import re
import secrets
import smtplib
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
import uvicorn
import yt_dlp
try:  # package was renamed: works with both `duckduckgo-search` and `ddgs`
    from duckduckgo_search import DDGS
except ImportError:  # pragma: no cover
    from ddgs import DDGS
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
#  Application                                                                 #
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="Neura Stream",
    description="Hybrid video platform: uploads, reels, multi-platform extraction, "
    "live web search and an AI co-pilot — zero API keys.",
    version="7.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
THUMB_DIR = BASE_DIR / "thumbs"
DB_PATH = BASE_DIR / "neura.db"
UPLOAD_DIR.mkdir(exist_ok=True)
THUMB_DIR.mkdir(exist_ok=True)

# Storage: local disk (ephemeral) OR Supabase Storage (free 1GB, permanent).
# Set SUPABASE_URL + SUPABASE_SERVICE_KEY env vars and uploads go to the cloud
# bucket instead of the code container — up to 500 MB per video, restart-safe.
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "uploads")
MAX_UPLOAD_LOCAL = 200 * 1024 * 1024    # when stored in the container
MAX_UPLOAD_CLOUD = 500 * 1024 * 1024   # when stored in Supabase
MAX_UPLOAD_BYTES = MAX_UPLOAD_CLOUD if (SUPABASE_URL and SUPABASE_KEY) else MAX_UPLOAD_LOCAL

YDL_OPTS: Dict[str, Any] = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "skip_download": True,
    "socket_timeout": 20,
    "retries": 2,
    "nocheckcertificate": True,
    "cachedir": False,
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    "http_headers": {"User-Agent": USER_AGENT},
}

# Hosts the media proxy is allowed to fetch from (SSRF guard).
ALLOWED_PROXY_SUFFIXES = (
    "googlevideo.com",
    "cdninstagram.com",
    "fbcdn.net",
    "twimg.com",
    "akamaized.net",
    "cloudfront.net",
    "ytimg.com",
    "googleusercontent.com",
    "jwpcdn.com",
    "streamcraft.net",
)

# Fallback ladder of YouTube player clients (retry once on bot-check).
YDL_CLIENT_LADDERS = [
    {"youtube": {"player_client": ["android", "web"]}},
    {"youtube": {"player_client": ["tv", "web_safari"]}},
]

PLATFORM_MATCHERS = [
    ("whatsapp", r"(wa\.me|whatsapp\.com)"),
    ("youtube", r"(youtube\.com|youtu\.be)"),
    ("instagram", r"instagram\.com"),
    ("facebook", r"(facebook\.com|fb\.watch|fb\.com)"),
    ("x", r"(twitter\.com|(^|//)x\.com)"),
    ("telegram", r"(t\.me|telegram\.me)"),
]


def detect_platform(url: str) -> Optional[str]:
    for name, pattern in PLATFORM_MATCHERS:
        if re.search(pattern, url, re.I):
            return name
    return None


# --------------------------------------------------------------------------- #
#  Local platform storage (SQLite)                                             #
# --------------------------------------------------------------------------- #

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id          TEXT PRIMARY KEY,
                title       TEXT NOT NULL,
                description TEXT DEFAULT '',
                uploader    TEXT DEFAULT 'Guest Creator',
                filename    TEXT NOT NULL,
                thumb       TEXT,
                duration    REAL DEFAULT 0,
                views       INTEGER DEFAULT 0,
                likes       INTEGER DEFAULT 0,
                created_at  REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS comments (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id   TEXT NOT NULL,
                author     TEXT DEFAULT 'Guest',
                body       TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                email      TEXT UNIQUE NOT NULL,
                name       TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS otp_codes (
                email      TEXT PRIMARY KEY,
                code_hash  TEXT NOT NULL,
                attempts   INTEGER DEFAULT 0,
                expires_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id  INTEGER NOT NULL,
                receiver_id INTEGER NOT NULL,
                body       TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            """
        )


init_db()


def _video_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "description": row["description"],
        "uploader": row["uploader"],
        "kind": "local",
        "filename": row["filename"],
        "thumb": f"/thumb/{row['thumb']}" if row["thumb"] else None,
        "duration": row["duration"],
        "duration_label": _fmt_duration(row["duration"]),
        "views": row["views"],
        "likes": row["likes"],
        "created_at": row["created_at"],
        "webpage_url": row["filename"] if row["filename"].startswith("http") else f"/media/{row['filename']}",
        "stored": "cloud" if row["filename"].startswith("http") else "local",
        "extractor": "Neura upload",
    }


@app.get("/api/videos")
def api_list_videos(limit: int = Query(48, le=100), offset: int = 0):
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM videos ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM videos").fetchone()["c"]
    return {"videos": [_video_row_to_dict(r) for r in rows], "total": total}


@app.get("/api/videos/{vid}")
def api_get_video(vid: str):
    with _db() as conn:
        conn.execute("UPDATE videos SET views = views + 1 WHERE id = ?", (vid,))
        row = conn.execute("SELECT * FROM videos WHERE id = ?", (vid,)).fetchone()
    if not row:
        return {"error": "not_found",
                "message": "That video doesn't exist (a server restart may have cleared uploads)."}
    return _video_row_to_dict(row)


@app.post("/api/videos/{vid}/like")
def api_like_video(vid: str):
    with _db() as conn:
        cur = conn.execute("UPDATE videos SET likes = likes + 1 WHERE id = ?", (vid,))
        if cur.rowcount == 0:
            return {"error": "not_found", "message": "Video not found."}
        row = conn.execute("SELECT likes FROM videos WHERE id = ?", (vid,)).fetchone()
    return {"ok": True, "likes": row["likes"]}


@app.get("/api/videos/{vid}/comments")
def api_list_comments(vid: str):
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM comments WHERE video_id = ? ORDER BY created_at DESC LIMIT 200",
            (vid,),
        ).fetchall()
    return {"comments": [
        {"id": r["id"], "author": r["author"], "body": r["body"], "created_at": r["created_at"]}
        for r in rows
    ]}


class CommentIn(BaseModel):
    author: str = Field("Guest", max_length=40)
    body: str = Field(..., min_length=1, max_length=1000)


@app.post("/api/videos/{vid}/comments")
def api_add_comment(vid: str, payload: CommentIn, request: Request):
    with _db() as conn:
        is_external = vid.startswith("yt:")
        if not is_external:
            exists = conn.execute("SELECT 1 FROM videos WHERE id = ?", (vid,)).fetchone()
            if not exists:
                return {"error": "not_found", "message": "Video not found."}
        conn.execute(
            "INSERT INTO comments (video_id, author, body, created_at) VALUES (?, ?, ?, ?)",
            (vid, (_current_user(request) or {}).get("name") or payload.author.strip() or "Guest",
             payload.body.strip(), time.time()),
        )
    return {"ok": True}


# --------------------------- upload ---------------------------------------- #

def _save_data_url_thumbnail(data_url: str, vid: str) -> Optional[str]:
    """Persist a browser-captured 'data:image/...;base64,...' poster frame."""
    try:
        if not data_url.startswith("data:image/"):
            return None
        meta, b64 = data_url.split(",", 1)
        ext = "png" if "png" in meta else "jpg"
        raw = base64.b64decode(b64)
        if len(raw) > 2 * 1024 * 1024:
            return None
        name = f"{vid}.{ext}"
        (THUMB_DIR / name).write_bytes(raw)
        return name
    except Exception:
        return None


async def _supabase_upload(path: Path, name: str, ctype: str) -> Optional[str]:
    """PUT a local temp file into the Supabase storage bucket; return public URL."""
    try:
        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "apikey": SUPABASE_KEY,
            "Content-Type": ctype or "video/mp4",
            "x-upsert": "true",
        }
        async with httpx.AsyncClient(timeout=600.0) as client:
            with path.open("rb") as fh:
                r = await client.put(
                    f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{name}",
                    headers=headers, content=fh,
                )
        if r.status_code in (200, 201):
            return f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{name}"
        return None
    except Exception:
        return None


@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(""),
    uploader: str = Form(""),
    request: Request = None,
    duration: float = Form(0.0),
    thumbnail: str = Form(""),
):
    title = title.strip()[:150] or file.filename or "Untitled upload"
    session_user = _current_user(request) if request else None
    uploader = (uploader.strip()[:60]) or (session_user or {}).get("name") or "Guest Creator"
    description = description.strip()[:2000]

    if file.content_type and not file.content_type.startswith("video/"):
        return {"error": "bad_type", "message": "Only video files are supported."}

    vid = uuid.uuid4().hex[:12]
    ext = Path(file.filename or "video.mp4").suffix.lower() or ".mp4"
    if not re.fullmatch(r"\.[a-z0-9]{2,5}", ext):
        ext = ".mp4"
    dest = UPLOAD_DIR / f"{vid}{ext}"
    written = 0
    limit_mb = "500" if (SUPABASE_URL and SUPABASE_KEY) else "200"
    try:
        with dest.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)  # 1 MB chunks -> flat RAM
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413,
                                        detail=f"File exceeds the {limit_mb} MB limit.")
                out.write(chunk)
    except HTTPException:
        dest.unlink(missing_ok=True)
        raise
    except Exception as exc:
        dest.unlink(missing_ok=True)
        return {"error": "write_failed", "message": f"Upload failed: {str(exc)[:200]}"}
    finally:
        await file.close()

    if written == 0:
        dest.unlink(missing_ok=True)
        return {"error": "empty", "message": "The selected file was empty."}

    stored_name = dest.name
    if SUPABASE_URL and SUPABASE_KEY:
        # push to external cloud storage (permanent), then drop the local copy
        remote = await _supabase_upload(dest, dest.name, ctype="video/mp4")
        dest.unlink(missing_ok=True)
        if remote:
            stored_name = remote

    thumb_name = _save_data_url_thumbnail(thumbnail, vid)
    with _db() as conn:
        conn.execute(
            "INSERT INTO videos (id, title, description, uploader, filename, thumb, duration, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (vid, title, description, uploader, stored_name, thumb_name, float(duration or 0), time.time()),
        )
    with _db() as conn:
        row = conn.execute("SELECT * FROM videos WHERE id = ?", (vid,)).fetchone()
    return {"ok": True, "video": _video_row_to_dict(row)}


# --------------------------- media serving (Range-aware) -------------------- #

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def _stream_local_file(path: Path, range_header: Optional[str], content_type: str) -> StreamingResponse:
    size = path.stat().st_size
    if range_header:
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if m:
            start = int(m.group(1) or 0)
            end = min(int(m.group(2) or size - 1), size - 1)
            if start > end or start >= size:
                return StreamingResponse(iter(()), status_code=416)
            length = end - start + 1

            def gen():
                with path.open("rb") as f:
                    f.seek(start)
                    remaining = length
                    while remaining > 0:
                        chunk = f.read(min(65536, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk

            return StreamingResponse(
                gen(), status_code=206, media_type=content_type,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{size}",
                    "Content-Length": str(length),
                    "Accept-Ranges": "bytes",
                },
            )
    return StreamingResponse(
        path.open("rb"), media_type=content_type,
        headers={"Accept-Ranges": "bytes", "Content-Length": str(size)},
    )


@app.get("/media/{filename}")
def api_media(filename: str, request: Request):
    if not _SAFE_NAME.fullmatch(filename):
        raise HTTPException(status_code=400, detail="Bad filename")
    path = UPLOAD_DIR / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Media not found (server restart may have cleared uploads).")
    ctype = mimetypes.guess_type(filename)[0] or "video/mp4"
    return _stream_local_file(path, request.headers.get("range"), ctype)


@app.get("/thumb/{filename}")
def api_thumb(filename: str):
    if not _SAFE_NAME.fullmatch(filename):
        raise HTTPException(status_code=400, detail="Bad filename")
    path = THUMB_DIR / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="No thumbnail")
    ctype = mimetypes.guess_type(filename)[0] or "image/png"
    return _stream_local_file(path, None, ctype)


# --------------------------------------------------------------------------- #
#  Multi-platform extraction engine (yt-dlp, download=False)                    #
# --------------------------------------------------------------------------- #

def _pick_streams(info: Dict[str, Any]) -> Dict[str, Any]:
    formats: List[Dict[str, Any]] = info.get("formats") or []
    progressive, video_only, audio_only = [], [], []
    for f in formats:
        url = f.get("url")
        if not url:
            continue
        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        has_video, has_audio = vcodec != "none", acodec != "none"
        height = f.get("height") or 0
        ext = (f.get("ext") or "").lower()
        entry = {
            "id": f.get("format_id"), "url": url, "ext": ext,
            "mime": f.get("mime_type") or ("video/mp4" if has_video else "audio/mp4"),
            "vcodec": vcodec, "acodec": acodec,
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "progressive": has_video and has_audio,
        }
        if has_video and has_audio:
            entry.update(label=f"{height or '?'}p" + (" HD" if height and height >= 720 else ""), quality=height or 1)
            progressive.append(entry)
        elif has_video:
            entry.update(label=f"{height or '?'}p (video)", quality=height or 1)
            video_only.append(entry)
        else:
            entry.update(label=f"audio {f.get('abr') or '?'}kbps {ext.upper()}", quality=int(f.get("abr") or 0))
            audio_only.append(entry)

    if not progressive and not video_only and not audio_only and info.get("url"):
        progressive.append({
            "id": info.get("format_id") or "0", "url": info["url"],
            "ext": (info.get("ext") or "mp4").lower(), "mime": info.get("mime_type") or "video/mp4",
            "vcodec": info.get("vcodec") or "unknown", "acodec": info.get("acodec") or "unknown",
            "filesize": info.get("filesize"), "progressive": True, "label": "source",
            "quality": info.get("height") or 1,
        })

    progressive.sort(key=lambda f: f["quality"], reverse=True)
    video_only.sort(key=lambda f: f["quality"], reverse=True)
    audio_only.sort(key=lambda f: f["quality"], reverse=True)
    streams = (progressive + video_only + audio_only)[:18]
    return {
        "streams": streams,
        "best": progressive[0] if progressive else (video_only[0] if video_only else (audio_only[0] if audio_only else None)),
        "audio": audio_only[0] if audio_only else None,
    }


def _fmt_duration(seconds: Optional[float]) -> str:
    try:
        seconds = int(seconds or 0)
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        return "0:00"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _extract_media_sync(url: str) -> Dict[str, Any]:
    info: Optional[Dict[str, Any]] = None
    last_error: Optional[Exception] = None
    for client_args in YDL_CLIENT_LADDERS:
        opts = dict(YDL_OPTS)
        opts["extractor_args"] = client_args
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            break
        except yt_dlp.utils.DownloadError as exc:
            last_error = exc
            info = None
    if info is None:
        raise last_error or RuntimeError("Extraction produced no data.")

    if not info:
        raise RuntimeError("Extractor returned no data for this URL.")
    if "entries" in info:
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise RuntimeError("This playlist/link contains no playable media.")
        info = entries[0]

    picks = _pick_streams(info)
    duration = info.get("duration") or 0
    payload = {
        "title": info.get("title") or "Untitled media",
        "webpage_url": info.get("webpage_url") or info.get("url") or url,
        "extractor": (info.get("extractor_key") or info.get("extractor") or detect_platform(url) or "generic").title(),
        "uploader": info.get("uploader") or info.get("channel") or info.get("uploader_id") or "unknown",
        "duration": duration,
        "duration_label": _fmt_duration(duration),
        "thumbnail": info.get("thumbnail"),
        "is_live": bool(info.get("is_live")),
        "view_count": info.get("view_count") or info.get("like_count"),
        "description": (info.get("description") or info.get("title") or "")[:2000],
        "streams": picks["streams"],
        "best": picks["best"],
        "audio": picks["audio"],
        "kind": "remote",
    }
    if not payload["best"]:
        raise RuntimeError("No playable stream format was found for this media.")
    return payload


WHATSAPP_MESSAGE = (
    "WhatsApp statuses and chats are end-to-end encrypted and need a logged-in "
    "account, so no open extractor can pull media from them. Try a public "
    "YouTube / Instagram / X / Facebook / Telegram link instead."
)


@app.get("/api/stream")
async def api_stream(url: str = Query(..., min_length=8)):
    """Extract direct ad-free CDN stream URLs from any supported platform link."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return {"error": "invalid_url", "message": "Please provide a valid http(s) media URL."}
    if detect_platform(url) == "whatsapp":
        return {"error": "unsupported_platform", "message": WHATSAPP_MESSAGE}
    try:
        payload = await asyncio.to_thread(_extract_media_sync, url)
        return payload
    except yt_dlp.utils.DownloadError as exc:
        msg = str(exc).replace("ERROR:", "").strip()[:400]
        if "Sign in to confirm" in msg or "not a bot" in msg:
            msg = ("The platform is bot-checking this server's IP for this video. "
                   "Wait a minute and retry, or try a different link.")
        return {"error": "extraction_failed", "message": msg or "The media could not be extracted."}
    except Exception as exc:
        return {"error": "extraction_failed", "message": f"Extraction failed: {str(exc)[:300]}"}


# --------------------------------------------------------------------------- #
#  Zero-API YouTube browse engine (yt-dlp ytsearch, extract_flat = fast)       #
# --------------------------------------------------------------------------- #

BROWSE_OPTS: Dict[str, Any] = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    "extract_flat": "in_playlist",  # metadata only — no per-video format fetch
    "socket_timeout": 15,
    "retries": 2,
    "nocheckcertificate": True,
    "cachedir": False,
    "http_headers": {"User-Agent": USER_AGENT},
}


def _browse_sync(query: str, count: int) -> List[Dict[str, Any]]:
    """Search YouTube via ytsearch{N}: — zero API keys, flat and fast."""
    with yt_dlp.YoutubeDL(dict(BROWSE_OPTS)) as ydl:
        info = ydl.extract_info(f"ytsearch{count}:{query}", download=False)
    out: List[Dict[str, Any]] = []
    for e in (info.get("entries") or []):
        if not e or not e.get("id"):
            continue
        thumbs = e.get("thumbnails") or []
        thumb = e.get("thumbnail") or (thumbs[-1]["url"] if thumbs else None)
        out.append({
            "title": e.get("title") or "Untitled",
            "url": f"https://www.youtube.com/watch?v={e['id']}",
            "thumbnail": thumb,
            "duration": e.get("duration") or 0,
            "duration_label": _fmt_duration(e.get("duration")),
            "view_count": e.get("view_count"),
            "uploader": e.get("channel") or e.get("uploader") or "YouTube",
            "kind": "youtube",
        })
    return out


# Optional: YouTube Data API v3 (FREE tier — 10,000 units/day).
# Get a key at console.cloud.google.com -> enable "YouTube Data API v3" -> Credentials -> API key.
# If the env var is not set, the app automatically uses the keyless yt-dlp ytsearch engine.
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

_ISO8601_DURATION = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def _parse_iso_duration(value: str) -> int:
    m = _ISO8601_DURATION.fullmatch(value or "")
    if not m:
        return 0
    h, mi, sec = (int(x or 0) for x in m.groups())
    return h * 3600 + mi * 60 + sec


async def _youtube_api_browse(query: str, count: int) -> List[Dict[str, Any]]:
    """YouTube Data API v3 search (~101 units/call): richer and bot-check-free."""
    import urllib.parse  # noqa: F401 (httpx handles encoding)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet", "type": "video", "maxResults": min(count, 50),
                "q": query, "key": YOUTUBE_API_KEY,
            },
        )
        r.raise_for_status()
        search_items = [it for it in r.json().get("items", []) if it.get("id", {}).get("videoId")]
        if not search_items:
            return []
        ids = [it["id"]["videoId"] for it in search_items]
        stats: Dict[str, Dict[str, Any]] = {}
        try:
            v = await client.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={"part": "contentDetails,statistics", "id": ",".join(ids), "key": YOUTUBE_API_KEY},
            )
            v.raise_for_status()
            stats = {it["id"]: it for it in v.json().get("items", [])}
        except Exception:
            pass  # durations/views are optional garnish
        items: List[Dict[str, Any]] = []
        for it in search_items:
            vid = it["id"]["videoId"]
            sn = it.get("snippet", {})
            st = stats.get(vid, {})
            duration = _parse_iso_duration((st.get("contentDetails") or {}).get("duration", ""))
            thumbs = sn.get("thumbnails") or {}
            thumb = (thumbs.get("maxres") or thumbs.get("high") or thumbs.get("medium")
                     or thumbs.get("default") or {}).get("url")
            try:
                views = int((st.get("statistics") or {}).get("viewCount") or 0) or None
            except (TypeError, ValueError):
                views = None
            items.append({
                "title": sn.get("title") or "Untitled",
                "url": f"https://www.youtube.com/watch?v={vid}",
                "thumbnail": thumb,
                "duration": duration,
                "duration_label": _fmt_duration(duration),
                "view_count": views,
                "uploader": sn.get("channelTitle") or "YouTube",
                "kind": "youtube",
            })
        return items


@app.get("/api/youtube/trending")
async def api_yt_trending(region: str = Query("IN", min_length=2, max_length=2), n: int = Query(24, ge=1, le=50)):
    """YouTube trending feed via the FREE Data API (chart=mostPopular, 1 unit/call).
    Falls back to keyless ytsearch when no API key is configured."""
    if YOUTUBE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    "https://www.googleapis.com/youtube/v3/videos",
                    params={
                        "part": "snippet,statistics,contentDetails", "chart": "mostPopular",
                        "regionCode": region.upper(), "maxResults": min(n, 50), "key": YOUTUBE_API_KEY,
                    },
                )
                r.raise_for_status()
                items = []
                for it in r.json().get("items", []):
                    dur = _parse_iso_duration((it.get("contentDetails") or {}).get("duration", ""))
                    sn = it.get("snippet", {})
                    thumbs = sn.get("thumbnails") or {}
                    thumb = (thumbs.get("maxres") or thumbs.get("high") or thumbs.get("medium")
                             or thumbs.get("default") or {}).get("url")
                    try:
                        views = int((it.get("statistics") or {}).get("viewCount") or 0) or None
                    except (TypeError, ValueError):
                        views = None
                    items.append({
                        "title": sn.get("title") or "Untitled",
                        "url": f"https://www.youtube.com/watch?v={it.get('id')}",
                        "thumbnail": thumb,
                        "duration": dur, "duration_label": _fmt_duration(dur),
                        "view_count": views,
                        "uploader": sn.get("channelTitle") or "YouTube",
                        "published_at": sn.get("publishedAt") or "",
                        "kind": "youtube",
                    })
                return {"items": items, "engine": "youtube-data-api-v3", "error": None}
        except Exception as exc:
            # quota/bad key -> keyless fallback below
            pass
    try:
        items = await asyncio.to_thread(_browse_sync, "trending videos today", n)
        return {"items": items, "engine": "ytsearch (keyless)", "error": None}
    except Exception as exc:
        return {"items": [], "engine": "none", "error": f"Trending failed: {str(exc)[:200]}"}


@app.get("/api/browse")
async def api_browse(q: str = Query(..., min_length=2), n: int = Query(24, ge=1, le=40)):
    """Prime-style browse rows. Uses the FREE YouTube Data API v3 when
    YOUTUBE_API_KEY is set, otherwise the keyless yt-dlp ytsearch engine."""
    engine = "ytsearch (keyless)"
    items: List[Dict[str, Any]] = []
    error: Optional[str] = None
    error_note: Optional[str] = None

    if YOUTUBE_API_KEY:
        try:
            items = await _youtube_api_browse(q, n)
            if items:
                engine = "youtube-data-api-v3"
        except Exception as exc:
            # quota exceeded / bad key -> silently fall back to keyless engine
            error_note = f"YouTube API unavailable ({str(exc)[:120]}) — using keyless engine."
    if not items:
        try:
            items = await asyncio.to_thread(_browse_sync, q, n)
            if YOUTUBE_API_KEY and not items and locals().get("error_note"):
                error = error_note
        except yt_dlp.utils.DownloadError as exc:
            msg = str(exc).replace("ERROR:", "").strip()[:300]
            if "Sign in to confirm" in msg or "not a bot" in msg:
                msg = ("YouTube is bot-checking this server's IP right now — shelves will fill "
                       "in when it clears. Try a direct link meanwhile.")
            error = msg
        except Exception as exc:
            error = f"Browse failed: {str(exc)[:200]}"
    return {"query": q, "items": items, "engine": engine, "error": error}


# --------------------------------------------------------------------------- #
#  Zero-API live web search (duckduckgo_search)                                #
# --------------------------------------------------------------------------- #

def _ddg_search_sync(query: str, max_results: int = 10) -> Dict[str, Any]:
    web_results: List[Dict[str, Any]] = []
    news_results: List[Dict[str, Any]] = []
    errors: List[str] = []
    with DDGS(timeout=15) as ddgs:
        try:
            for r in ddgs.text(query, region="wt-wt", max_results=max_results):
                href = r.get("href") or r.get("url") or ""
                web_results.append({
                    "title": r.get("title") or "",
                    "url": href,
                    "body": (r.get("body") or "")[:500],
                    "source": href.split("/")[2] if "://" in href else "web",
                })
        except Exception as exc:
            errors.append(f"web: {str(exc)[:120]}")
        try:
            for r in ddgs.news(query, region="wt-wt", max_results=5):
                news_results.append({
                    "title": r.get("title") or "",
                    "url": r.get("url") or "",
                    "body": (r.get("body") or r.get("excerpt") or "")[:500],
                    "source": r.get("source") or "news",
                    "date": r.get("date") or "",
                })
        except Exception as exc:
            errors.append(f"news: {str(exc)[:120]}")
    if not web_results and not news_results:
        return {"query": query, "results": [], "news": [], "error": "search_failed",
                "message": "No results right now (rate-limited or empty). Try again in a moment."
                + (f" Detail: {'; '.join(errors)}" if errors else "")}
    return {"query": query, "results": web_results, "news": news_results, "error": None}


@app.get("/api/reels/discover")
async def api_reels_discover(platform: str = Query("instagram"), q: str = Query("", max_length=100)):
    """Keyless reels discovery: DDG site-filtered search for Instagram/Facebook reel links."""
    platform = platform.lower() if platform.lower() in ("instagram", "facebook") else "instagram"
    base_q = q.strip() or ("reels" if platform == "instagram" else "reels")
    site = "site:instagram.com/reel" if platform == "instagram" else "site:facebook.com/reel"
    def _sync():
        with DDGS(timeout=15) as ddgs:
            return list(ddgs.text(f"{site} {base_q}", region="wt-wt", max_results=24))
    try:
        raw = await asyncio.to_thread(_sync)
    except Exception as exc:
        return {"platform": platform, "items": [], "error": f"Discovery failed: {str(exc)[:200]}"}
    items = []
    for r in raw:
        url = r.get("href") or r.get("url") or ""
        if "/reel" not in url and "/reels" not in url and "/watch" not in url:
            continue
        items.append({"title": r.get("title") or "Reel", "url": url, "body": (r.get("body") or "")[:200],
                      "source": platform})
    return {"platform": platform, "items": items, "error": None}


@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=2)):
    return await asyncio.to_thread(_ddg_search_sync, q, 10)


# --------------------------------------------------------------------------- #
#  Lightweight heuristic AI / chatbot engine                                    #
# --------------------------------------------------------------------------- #

STOPWORDS = set(
    """a an the is are was were be been being am of in on at to for with about from
    by and or but if then than as it its it's this that these those what which who
    whom whose how when where why do does did done can could should would will wont
    just also very your yours you i me my we us our they them their he she his her
    there here get got some any more most other into over under out up down off no
    not so such only own same too s t don now""".split()
)
GREETINGS = {"hi", "hello", "hey", "yo", "sup", "namaste", "hola", "good morning", "good evening"}
THANKS = {"thanks", "thank you", "thx", "ty"}


def _tokenize(text: str) -> set:
    words = re.findall(r"[a-z0-9']+", (text or "").lower())
    return {w for w in words if w not in STOPWORDS and len(w) > 2}


def _sentences(text: str) -> List[str]:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    return [p.strip() for p in re.split(r"(?<=[.!?])\s+", text) if 25 <= len(p.strip()) <= 400]


def _score(sentence: str, qtokens: set, position: int, total: int) -> float:
    stokens = _tokenize(sentence)
    if not stokens:
        return 0.0
    overlap = len(qtokens & stokens) / max(1, len(qtokens))
    coverage = len(qtokens & stokens) / len(stokens)
    pos_bonus = 0.25 * (1 - (position / max(1, total)))
    length_penalty = 0.85 if len(sentence) > 320 else 1.0
    return (0.65 * overlap + 0.25 * coverage + 0.10 + pos_bonus) * length_penalty


def _summarize_corpus(query: str, corpus: List[Dict[str, Any]]) -> Dict[str, Any]:
    qtokens = _tokenize(query)
    scored = []
    for block in corpus:
        for position, sentence in enumerate(_sentences(block.get("body", ""))):
            score = _score(sentence, qtokens, position, 8)
            if qtokens:
                tmatch = len(qtokens & _tokenize(block.get("title", "")))
                score += 0.15 * min(tmatch, 3) / 3
            scored.append((score, sentence, block.get("title", ""), block.get("url", "")))
    scored.sort(key=lambda x: x[0], reverse=True)

    picked, seen = [], set()
    for _, sentence, title, url in scored:
        key = sentence[:60]
        if key in seen:
            continue
        seen.add(key)
        picked.append({"sentence": sentence, "title": title, "url": url})
        if len(picked) >= 4:
            break
    if not picked:
        return {"answer": "", "sources": []}

    answer = "\n".join(f"- {p['sentence']}" for p in picked)
    sources = []
    for p in picked:
        if p["url"] and p["url"] not in [s["url"] for s in sources]:
            sources.append({"title": p["title"] or p["url"], "url": p["url"]})
    return {"answer": answer, "sources": sources[:4]}


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    video: Optional[Dict[str, Any]] = None
    search_results: Optional[List[Dict[str, Any]]] = None
    auto_search: bool = True


def _chat_sync(payload: ChatRequest) -> Dict[str, Any]:
    query = payload.query.strip()
    ql = query.lower()
    video = payload.video or {}
    search_results = (payload.search_results or [])[:8]
    used_search = False

    if ql in GREETINGS:
        return {"answer": "Hey! I'm your Neura co-pilot. Paste any YouTube / Instagram / X / Facebook / "
                          "Telegram link and I'll pull it up ad-free, or ask me anything and I'll search the live web for you.",
                "sources": [], "used_search": False}
    if ql in THANKS:
        return {"answer": "Anytime! Ask me anything else — about the video playing or the wider web.",
                "sources": [], "used_search": False}

    wants_summary = bool(re.search(r"\b(summar|recap|what('| i)?s (this|it) about|overview|tl;?dr)\b", ql))
    if wants_summary and video:
        corpus = [{"title": video.get("title") or "",
                   "body": f"{video.get('title', '')}. {video.get('description', '')}",
                   "url": video.get("webpage_url") or ""}]
        result = _summarize_corpus(query or "summarize this video", corpus)
        head = f"Now playing: {video.get('title', 'unknown')} by {video.get('uploader', 'unknown')}. "
        if result["answer"]:
            return {"answer": head + "Key points:\n" + result["answer"], "sources": result["sources"], "used_search": False}
        return {"answer": head + "The source has no long description to summarize, but it's "
                            f"{video.get('duration_label', 'unknown length')} long. Ask me to search the web for "
                            "more background on it.", "sources": [], "used_search": False}

    wants_web = bool(re.search(r"\b(search|look ?up|find|google|news|latest|trending)\b", ql))
    if not search_results and (wants_web or not video) and payload.auto_search:
        found = _ddg_search_sync(query, 8)
        search_results = found.get("results", [])[:6] + found.get("news", [])[:2]
        used_search = True

    corpus: List[Dict[str, Any]] = []
    if video:
        corpus.append({"title": video.get("title") or "current video",
                       "body": f"{video.get('title', '')} by {video.get('uploader', '')}. {video.get('description', '')}",
                       "url": video.get("webpage_url") or ""})
    for r in search_results:
        corpus.append({"title": r.get("title", ""),
                       "body": f"{r.get('title', '')}. {r.get('body', '')}", "url": r.get("url", "")})

    if corpus:
        result = _summarize_corpus(query, corpus)
        if result["answer"]:
            prefix = ("Based on the video and live web context" if (video and used_search)
                      else "Based on the live web search" if used_search else "Based on the current context")
            return {"answer": f"{prefix}:\n{result['answer']}", "sources": result["sources"], "used_search": used_search}

    if used_search:
        return {"answer": "I searched but found nothing solid for that. Try rephrasing, or add a couple of keywords.",
                "sources": [], "used_search": True}
    return {"answer": "I don't have context for that yet. Paste a video URL or run a web search first, then ask me.",
            "sources": [], "used_search": False}


@app.post("/api/chat")
async def api_chat(payload: ChatRequest):
    return await asyncio.to_thread(_chat_sync, payload)


# --------------------------------------------------------------------------- #
#  Chunked media proxy (IP-locked CDN URLs -> browser, flat RAM)                #
# --------------------------------------------------------------------------- #

def _host_allowed(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return False
    return any(host == s or host.endswith("." + s) for s in ALLOWED_PROXY_SUFFIXES)


@app.get("/api/proxy")
async def api_proxy(url: str, request: Request):
    if not _host_allowed(url):
        raise HTTPException(status_code=403, detail="This host is not an allowed media CDN.")
    fwd = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if rng := request.headers.get("range"):
        fwd["Range"] = rng
    client = httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0, read=60.0))
    try:
        upstream = await client.send(client.build_request("GET", url, headers=fwd), stream=True)
    except httpx.HTTPError as exc:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Upstream CDN unreachable: {str(exc)[:200]}") from exc

    async def relay():
        try:
            async for chunk in upstream.aiter_bytes(65536):
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    passthrough = {k: v for k, v in upstream.headers.items()
                   if k.lower() in ("content-type", "content-length", "accept-ranges",
                                    "content-range", "etag", "last-modified")}
    if request.query_params.get("dl") == "1":
        ctype = (upstream.headers.get("content-type") or "").lower()
        name = "neura-audio.mp3" if "audio" in ctype else "neura-video.mp4"
        passthrough["Content-Disposition"] = f'attachment; filename="{name}"'
    return StreamingResponse(relay(), status_code=upstream.status_code, headers=passthrough)


# --------------------------------------------------------------------------- #
#  Frontend — hybrid platform UI (embedded HTML/CSS/JS)                         #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
#  Email OTP auth (free SMTP via env vars; dev-mode fallback shows the code)   #
# --------------------------------------------------------------------------- #

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
MAIL_FROM = os.getenv("MAIL_FROM", "Neura Studio <no-reply@neurastudio.official.com>")

SESSION_COOKIE = "neura_session"
SESSION_DAYS = 30
OTP_TTL_SECONDS = 600
OTP_MAX_ATTEMPTS = 5

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _send_otp_email(to_email: str, code: str) -> bool:
    """Send the OTP over SMTP. Works with any free provider (Gmail app password,
    Brevo, Resend SMTP etc.). Returns False when not configured."""
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS):
        return False
    body = (
        "Your Neura Stream verification code is:\n\n"
        f"    {code}\n\n"
        "It expires in 10 minutes. If you did not request it, ignore this mail.\n\n"
        "— Neura Studio (neurastudio.official.com)"
    )
    msg = (
        f"From: {MAIL_FROM}\r\n"
        f"To: {to_email}\r\n"
        "Subject: Your Neura Stream login code\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n\r\n"
        f"{body}"
    )
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [to_email], msg)
        return True
    except Exception:
        return False


def _current_user(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    with _db() as conn:
        row = conn.execute(
            "SELECT u.id, u.email, u.name FROM sessions s JOIN users u ON u.id = s.user_id "
            "WHERE s.token_hash = ? AND s.expires_at > ?",
            (_sha256(token), time.time()),
        ).fetchone()
    return {"id": row["id"], "email": row["email"], "name": row["name"]} if row else None


class OtpRequest(BaseModel):
    email: str = Field(..., max_length=120)


class OtpVerify(BaseModel):
    email: str = Field(..., max_length=120)
    code: str = Field(..., min_length=4, max_length=8)
    name: str = Field("", max_length=40)


@app.post("/api/auth/request-otp")
def api_request_otp(payload: OtpRequest):
    email = payload.email.strip().lower()
    if not EMAIL_RE.fullmatch(email):
        return {"error": "bad_email", "message": "Please enter a valid email address."}
    code = f"{secrets.randbelow(1000000):06d}"
    with _db() as conn:
        conn.execute(
            "INSERT INTO otp_codes (email, code_hash, attempts, expires_at) VALUES (?, ?, 0, ?) "
            "ON CONFLICT(email) DO UPDATE SET code_hash = excluded.code_hash, attempts = 0, expires_at = excluded.expires_at",
            (email, _sha256(code), time.time() + OTP_TTL_SECONDS),
        )
    sent = _send_otp_email(email, code)
    resp = {"ok": True, "email": email, "email_sent": sent}
    if not sent:
        # dev-mode fallback: no SMTP configured — surface the code so the flow works
        resp["dev_otp"] = code
        resp["message"] = ("SMTP is not configured on this server (set SMTP_HOST/SMTP_USER/SMTP_PASS), "
                           "so here is your code directly.")
    return resp


@app.post("/api/auth/verify")
def api_verify_otp(payload: OtpVerify, request: Request):
    email = payload.email.strip().lower()
    code = payload.code.strip()
    with _db() as conn:
        row = conn.execute("SELECT * FROM otp_codes WHERE email = ?", (email,)).fetchone()
        if not row:
            return {"error": "no_code", "message": "Request a code first."}
        if row["expires_at"] < time.time():
            conn.execute("DELETE FROM otp_codes WHERE email = ?", (email,))
            return {"error": "expired", "message": "That code expired — request a new one."}
        if row["attempts"] >= OTP_MAX_ATTEMPTS:
            return {"error": "too_many", "message": "Too many wrong tries — request a new code."}
        if row["code_hash"] != _sha256(code):
            conn.execute("UPDATE otp_codes SET attempts = attempts + 1 WHERE email = ?", (email,))
            return {"error": "wrong_code", "message": "That code is incorrect."}
        conn.execute("DELETE FROM otp_codes WHERE email = ?", (email,))
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user:
            name = payload.name.strip() or email.split("@")[0]
            conn.execute("INSERT INTO users (email, name, created_at) VALUES (?, ?, ?)", (email, name, time.time()))
            user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        token = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO sessions (token_hash, user_id, expires_at) VALUES (?, ?, ?)",
            (_sha256(token), user["id"], time.time() + SESSION_DAYS * 86400),
        )
    response = JSONResponse({"ok": True, "user": {"email": email, "name": user["name"]}})
    response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_DAYS * 86400, httponly=True, samesite="lax")
    return response


# --------------------------- in-app chat (DMs) ----------------------------- #

@app.get("/api/chat/users")
def api_chat_users(request: Request):
    me = _current_user(request)
    if not me:
        return {"error": "auth_required", "message": "Sign in to chat."}
    with _db() as conn:
        rows = conn.execute(
            "SELECT u.id, u.name, u.email FROM users u WHERE u.id != ? ORDER BY u.id DESC LIMIT 100",
            (me["id"],),
        ).fetchall()
        users = []
        for r in rows:
            last = conn.execute(
                "SELECT body, created_at FROM messages WHERE (sender_id=? AND receiver_id=?) OR (sender_id=? AND receiver_id=?) "
                "ORDER BY id DESC LIMIT 1",
                (me["id"], r["id"], r["id"], me["id"]),
            ).fetchone()
            users.append({
                "id": r["id"], "name": r["name"], "email": r["email"],
                "last": last["body"][:60] if last else "Say hi!",
                "last_at": last["created_at"] if last else 0,
            })
    users.sort(key=lambda u: u["last_at"], reverse=True)
    return {"me": me, "users": users}


@app.get("/api/chat/messages")
def api_chat_messages(with_user: int, request: Request):
    me = _current_user(request)
    if not me:
        return {"error": "auth_required", "message": "Sign in to chat."}
    with _db() as conn:
        rows = conn.execute(
            "SELECT m.*, s.name AS sender_name FROM messages m "
            "JOIN users s ON s.id = m.sender_id "
            "WHERE (m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?) "
            "ORDER BY m.id ASC LIMIT 200",
            (me["id"], with_user, with_user, me["id"]),
        ).fetchall()
    return {"messages": [
        {"id": r["id"], "mine": r["sender_id"] == me["id"], "body": r["body"],
         "created_at": r["created_at"], "sender": r["sender_name"]}
        for r in rows
    ]}


class MessageIn(BaseModel):
    to: int
    body: str = Field(..., min_length=1, max_length=2000)


@app.post("/api/chat/send")
def api_chat_send(payload: MessageIn, request: Request):
    me = _current_user(request)
    if not me:
        return {"error": "auth_required", "message": "Sign in to chat."}
    if payload.to == me["id"]:
        return {"error": "bad_target", "message": "Can't message yourself."}
    with _db() as conn:
        target = conn.execute("SELECT 1 FROM users WHERE id = ?", (payload.to,)).fetchone()
        if not target:
            return {"error": "not_found", "message": "That user doesn't exist."}
        conn.execute(
            "INSERT INTO messages (sender_id, receiver_id, body, created_at) VALUES (?, ?, ?, ?)",
            (me["id"], payload.to, payload.body.strip(), time.time()),
        )
    return {"ok": True}


@app.get("/api/auth/me")
def api_auth_me(request: Request):
    return {"user": _current_user(request)}


@app.post("/api/auth/logout")
def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        with _db() as conn:
            conn.execute("DELETE FROM sessions WHERE token_hash = ?", (_sha256(token),))
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE)
    return response


FRONTEND = r"""
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NeuraStream</title>
<meta name="description" content="NeuraStream — YouTube-style streaming platform with YouTube content, own uploads, Shorts, reels and AI co-pilot.">
<meta name="theme-color" content="#0f0f0f">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z' fill='%23ff0033'/%3E%3C/svg%3E">
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#0f0f0f;--bg2:#272727;--fg:#f1f1f1;--muted:#aaa;--accent:#ff0033;--chip:#272727;--line:#303030;--link:#3ea6ff}
html.light{--bg:#ffffff;--bg2:#f2f2f2;--fg:#0f0f0f;--muted:#606060;--accent:#ff0033;--chip:#f2f2f2;--line:#e5e5e5}
*{-webkit-tap-highlight-color:transparent}
body{background:var(--bg);color:var(--fg);font-family:Roboto,Arial,sans-serif}
::selection{background:rgba(255,0,51,.3)}
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px}
::-webkit-scrollbar-track{background:transparent}
a{color:var(--link)}
.sb-item{display:flex;align-items:center;gap:1.5rem;padding:.55rem .75rem;border-radius:.65rem;cursor:pointer;font-size:14px;color:var(--fg);transition:background .15s}
.sb-item:hover{background:var(--bg2)}
.sb-item.active{background:var(--bg2);font-weight:500}
.sb-title{padding:.5rem .75rem;font-size:14px;font-weight:500;color:var(--fg)}
.chip-row{display:flex;gap:.55rem;overflow-x:auto;padding-bottom:.25rem}
.chip-row::-webkit-scrollbar{display:none}
.chip-btn{white-space:nowrap;padding:.4rem .8rem;border-radius:9999px;background:var(--chip);color:var(--fg);font-size:13.5px;font-weight:500;cursor:pointer;border:1px solid transparent;transition:all .15s}
.chip-btn:hover{background:var(--line)}
.chip-btn.on{background:var(--fg);color:var(--bg)}
.yt-card{cursor:pointer}
.yt-card .thumb{border-radius:.75rem;overflow:hidden;position:relative}
.yt-card .thumb img{width:100%;aspect-ratio:16/9;object-fit:cover;background:var(--bg2);transition:transform .3s}
.yt-card:hover .thumb img{transform:scale(1.03)}
.dur-badge{position:absolute;bottom:.4rem;right:.4rem;background:rgba(0,0,0,.8);color:#fff;font-size:11.5px;font-weight:500;padding:.1rem .3rem;border-radius:.25rem}
.v-title{font-size:14.5px;font-weight:500;line-height:1.35;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;margin-bottom:.25rem}
.v-meta{font-size:12.5px;color:var(--muted)}
.ch-avatar{width:36px;height:36px;border-radius:9999px;display:grid;place-items:center;font-weight:700;color:#fff;font-size:14px;flex-shrink:0}
.rail{width:72px}
.rail .sb-item{flex-direction:column;gap:.35rem;font-size:10px;padding:.9rem 0}
input[type=range].seek{-webkit-appearance:none;appearance:none;height:4px;border-radius:99px;background:linear-gradient(90deg,var(--accent) var(--fill,0%),rgba(255,255,255,.25) var(--fill,0%));cursor:pointer}
input[type=range].seek::-webkit-slider-thumb{-webkit-appearance:none;width:13px;height:13px;border-radius:99px;background:#f00}
input[type=range].seek::-moz-range-thumb{width:13px;height:13px;border:none;border-radius:999px;background:#f00}
.ctr-btn{padding:.45rem;border-radius:9999px;cursor:pointer;transition:background .15s}
.ctr-btn:hover{background:rgba(255,255,255,.12)}
.act-pill{display:inline-flex;align-items:center;gap:.5rem;padding:.5rem .9rem;border-radius:9999px;background:var(--bg2);font-size:13.5px;font-weight:500;cursor:pointer;transition:filter .15s}
.act-pill:hover{filter:brightness(1.2)}
.msg-in{animation:fadeUp .3s ease-out both}
@keyframes fadeUp{0%{opacity:0;transform:translateY(8px)}100%{opacity:1;transform:translateY(0)}}
.view{display:none}
.view.active{display:block}
/* mobile bottom nav */
#bottom-nav{display:flex}
@media(min-width:1024px){#bottom-nav{display:none}}
.bn-item{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;padding:8px 0;font-size:10px;color:var(--muted);cursor:pointer}
.bn-item.active{color:var(--fg)}
.bn-item svg{width:22px;height:22px}
/* chat bubbles */
.bubble{max-width:75%;padding:8px 12px;border-radius:12px;font-size:14px;line-height:1.4;position:relative;word-break:break-word}
.bubble.me{align-self:flex-end;background:#005c4b;color:#fff;border-bottom-right-radius:2px}
.bubble.them{align-self:flex-start;background:var(--bg2);color:var(--fg);border-bottom-left-radius:2px}
.bubble .btime{display:block;font-size:10px;opacity:.7;margin-top:3px;text-align:right}
.skeleton{position:relative;overflow:hidden;background:var(--bg2);border-radius:.75rem}
.skeleton::after{content:'';position:absolute;inset:0;background:linear-gradient(90deg,transparent,rgba(255,255,255,.06),transparent);animation:shimmer 1.5s infinite}
@keyframes shimmer{0%{transform:translateX(-100%)}100%{transform:translateX(100%)}}
.reel-track{scroll-snap-type:y mandatory;-ms-overflow-style:none;scrollbar-width:none}
.reel-track::-webkit-scrollbar{display:none}
.reel-item{scroll-snap-align:start;scroll-snap-stop:always}
.drop-zone.drag{border-color:var(--accent);background:rgba(255,0,51,.08)}
.line-clamp-1{display:-webkit-box;-webkit-line-clamp:1;-webkit-box-orient:vertical;overflow:hidden}
.line-clamp-2{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.line-clamp-3{display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
:focus-visible{outline:2px solid var(--link);outline-offset:2px;border-radius:8px}
</style>
</head>
<body class="min-h-screen">

<!-- ================= TOPBAR ================= -->
<header class="fixed top-0 inset-x-0 h-14 z-50 flex items-center gap-2 px-2 sm:px-4" style="background:var(--bg)">
  <button id="btn-menu" class="ctr-btn p-2" title="Menu">
    <svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M3 6h18v2H3V6Zm0 5h18v2H3v-2Zm0 5h18v2H3v-2Z"/></svg>
  </button>
  <button id="nav-home" class="flex items-center gap-1 shrink-0 pr-2">
    <svg class="w-7 h-7" viewBox="0 0 24 24"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z" fill="#ff0033"/></svg>
    <span class="text-[19px] font-bold tracking-tight hidden sm:block">NeuraStream</span>
  </button>

  <form id="main-form" class="flex-1 max-w-2xl mx-auto hidden sm:flex items-center">
    <div class="flex flex-1">
      <input id="main-input" autocomplete="off" placeholder="Search videos or paste any link"
        class="flex-1 px-4 py-2 text-sm rounded-l-full border outline-none" style="background:var(--bg);border-color:var(--line);color:var(--fg)" />
      <button class="px-5 py-2 rounded-r-full border border-l-0 grid place-items-center" style="background:var(--bg2);border-color:var(--line)" title="Search">
        <svg class="w-5 h-5" fill="currentColor" style="color:var(--fg)"><path d="M20.87 20.17l-5.59-5.59A6.94 6.94 0 0 0 17 10a7 7 0 1 0-7 7 6.94 6.94 0 0 0 4.58-1.72l5.59 5.59.7-.7ZM10 16a6 6 0 1 1 6-6 6 6 0 0 1-6 6Z"/></svg>
      </button>
    </div>
  </form>
  <div class="flex-1 sm:hidden"></div>

  <div class="flex items-center gap-1 shrink-0">
    <button id="btn-search-m" class="ctr-btn sm:hidden" title="Search">
      <svg class="w-5 h-5" fill="currentColor"><path d="M20.87 20.17l-5.59-5.59A6.94 6.94 0 0 0 17 10a7 7 0 1 0-7 7 6.94 6.94 0 0 0 4.58-1.72l5.59 5.59.7-.7ZM10 16a6 6 0 1 1 6-6 6 6 0 0 1-6 6Z"/></svg>
    </button>
    <button id="btn-theme" class="ctr-btn" title="Dark / light">
      <svg id="ic-sun" class="w-5 h-5 hidden" fill="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>
      <svg id="ic-moon" class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z"/></svg>
    </button>
    <button id="btn-copilot" class="ctr-btn" title="AI Co-pilot">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" style="color:var(--accent)"><path stroke-linecap="round" stroke-linejoin="round" d="M12 3l1.9 4.6L18.5 9.5l-4.6 1.9L12 16l-1.9-4.6L5.5 9.5l4.6-1.9L12 3Z"/></svg>
    </button>
    <button id="btn-upload" class="ctr-btn" title="Upload video">
      <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M14 13h-3v3H9v-3H6v-2h3V8h2v3h3v2Zm3-7H3a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2Zm4 5.5L21 8v8l-3-2.5v-3Z"/></svg>
    </button>
    <button id="auth-zone-btn" class="ml-1 flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium" style="border:1px solid var(--line)">
      <svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24" style="color:var(--link)"><path d="M12 4a4 4 0 1 1-4 4 4 4 0 0 1 4-4Zm0 10c4.42 0 8 1.79 8 4v2H4v-2c0-2.21 3.58-4 8-4Z"/></svg>
      <span id="auth-zone-label" class="hidden sm:block" style="color:var(--link)">Sign in</span>
    </button>
  </div>
</header>
<form id="main-form-m" class="hidden fixed top-14 inset-x-0 z-40 px-3 py-2 sm:hidden" style="background:var(--bg)">
  <div class="flex">
    <input id="main-input-m" autocomplete="off" placeholder="Search videos or paste any link"
      class="flex-1 px-4 py-2 text-sm rounded-l-full border outline-none" style="background:var(--bg);border-color:var(--line);color:var(--fg)">
    <button class="px-4 rounded-r-full border border-l-0" style="background:var(--bg2);border-color:var(--line)">
      <svg class="w-5 h-5" fill="currentColor" style="color:var(--fg)"><path d="M20.87 20.17l-5.59-5.59A6.94 6.94 0 0 0 17 10a7 7 0 1 0-7 7 6.94 6.94 0 0 0 4.58-1.72l5.59 5.59.7-.7ZM10 16a6 6 0 1 1 6-6 6 6 0 0 1-6 6Z"/></svg>
    </button>
  </div>
</form>

<!-- ================= SIDEBAR ================= -->
<aside id="sidebar" class="fixed left-0 top-14 bottom-0 w-60 overflow-y-auto z-40 px-1 py-2 hidden lg:block transition-transform" style="background:var(--bg)">
  <div id="sb-full">
    <div class="sb-item active" data-nav="home"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 3 4 9v12h5v-7h6v7h5V9l-8-6Z"/></svg>Home</div>
    <div class="sb-item" data-nav="shorts"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M10 14.65v-5.3L15 12l-5 2.65Zm7.77-4.33-1.2-.5L18 9.06c1.84-.96 2.53-3.23 1.5-5.06s-3.42-2.45-5.26-1.49L6 6.94c-1.88.98-2.57 3.4-1.46 5.24.3.53.72.96 1.23 1.27l1.2.5L5.99 15c-1.84.96-2.53 3.23-1.5 5.06s3.42 2.45 5.26 1.49L18 17.06c1.88-.98 2.57-3.4 1.46-5.24a3.4 3.4 0 0 0-1.23-1.27Z"/></svg>Shorts</div>
    <div class="sb-item" data-nav="chats"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 3a9 9 0 0 0-7.6 13.8L3 21l4.4-1.4A9 9 0 1 0 12 3Zm-4 8h8v1.5H8V11Zm0-3h8v1.5H8V8Z"/></svg>Chats</div>
    <div class="sb-item" data-nav="history"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 3a9 9 0 1 0 8.66 11.5l-1.9-.6A7 7 0 1 1 12 5c1.9 0 3.6.76 4.86 2H14v2h7V2h-2v3.35A8.96 8.96 0 0 0 12 3Zm-1 5v5l4.25 2.52.75-1.23-3.5-2.07V8H11Z"/></svg>History</div>
    <div class="sb-item" data-nav="liked"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M18.77 11h-4.23l1.52-4.94A1.54 1.54 0 0 0 14.6 4h-.2a1.54 1.54 0 0 0-1.34.77L8.92 12H6V4H4v16h14a2 2 0 0 0 1.95-1.55l1.66-6A2 2 0 0 0 19.6 11h-.83ZM6 18v-4h3.42l.6-1L13.19 6l-1.42 4.62-.6 2A1.5 1.5 0 0 0 12.62 15h5.13l-1.44 3H6Z"/></svg>Liked videos</div>
    <hr class="my-2 border-0 h-px" style="background:var(--line)">
    <div class="sb-title">Explore</div>
    <div class="sb-item" data-explore="trending"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M13.5 1.5s.83 2.83.83 5.15c0 2.22-1.46 4.02-3.68 4.02S6.9 8.87 6.9 6.65c0-.32.02-.64.07-.95C4.53 7.26 3 10.03 3 13.15 3 18.05 7.03 22 12 22s9-3.95 9-8.85c0-5.85-4.24-10.15-7.5-11.65Z"/></svg>Trending</div>
    <div class="sb-item" data-explore="music"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 3v10.55A4 4 0 1 0 14 17V7h4V3h-6Z"/></svg>Music</div>
    <div class="sb-item" data-explore="movie"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M22 4H2v16h20V4ZM6 18H4v-2h2v2Zm0-4H4v-2h2v2Zm0-4H4V8h2v2Zm0-4H4V4h2v2Zm10 8h-2v-2h2v2Zm0-4h-2v-2h2v2Zm0-4h-2V4h2v2Zm4 12h-2v-2h2v2Zm0-4h-2v-2h2v2Zm0-4h-2V8h2v2Zm0-4h-2V4h2v2Z"/></svg>Movies</div>
    <div class="sb-item" data-explore="gaming"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M10 8v6H7.83L12 18.2l4.17-4.2H14V8h-4Zm11-1.5v11A2.5 2.5 0 0 1 18.5 20h-13A2.5 2.5 0 0 1 3 17.5v-11A2.5 2.5 0 0 1 5.5 4h13A2.5 2.5 0 0 1 21 6.5Z"/></svg>Gaming</div>
    <div class="sb-item" data-nav="mine"><svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M14 13h-3v3H9v-3H6v-2h3V8h2v3h3v2Zm3-7H3a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2Zm4 5.5L21 8v8l-3-2.5v-3Z"/></svg>Your videos</div>
  </div>
</aside>
<aside id="sidebar-m" class="fixed left-0 top-14 bottom-0 w-60 overflow-y-auto z-40 px-1 py-2 hidden" style="background:var(--bg)"></aside>
<div id="sb-backdrop" class="hidden fixed inset-0 top-14 z-30 bg-black/50"></div>

<main id="main" class="pt-14 pb-16 lg:pb-0 lg:pl-60 transition-all">
<!-- ================= VIEW: HOME ================= -->
<div id="view-home" class="view active px-2 sm:px-6 py-3">
  <div class="chip-row sticky top-14 z-20 py-2" style="background:var(--bg)" id="home-chips">
    <button class="chip-btn on" data-feed="trending">All</button>
    <button class="chip-btn" data-feed="music">Music</button>
    <button class="chip-btn" data-feed="movie">Movies</button>
    <button class="chip-btn" data-feed="gaming">Gaming</button>
    <button class="chip-btn" data-feed="live">Live</button>
    <button class="chip-btn" data-feed="cricket">Cricket</button>
    <button class="chip-btn" data-feed="comedy">Comedy</button>
    <button class="chip-btn" data-feed="news">News</button>
  </div>
  <div id="feed-title" class="px-1 pb-2 pt-1 text-sm font-medium hidden" style="color:var(--muted)"></div>
  <div id="feed-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-x-4 gap-y-8"></div>
  <div id="feed-skeleton" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-x-4 gap-y-8"></div>
  <div id="feed-empty" class="hidden text-center py-16 text-sm" style="color:var(--muted)">Nothing here — try another search.</div>
</div>

<!-- ================= VIEW: WATCH ================= -->
<div id="view-watch" class="view px-2 sm:px-6 py-4">
  <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_402px] gap-6 max-w-[1750px] mx-auto">
    <div class="min-w-0">
      <div class="rounded-xl overflow-hidden bg-black relative group select-none" id="player-shell">
        <video id="video" class="w-full aspect-video max-h-[75vh] bg-black" playsinline preload="metadata"></video>
        <div id="iframe-wrap" class="hidden w-full aspect-video max-h-[75vh] bg-black">
          <iframe id="yt-frame" class="w-full h-full" src="" title="player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
        </div>
        <div id="embed-note" class="hidden absolute top-2 left-2 right-2 z-10 rounded-lg px-3 py-2 text-xs" style="background:rgba(0,0,0,.75);color:#fde047"></div>
        <button id="big-play" class="absolute inset-0 grid place-items-center opacity-0 group-hover:opacity-100 transition">
          <span class="w-20 h-20 rounded-full grid place-items-center" style="background:rgba(0,0,0,.55)">
            <svg class="w-10 h-10 ml-1" viewBox="0 0 24 24" fill="#fff"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg>
          </span>
        </button>
        <div class="absolute bottom-0 inset-x-0 px-3 pb-2 pt-10 bg-gradient-to-t from-black/90 via-black/40 to-transparent opacity-0 group-hover:opacity-100 focus-within:opacity-100 transition">
          <input id="seek" class="seek w-full mb-1.5" type="range" min="0" max="1000" value="0" step="0.1" aria-label="Seek">
          <div class="flex items-center gap-1 text-white">
            <button id="btn-play" class="ctr-btn"><svg id="ic-play" class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg><svg id="ic-pause" class="w-6 h-6 hidden" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16" rx="1.5"/><rect x="14" y="4" width="4" height="16" rx="1.5"/></svg></button>
            <button id="btn-mute" class="ctr-btn"><svg id="ic-vol" class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor"><path d="M13 4.5v15a1 1 0 0 1-1.64.77L6.8 16.5H4a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h2.8l4.56-3.77A1 1 0 0 1 13 4.5Z"/><path d="M16 8.5a5 5 0 0 1 0 7" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg><svg id="ic-muted" class="w-6 h-6 hidden" viewBox="0 0 24 24" fill="currentColor"><path d="M13 4.5v15a1 1 0 0 1-1.64.77L6.8 16.5H4a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h2.8l4.56-3.77A1 1 0 0 1 13 4.5Z"/><path d="m16 9 5 6m0-6-5 6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg></button>
            <span class="text-xs tabular-nums font-medium ml-1"><span id="t-now">0:00</span> / <span id="t-dur">0:00</span></span>
            <div class="flex-1"></div>
            <select id="quality-select" class="bg-white/10 border border-white/20 rounded-lg text-xs px-2 py-1.5 outline-none max-w-[120px]"></select>
            <button id="btn-pip" class="ctr-btn" title="Picture-in-picture"><svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><rect x="2" y="4" width="20" height="16" rx="2"/><rect x="12" y="12" width="8" height="6" rx="1" fill="currentColor" stroke="none"/></svg></button>
            <button id="btn-fs" class="ctr-btn" title="Fullscreen"><svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><path stroke-linecap="round" d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3M3 16v3a2 2 0 0 0 2 2h3m13-5v5a2 2 0 0 1-2 2h-3"/></svg></button>
          </div>
        </div>
      </div>

      <h1 id="w-title" class="text-lg sm:text-xl font-bold leading-snug mt-3"></h1>

      <div class="flex flex-wrap items-center gap-3 mt-3">
        <div class="flex items-center gap-3 flex-1 min-w-[200px] cursor-pointer">
          <div class="ch-avatar" id="w-avatar" style="width:40px;height:40px">N</div>
          <div>
            <p id="w-channel" class="text-sm font-medium leading-tight"></p>
            <p id="w-subs" class="text-xs" style="color:var(--muted)"></p>
          </div>
          <button id="w-sub" class="ml-2 px-4 py-2 rounded-full text-sm font-medium" style="background:var(--fg);color:var(--bg)">Subscribe</button>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <div class="flex rounded-full overflow-hidden" style="background:var(--bg2)">
            <button id="w-like" class="flex items-center gap-1.5 px-4 py-2 text-sm font-medium" title="Like"><svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M18.77 11h-4.23l1.52-4.94A1.54 1.54 0 0 0 14.6 4h-.2a1.54 1.54 0 0 0-1.34.77L8.92 12H6V4H4v16h14a2 2 0 0 0 1.95-1.55l1.66-6A2 2 0 0 0 19.6 11h-.83ZM6 18v-4h3.42l.6-1L13.19 6l-1.42 4.62-.6 2A1.5 1.5 0 0 0 12.62 15h5.13l-1.44 3H6Z"/></svg><span id="w-likes">Like</span></button>
            <div class="w-px" style="background:var(--line)"></div>
            <button id="w-dislike" class="px-4 py-2" title="Dislike"><svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M5.23 13h4.23l-1.52 4.94A1.54 1.54 0 0 0 9.4 20h.2a1.54 1.54 0 0 0 1.34-.77L15.08 12H18v8h2V4H6a2 2 0 0 0-1.95 1.55l-1.66 6A2 2 0 0 0 4.4 13h.83ZM18 6v4h-3.42l-.6 1L10.81 18l1.42-4.62.6-2A1.5 1.5 0 0 0 11.38 9H6.25l1.44-3H18Z"/></svg></button>
          </div>
          <button id="w-share" class="act-pill"><svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M15 5.63 20.66 12 15 18.37V15h-1c-3.96 0-7.14 1-9.75 3.09 1.84-4.07 5.11-6.4 9.89-7.1l.86-.13V5.63Z"/></svg>Share</button>
          <button id="w-download" class="act-pill"><svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M17 18v1H6v-1h11Zm-.5-6.6-.7-.7-3.3 3.28V4h-1v9.98L8.2 10.7l-.7.7 4.5 4.5 4.5-4.5Z"/></svg>Download</button>
        </div>
      </div>

      <div class="mt-3 rounded-xl p-3 text-sm" style="background:var(--bg2)">
        <p id="w-views" class="font-medium"></p>
        <p id="w-desc" class="mt-1 whitespace-pre-line" style="color:var(--fg)"></p>
      </div>

      <div class="mt-6">
        <h3 class="font-bold mb-4"><span id="c-count"></span> Comments</h3>
        <form id="c-form" class="flex gap-3 mb-6">
          <div class="ch-avatar" style="width:40px;height:40px" id="c-avatar">Y</div>
          <div class="flex-1">
            <input id="c-body" placeholder="Add a comment…" required class="w-full bg-transparent border-b pb-1 text-sm outline-none focus:border-current" style="border-color:var(--line)">
            <div class="flex justify-end gap-2 mt-2">
              <input id="c-author" placeholder="name (optional)" class="px-3 py-1.5 rounded-full text-xs outline-none" style="background:var(--bg2)">
              <button class="px-4 py-1.5 rounded-full text-sm font-medium" style="background:#3ea6ff;color:#fff">Comment</button>
            </div>
          </div>
        </form>
        <div id="c-list" class="space-y-4"></div>
      </div>
    </div>

    <div class="min-w-0">
      <h3 class="font-medium text-sm mb-3" style="color:var(--muted)">Related videos</h3>
      <div id="related-list" class="space-y-2"></div>
    </div>
  </div>
</div>

<!-- ================= VIEW: SHORTS ================= -->
<div id="view-shorts" class="view px-2 sm:px-6 py-3">
  <div class="flex gap-2 mb-4 max-w-xl">
    <input id="shorts-search" placeholder="Search Shorts… (funny, dance, ipl)" class="flex-1 px-4 py-2.5 rounded-full border text-sm outline-none" style="background:var(--bg);border-color:var(--line)">
    <button id="shorts-btn" class="px-5 py-2.5 rounded-full text-sm font-medium" style="background:var(--accent);color:#fff">Search</button>
  </div>
  <div id="shorts-grid" class="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 xl:grid-cols-8 gap-3 mb-6"></div>
  <div class="flex items-center gap-2 mb-3">
    <span class="w-1.5 h-1.5 rounded-full" style="background:var(--accent)"></span>
    <h3 class="font-medium text-sm" style="color:var(--muted)">Community shorts feed (vertical scroll)</h3>
  </div>
  <div id="reel-track" class="reel-track rounded-2xl overflow-y-auto h-[75vh] bg-black relative"></div>
  <p id="reels-empty" class="hidden text-center text-sm py-12" style="color:var(--muted)">No community shorts yet — upload a short video.</p>
  <div class="flex gap-2 mt-4 max-w-xl">
    <input id="reel-url-input" placeholder="Paste any Instagram / Facebook / X / Telegram reel URL…" class="flex-1 px-4 py-2.5 rounded-full border text-sm outline-none" style="background:var(--bg);border-color:var(--line)">
  </div>
</div>

<!-- ================= VIEW: LIKED / MINE ================= -->
<div id="view-liked" class="view px-2 sm:px-6 py-4">
  <h2 class="text-2xl font-bold mb-4">Liked videos</h2>
  <div id="liked-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-x-4 gap-y-8"></div>
  <p id="liked-empty" class="hidden text-center text-sm py-12" style="color:var(--muted)">No liked videos yet.</p>
</div>
<div id="view-mine" class="view px-2 sm:px-6 py-4">
  <div class="flex items-center justify-between mb-4">
    <h2 class="text-2xl font-bold">Your videos</h2>
    <button id="mine-upload-btn" class="px-4 py-2 rounded-full text-sm font-medium" style="background:var(--accent);color:#fff">+ Upload</button>
  </div>
  <div id="mine-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-x-4 gap-y-8"></div>
  <p id="mine-empty" class="hidden text-center text-sm py-12" style="color:var(--muted)">You haven't uploaded anything yet.</p>
</div>

<!-- ================= VIEW: CHATS (WhatsApp-style) ================= -->
<div id="view-chats" class="view px-2 sm:px-6 py-3">
  <div class="grid grid-cols-1 md:grid-cols-[320px_minmax(0,1fr)] gap-4 max-w-5xl mx-auto h-[calc(100vh-7rem)]">
    <div class="rounded-2xl overflow-hidden flex flex-col" style="background:var(--bg2)">
      <div class="px-4 py-3 font-medium text-sm border-b flex items-center gap-2" style="border-color:var(--line)">
        <svg class="w-5 h-5" style="color:#25d366" fill="currentColor" viewBox="0 0 24 24"><path d="M12 3a9 9 0 0 0-8.66 13.8L3 21l4.4-1.4A9 9 0 1 0 12 3Z"/></svg>
        Neura Chats <span id="chat-me-name" class="ml-auto text-xs font-normal" style="color:var(--muted)"></span>
      </div>
      <div id="chat-users" class="flex-1 overflow-y-auto">
        <p class="p-6 text-center text-xs" style="color:var(--muted)">Loading users…</p>
      </div>
    </div>
    <div class="rounded-2xl overflow-hidden flex flex-col" style="background:var(--bg2)">
      <div id="chat-head" class="px-4 py-3 border-b flex items-center gap-3" style="border-color:var(--line)">
        <div class="ch-avatar" id="chat-peer-avatar" style="width:38px;height:38px;background:#25d366">?</div>
        <div><p id="chat-peer-name" class="font-medium text-sm">Pick a chat</p>
        <p class="text-[11px]" style="color:var(--muted)" id="chat-peer-mail"></p></div>
      </div>
      <div id="chat-msgs" class="flex-1 overflow-y-auto p-4 flex flex-col gap-2"></div>
      <form id="dm-form" class="p-2.5 flex gap-2 border-t" style="border-color:var(--line)">
        <input id="dm-input" placeholder="Type a message…" class="flex-1 px-4 py-2.5 rounded-full text-sm outline-none border" style="background:var(--bg);border-color:var(--line)" autocomplete="off">
        <button class="w-10 h-10 rounded-full grid place-items-center shrink-0" style="background:#25d366" aria-label="Send">
          <svg class="w-5 h-5 text-white" fill="currentColor" viewBox="0 0 24 24"><path d="M21.9 4.6 18.9 19c-.23 1-.8 1.25-1.63.78l-4.5-3.32-2.17 2.09c-.24.24-.44.44-.9.44l.33-4.6 8.37-7.56c.36-.32-.08-.5-.57-.18L7.66 13.53l-4.44-1.39c-.96-.3-.98-.96.2-1.42l17.3-6.67c.8-.3 1.5.18 1.18 1.55Z"/></svg>
        </button>
      </form>
    </div>
  </div>
</div>

<!-- ================= VIEW: HISTORY ================= -->
<div id="view-history" class="view px-2 sm:px-6 py-4">
  <h2 class="text-2xl font-bold mb-4">Watch history</h2>
  <div id="history-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-x-4 gap-y-8"></div>
  <p id="history-empty" class="hidden text-center text-sm py-12" style="color:var(--muted)">Nothing watched yet.</p>
</div>
</main>

<!-- mobile bottom nav -->
<nav id="bottom-nav" class="fixed bottom-0 inset-x-0 z-40 lg:hidden border-t" style="background:var(--bg);border-color:var(--line);padding-bottom:env(safe-area-inset-bottom)">
  <div class="bn-item active" data-bnav="home"><svg fill="currentColor" viewBox="0 0 24 24"><path d="M12 3 4 9v12h5v-7h6v7h5V9l-8-6Z"/></svg>Home</div>
  <div class="bn-item" data-bnav="shorts"><svg fill="currentColor" viewBox="0 0 24 24"><path d="M10 14.65v-5.3L15 12l-5 2.65Zm7.77-4.33c1.84-.96 2.53-3.23 1.5-5.06s-3.42-2.45-5.26-1.49L6 6.94c-1.88.98-2.57 3.4-1.46 5.24.3.53.72.96 1.23 1.27l1.2.5L5.99 15c-1.84.96-2.53 3.23-1.5 5.06s3.42 2.45 5.26 1.49L18 17.06c1.88-.98 2.57-3.4 1.46-5.24a3.4 3.4 0 0 0-1.23-1.27Z"/></svg>Shorts</div>
  <div class="bn-item" data-bnav="upload"><svg fill="currentColor" viewBox="0 0 24 24"><path d="M14 13h-3v3H9v-3H6v-2h3V8h2v3h3v2Zm3-7H3a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2Zm4 5.5L21 8v8l-3-2.5v-3Z"/></svg>Create</div>
  <div class="bn-item" data-bnav="chats"><svg fill="currentColor" viewBox="0 0 24 24"><path d="M12 3a9 9 0 0 0-8.66 13.8L3 21l4.4-1.4A9 9 0 1 0 12 3Z"/></svg>Chats</div>
  <div class="bn-item" data-bnav="mine"><svg fill="currentColor" viewBox="0 0 24 24"><path d="M12 4a4 4 0 1 1-4 4 4 4 0 0 1 4-4Zm0 10c4.42 0 8 1.79 8 4v2H4v-2c0-2.21 3.58-4 8-4Z"/></svg>You</div>
</nav>

<!-- ================= STATUS / ERROR ================= -->
<div class="max-w-6xl mx-auto px-4 space-y-3">
  <div id="status-bar" class="hidden fixed bottom-4 left-4 z-50 rounded-full px-4 py-2.5 text-sm flex items-center gap-2.5 shadow-2xl" style="background:var(--bg2)">
    <svg class="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24" style="color:var(--accent)"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 0 1 8-8v4a4 4 0 0 0-4 4H4Z"/></svg>
    <span id="status-text" style="color:var(--fg)">Working…</span>
  </div>
  <div id="error-bar" class="hidden fixed bottom-4 right-4 z-50 max-w-sm rounded-xl px-4 py-3 text-sm shadow-2xl border" style="background:var(--bg2);border-color:rgba(239,68,68,.4);color:var(--fg)">
    <span id="error-text" style="color:#fca5a5"></span>
  </div>
</div>

<!-- ================= CO-PILOT PANEL ================= -->
<div id="copilot-panel" class="hidden fixed bottom-4 right-4 z-50 w-[92vw] sm:w-96 h-[70vh] rounded-2xl flex flex-col overflow-hidden shadow-2xl" style="background:var(--bg);border:1px solid var(--line)">
  <div class="px-4 py-3 flex items-center gap-3 border-b" style="border-color:var(--line)">
    <div class="w-8 h-8 rounded-full grid place-items-center" style="background:linear-gradient(135deg,#3ea6ff,#ff0033)">
      <svg class="w-4 h-4 text-white" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2l2.4 5.9L20.3 10l-5.9 2.1L12 18l-2.4-5.9L3.7 10l5.9-2.1L12 2Z"/></svg>
    </div>
    <div class="flex-1"><p class="font-medium text-sm">Neura Co-pilot</p><p class="text-[11px]" style="color:var(--muted)">video & web synthesis</p></div>
    <button id="copilot-close" class="ctr-btn"><svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M6 18 18 6M6 6l12 12"/></svg></button>
  </div>
  <div id="chat-log" class="flex-1 overflow-y-auto p-3 space-y-3 text-sm">
    <div class="msg-in flex gap-2.5">
      <div class="ch-avatar" style="width:28px;height:28px;font-size:11px;background:linear-gradient(135deg,#3ea6ff,#ff0033)">AI</div>
      <div class="rounded-2xl rounded-tl-sm px-3.5 py-2.5 leading-relaxed max-w-[85%]" style="background:var(--bg2)">
        Playing something? Ask me to summarize it, or anything about the web.
      </div>
    </div>
  </div>
  <div class="px-3 pb-1.5 flex gap-2 overflow-x-auto chip-row">
    <button class="chat-chip chip-btn shrink-0">Summarize this video</button>
    <button class="chat-chip chip-btn shrink-0">Find related videos</button>
  </div>
  <form id="chat-form" class="p-2.5 flex gap-2 border-t" style="border-color:var(--line)">
    <input id="chat-input" autocomplete="off" placeholder="Ask anything…" class="flex-1 px-4 py-2.5 rounded-full text-sm outline-none border" style="background:var(--bg);border-color:var(--line)">
    <button class="w-10 h-10 rounded-full grid place-items-center" style="background:var(--accent)" aria-label="Send">
      <svg class="w-4.5 h-4.5 text-white" style="width:18px;height:18px" fill="none" stroke="#fff" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="m22 2-7 20-4-9-9-4Z"/><path stroke-linecap="round" d="M22 2 11 13"/></svg>
    </button>
  </form>
</div>

<!-- ================= UPLOAD MODAL ================= -->
<div id="upload-modal" class="hidden fixed inset-0 z-50 grid place-items-center p-4 bg-black/70 backdrop-blur-sm">
  <div class="rounded-2xl w-full max-w-lg p-6 max-h-[92vh] overflow-y-auto" style="background:var(--bg);border:1px solid var(--line)">
    <div class="flex items-center justify-between mb-5">
      <h3 class="font-bold text-lg">Upload video</h3>
      <button id="upload-close" class="ctr-btn"><svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="M6 18 18 6M6 6l12 12"/></svg></button>
    </div>
    <div id="drop-zone" class="drop-zone border-2 border-dashed rounded-2xl p-8 text-center cursor-pointer transition mb-4" style="border-color:var(--line)">
      <input id="file-input" type="file" accept="video/*" class="hidden">
      <div class="w-14 h-14 mx-auto rounded-full grid place-items-center mb-3" style="background:var(--bg2)">
        <svg class="w-7 h-7" style="color:var(--accent)" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 16V4m0 0L8 8m4-4 4 4M4 17v1a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-1"/></svg>
      </div>
      <p class="text-sm font-semibold" id="drop-title">Drag & drop or click to select</p>
      <p class="text-xs mt-1" style="color:var(--muted)">MP4 / WebM / MKV · 200 MB local · 500 MB with cloud storage · auto thumbnail</p>
      <video id="thumb-video" class="hidden"></video>
      <img id="thumb-preview" class="hidden w-48 aspect-video object-cover rounded-xl mx-auto mt-4 border" style="border-color:var(--line)">
    </div>
    <div class="space-y-3">
      <input id="up-title" placeholder="Title *" class="w-full px-4 py-3 rounded-xl text-sm outline-none border" style="background:var(--bg);border-color:var(--line)">
      <input id="up-uploader" placeholder="Channel name (optional)" class="w-full px-4 py-3 rounded-xl text-sm outline-none border" style="background:var(--bg);border-color:var(--line)">
      <textarea id="up-desc" rows="2" placeholder="Description (optional)" class="w-full px-4 py-3 rounded-xl text-sm outline-none border resize-none" style="background:var(--bg);border-color:var(--line)"></textarea>
    </div>
    <div id="up-progress" class="hidden mt-4">
      <div class="h-2 rounded-full overflow-hidden" style="background:var(--bg2)"><div id="up-bar" class="h-full w-0 transition-all" style="background:var(--accent)"></div></div>
      <p id="up-pct" class="text-xs mt-1.5 text-center" style="color:var(--muted)">0%</p>
    </div>
    <button id="up-submit" class="w-full mt-5 py-3.5 rounded-full font-semibold text-sm disabled:opacity-40" style="background:var(--accent);color:#fff" disabled>Upload</button>
    <p class="text-[10px] text-center mt-3" style="color:var(--muted)">Free-tier storage is ephemeral — uploads reset on server restart.</p>
  </div>
</div>

<!-- ================= AUTH MODAL ================= -->
<div id="auth-modal" class="hidden fixed inset-0 z-50 grid place-items-center p-4 bg-black/70 backdrop-blur-sm">
  <div class="rounded-2xl w-full max-w-sm p-6" style="background:var(--bg);border:1px solid var(--line)">
    <div class="flex items-center justify-between mb-1">
      <h3 class="font-bold text-lg" id="auth-title">Sign in to NeuraStream</h3>
      <button id="auth-close" class="ctr-btn"><svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="M6 18 18 6M6 6l12 12"/></svg></button>
    </div>
    <p class="text-xs mb-5" style="color:var(--muted)" id="auth-sub">Email par OTP aayega — Gmail bhi chalega.</p>
    <div id="auth-step-1" class="space-y-3">
      <input id="auth-email" type="email" autocomplete="email" placeholder="your.name@gmail.com" class="w-full px-4 py-3 rounded-xl text-sm outline-none border" style="background:var(--bg);border-color:var(--line)">
      <input id="auth-name" placeholder="Your name (optional)" class="w-full px-4 py-3 rounded-xl text-sm outline-none border" style="background:var(--bg);border-color:var(--line)">
      <button id="auth-send" class="w-full py-3 rounded-full font-semibold text-sm" style="background:var(--fg);color:var(--bg)">Send code</button>
    </div>
    <div id="auth-step-2" class="hidden space-y-3">
      <input id="auth-code" inputmode="numeric" maxlength="6" placeholder="6-digit code" class="w-full px-4 py-3 rounded-xl text-center text-2xl tracking-[0.5em] font-bold outline-none border" style="background:var(--bg);border-color:var(--line)">
      <button id="auth-verify" class="w-full py-3 rounded-full font-semibold text-sm" style="background:#3ea6ff;color:#fff">Verify & continue</button>
      <button id="auth-back" class="w-full py-2 text-xs" style="color:var(--muted)">&larr; change email</button>
    </div>
    <div id="auth-dev-note" class="hidden mt-4 rounded-xl p-3 text-xs" style="background:var(--bg2);color:#fbbf24"></div>
  </div>
</div>
<script>
/* ================= state & helpers ================= */
const $ = (id) => document.getElementById(id);
let currentMedia = null, currentStreams = [], currentUser = null, heroTimer = null;
let uploadCtx = { file: null, thumb: '', duration: 0 };
let reelsObserver = null;

const isUrl = (s) => /^https?:\/\/\S+\.\S+/i.test(s.trim());
const esc = (s) => { const d = document.createElement('div'); d.textContent = s ?? ''; return d.innerHTML; };
const fmtTime = (s) => { if (!isFinite(s) || s < 0) return '0:00'; s = Math.floor(s);
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), x = s%60;
  return h ? h+':'+String(m).padStart(2,'0')+':'+String(x).padStart(2,'0') : m+':'+String(x).padStart(2,'0'); };
const fmtViews = (n) => n == null ? '' : (n >= 1e7 ? (n/1e7).toFixed(1)+'Cr' : n >= 1e5 ? (n/1e5).toFixed(1)+'L' : n >= 1e3 ? (n/1e3).toFixed(0)+'K' : String(n));
const timeAgo = (ts) => { const d = Date.now()/1000 - ts;
  if (d < 60) return 'just now'; if (d < 3600) return Math.floor(d/60)+' minutes ago';
  if (d < 86400) return Math.floor(d/3600)+' hours ago'; if (d < 2592000) return Math.floor(d/86400)+' days ago';
  if (d < 31536000) return Math.floor(d/2592000)+' months ago'; return Math.floor(d/31536000)+' years ago'; };
const timeAgoISO = (iso) => { if (!iso) return ''; try { return timeAgo(Date.parse(iso)/1000); } catch(e) { return ''; } };
const avatarColor = () => { const c = ['#f97316','#22c55e','#3ea6ff','#a855f7','#ec4899','#eab308']; return c[Math.floor(Math.random()*c.length)]; };

function setStatus(msg) { if (!msg) { $('status-bar').classList.add('hidden'); return; }
  $('status-text').textContent = msg; $('status-bar').classList.remove('hidden'); }
function showError(msg) { $('error-text').textContent = msg; $('error-bar').classList.remove('hidden');
  setTimeout(() => $('error-bar').classList.add('hidden'), 8000); }
const lsGet = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch(e) { return d; } };
const lsSet = (k, v) => localStorage.setItem(k, JSON.stringify(v));

/* ================= theme ================= */
function applyTheme(mode) {
  document.documentElement.classList.toggle('light', mode === 'light');
  document.documentElement.classList.toggle('dark', mode !== 'light');
  $('ic-sun').classList.toggle('hidden', mode !== 'light');
  $('ic-moon').classList.toggle('hidden', mode === 'light');
  lsSet('nt-theme', mode);
}
$('btn-theme').addEventListener('click', () => applyTheme(lsGet('nt-theme','dark') === 'light' ? 'dark' : 'light'));
applyTheme(lsGet('nt-theme', 'dark'));

/* ================= router ================= */
function showView(v) {
  ['home','watch','shorts','platform','liked','mine','chats','history'].forEach(x => $('view-'+x).classList.toggle('active', x === v));
  document.querySelectorAll('.sb-item').forEach(it => {
    it.classList.toggle('active', it.dataset.nav === v && v !== 'platform');
  });
  document.querySelectorAll('.bn-item').forEach(it => it.classList.toggle('active', it.dataset.bnav === v));
  if (v === 'home') $('video').pause();
  window.scrollTo({ top: 0 });
  closeSidebarM();
}
document.querySelectorAll('.sb-item[data-nav]').forEach(it =>
  it.addEventListener('click', () => { showView(it.dataset.nav);
    if (it.dataset.nav === 'mine') loadMine();
    if (it.dataset.nav === 'liked') loadLiked();
    if (it.dataset.nav === 'shorts') loadReels();
    if (it.dataset.nav === 'chats') openChats();
    if (it.dataset.nav === 'history') loadHistory(); }));
document.querySelectorAll('.bn-item').forEach(it =>
  it.addEventListener('click', () => {
    const v = it.dataset.bnav;
    if (v === 'upload') { $('upload-modal').classList.remove('hidden'); return; }
    showView(v);
    if (v === 'shorts') loadReels();
    if (v === 'chats') openChats();
    if (v === 'mine') loadMine();
  }));
$('nav-home').addEventListener('click', () => showView('home'));

/* sidebar mobile */
function openSidebarM() { $('sidebar-m').classList.remove('hidden'); $('sb-backdrop').classList.remove('hidden'); }
function closeSidebarM() { $('sidebar-m').classList.add('hidden'); $('sb-backdrop').classList.add('hidden'); }
if (!$('sidebar-m').innerHTML.trim()) { $('sidebar-m').innerHTML = $('sb-full').innerHTML; }
$('btn-menu').addEventListener('click', openSidebarM);
$('sb-backdrop').addEventListener('click', closeSidebarM);
$('sidebar-m').addEventListener('click', e => { if (e.target.closest('.sb-item')) closeSidebarM(); });

/* ================= auth ================= */
function openAuth() { $('auth-modal').classList.remove('hidden'); $('auth-step-1').classList.remove('hidden');
  $('auth-step-2').classList.add('hidden'); $('auth-dev-note').classList.add('hidden'); }
$('auth-zone-btn').addEventListener('click', () => currentUser ? openCopilot() : openAuth());
$('auth-close').addEventListener('click', () => $('auth-modal').classList.add('hidden'));
$('auth-modal').addEventListener('click', e => { if (e.target === $('auth-modal')) $('auth-modal').classList.add('hidden'); });

async function authSendCode() {
  const email = $('auth-email').value.trim(); if (!email) return;
  $('auth-send').disabled = true; $('auth-send').textContent = 'Sending…';
  try {
    const d = await (await fetch('/api/auth/request-otp', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email }) })).json();
    if (d.error) { showError(d.message); return; }
    $('auth-step-1').classList.add('hidden'); $('auth-step-2').classList.remove('hidden');
    $('auth-sub').textContent = 'Code sent to ' + email + ' — check inbox & spam.';
    if (d.dev_otp) { $('auth-dev-note').classList.remove('hidden');
      $('auth-dev-note').textContent = 'SMTP not configured — your code: ' + d.dev_otp; }
  } catch (e) { showError('Send failed: ' + e.message); }
  finally { $('auth-send').disabled = false; $('auth-send').textContent = 'Send code'; }
}
$('auth-send').addEventListener('click', authSendCode);
$('auth-email').addEventListener('keydown', e => { if (e.key === 'Enter') authSendCode(); });
async function authVerify() {
  const email = $('auth-email').value.trim(), code = $('auth-code').value.trim(); if (!code) return;
  $('auth-verify').disabled = true; $('auth-verify').textContent = 'Verifying…';
  try {
    const d = await (await fetch('/api/auth/verify', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, code, name: $('auth-name').value.trim() }) })).json();
    if (d.error) { showError(d.message); return; }
    $('auth-modal').classList.add('hidden'); refreshUser();
  } catch (e) { showError('Verify failed: ' + e.message); }
  finally { $('auth-verify').disabled = false; $('auth-verify').textContent = 'Verify & continue'; }
}
$('auth-verify').addEventListener('click', authVerify);
$('auth-code').addEventListener('keydown', e => { if (e.key === 'Enter') authVerify(); });
$('auth-back').addEventListener('click', () => { $('auth-step-1').classList.remove('hidden');
  $('auth-step-2').classList.add('hidden'); $('auth-dev-note').classList.add('hidden'); });

async function refreshUser() {
  try {
    currentUser = (await (await fetch('/api/auth/me')).json()).user;
    const label = $('auth-zone-label');
    if (currentUser) {
      label.textContent = currentUser.name.split(' ')[0];
      const av = document.querySelector('#auth-zone-btn svg');
      if (av) { av.style.color = avatarColor(); }
      if ($('up-uploader')) { $('up-uploader').value = currentUser.name; $('up-uploader').readOnly = true; }
    } else { label.textContent = 'Sign in'; }
  } catch (e) {}
}
$('auth-zone-btn').addEventListener('dblclick', async () => {
  if (!currentUser) return;
  await fetch('/api/auth/logout', { method: 'POST' }); currentUser = null;
  $('auth-zone-label').textContent = 'Sign in';
  if ($('up-uploader')) { $('up-uploader').value = ''; $('up-uploader').readOnly = false; }
});

/* ================= cards ================= */
function ytCard(v) {
  const card = document.createElement('div');
  card.className = 'yt-card';
  const color = avatarColor();
  const meta = [v.uploader, v.view_count != null ? fmtViews(v.view_count) + ' views' : '',
    v.created_at ? timeAgo(v.created_at) : timeAgoISO(v.published_at)].filter(Boolean).join(' · ');
  const thumb = v.thumbnail
    ? `<img src="${esc(v.thumbnail)}" loading="lazy" onerror="this.style.opacity=.15">`
    : `<div style="width:100%;aspect-ratio:16/9"></div>`;
  card.innerHTML = `
    <div class="thumb">${thumb}
      <span class="dur-badge">${esc(v.duration_label || '')}</span></div>
    <div class="flex gap-3 mt-3">
      <div class="ch-avatar" style="background:${color}">${esc((v.uploader||'N')[0].toUpperCase())}</div>
      <div class="min-w-0 flex-1">
        <p class="v-title">${esc(v.title)}</p>
        <p class="v-meta line-clamp-1">${esc(meta)}</p>
      </div>
    </div>`;
  card.addEventListener('click', () => v.kind === 'local' ? openLocal(v.id) : openRemote(v.url, v));
  return card;
}

function compactCard(v) {
  const card = document.createElement('div');
  card.className = 'yt-card flex gap-2 p-1.5 rounded-xl';
  card.innerHTML = `
    <div class="thumb shrink-0 w-40"><img src="${esc(v.thumbnail || '')}" class="w-full aspect-video object-cover rounded-lg" loading="lazy">
      <span class="dur-badge">${esc(v.duration_label || '')}</span></div>
    <div class="min-w-0 flex-1 py-0.5">
      <p class="v-title">${esc(v.title)}</p>
      <p class="v-meta">${esc(v.uploader || '')}</p>
      <p class="v-meta">${v.view_count != null ? fmtViews(v.view_count) + ' views' : ''}</p>
    </div>`;
  card.addEventListener('click', () => v.kind === 'local' ? openLocal(v.id) : openRemote(v.url, v));
  return card;
}

function showSkeletons() {
  const sk = $('feed-skeleton'); sk.innerHTML = ''; sk.classList.remove('hidden'); $('feed-grid').innerHTML = '';
  for (let i = 0; i < 12; i++) {
    const d = document.createElement('div');
    d.innerHTML = `<div class="skeleton w-full aspect-video"></div>
      <div class="flex gap-3 mt-3"><div class="skeleton rounded-full w-9 h-9"></div>
      <div class="flex-1 space-y-2"><div class="skeleton h-3.5 w-full !rounded"></div><div class="skeleton h-3 w-2/3 !rounded"></div></div></div>`;
    sk.appendChild(d);
  }
}

/* ================= feed ================= */
async function loadFeed(kind, q) {
  showSkeletons();
  $('feed-empty').classList.add('hidden');
  try {
    let items = [];
    if (kind === 'trending') {
      const d = await (await fetch('/api/youtube/trending?region=IN&n=32')).json();
      items = d.items || [];
    } else {
      const queries = { music: 'music video', movie: 'full movie', gaming: 'gaming gameplay',
        live: 'live stream', cricket: 'cricket highlights', comedy: 'comedy video', news: 'news today' };
      const d = await (await fetch('/api/browse?q=' + encodeURIComponent(queries[kind] || kind) + '&n=32')).json();
      items = d.items || [];
    }
    $('feed-skeleton').classList.add('hidden');
    const grid = $('feed-grid'); grid.innerHTML = '';
    if (!items.length) { $('feed-empty').classList.remove('hidden'); return; }
    items.forEach(v => grid.appendChild(ytCard(v)));
  } catch (e) { $('feed-skeleton').classList.add('hidden'); $('feed-empty').classList.remove('hidden');
    showError('Feed load failed: ' + e.message); }
}
document.querySelectorAll('#home-chips .chip-btn').forEach(b => b.addEventListener('click', () => {
  document.querySelectorAll('#home-chips .chip-btn').forEach(x => x.classList.remove('on'));
  b.classList.add('on'); loadFeed(b.dataset.feed);
}));

async function runSearch(q) {
  showView('home'); showSkeletons();
  try {
    const d = await (await fetch('/api/browse?q=' + encodeURIComponent(q) + '&n=32')).json();
    $('feed-skeleton').classList.add('hidden');
    const grid = $('feed-grid'); grid.innerHTML = '';
    const items = d.items || [];
    if (!items.length) { $('feed-empty').classList.remove('hidden'); return; }
    $('feed-title').textContent = 'Results for ' + q; $('feed-title').classList.remove('hidden');
    items.forEach(v => grid.appendChild(ytCard(v)));
  } catch (e) { $('feed-skeleton').classList.add('hidden'); showError('Search failed: ' + e.message); }
}
function handleQuery(v) {
  v = v.trim(); if (!v) return;
  if (isUrl(v)) openRemote(v, null); else runSearch(v);
}
$('main-form').addEventListener('submit', e => { e.preventDefault(); handleQuery($('main-input').value); $('main-input').value = ''; });
$('main-form-m').addEventListener('submit', e => { e.preventDefault(); handleQuery($('main-input-m').value); $('main-input-m').value = ''; });
$('btn-search-m').addEventListener('click', () => { $('main-form-m').classList.toggle('hidden'); $('main-input-m').focus(); });

/* ================= watch ================= */
function setWatchMeta(v, isLocal) {
  $('w-title').textContent = v.title || '';
  $('w-channel').textContent = v.uploader || v.extractor || 'Neura Creator';
  $('w-avatar').textContent = (v.uploader || 'N')[0].toUpperCase();
  $('w-avatar').style.background = avatarColor();
  $('w-subs').textContent = isLocal ? timeAgo(v.created_at) : (v.extractor ? v.extractor + ' source' : '');
  $('w-views').textContent = (v.view_count != null ? fmtViews(v.view_count) + ' views' : (isLocal ? v.views + ' views' : '')) +
    (isLocal ? ' · ' + timeAgo(v.created_at) : (v.duration_label ? ' · ' + v.duration_label : ''));
  $('w-desc').textContent = (v.description || '').slice(0, 600) || 'No description.';
  $('w-likes').textContent = isLocal ? (v.likes + ' likes') : 'Like';
}

async function openLocal(id) {
  setStatus('Loading video…');
  showView('watch');
  try {
    const v = await (await fetch('/api/videos/' + id)).json();
    if (v.error) throw new Error(v.message);
    currentMedia = { ...v, isLocal: true, ytId: null };
    currentStreams = [];
    setWatchMeta(v, true);
    const sel = $('quality-select'); sel.innerHTML = '';
    sel.appendChild(new Option('original', 'original'));
    playSrc((v.filename && v.filename.startsWith('http')) ? v.filename : '/media/' + v.filename, true);
    loadComments(v.id);
    $('w-sub').textContent = lsGet('nt-subs', []).includes(v.uploader) ? 'Subscribed' : 'Subscribe';
    loadRelated(v.title, v);
    setStatus(null);
  } catch (e) { setStatus(null); showError(e.message); }
}

function ytEmbedFallback(url) {
  const m = url.match(/[?&]v=([\w-]{6,})/) || url.match(/youtu\.be\/([\w-]{6,})/) || url.match(/embed\/([\w-]{6,})/);
  const ytId = m && m[1];
  if (!ytId) return false;
  const video = $('video'); video.pause(); video.removeAttribute('src');
  $('iframe-wrap').classList.remove('hidden');
  video.classList.add('hidden');
  $('embed-note').classList.remove('hidden');
  $('embed-note').innerHTML = 'Direct stream blocked — playing via YouTube embed. <a class="underline ml-1" href="' + esc(url) + '" target="_blank" rel="noopener">open on YouTube</a>';
  $('yt-frame').src = 'https://www.youtube-nocookie.com/embed/' + ytId + '?autoplay=1&rel=0';
  currentStreams = [];
  const sel = $('quality-select'); sel.innerHTML = '';
  sel.appendChild(new Option('embed', 'embed'));
  return true;
}
function useNativePlayer() {
  $('iframe-wrap').classList.add('hidden');
  $('embed-note').classList.add('hidden');
  $('video').classList.remove('hidden');
  $('yt-frame').src = '';
}
function recordHistory(v) {
  try {
    const h = lsGet('nt-history', []);
    h.unshift({ title: v.title, uploader: v.uploader, url: v.webpage_url || v.url || '',
      thumbnail: v.thumbnail, view_count: v.view_count, duration_label: v.duration_label,
      published_at: '', kind: v.kind || 'youtube' });
    lsSet('nt-history', h.slice(0, 60));
  } catch (e) {}
}
async function openRemote(url, hint) {
  setStatus('Fetching stream (yt-dlp)…');
  useNativePlayer();
  showView('watch');
  if (hint && hint.title) {
    $('w-title').textContent = hint.title;
    $('w-channel').textContent = hint.uploader || 'Loading…';
    $('w-avatar').textContent = (hint.uploader || 'N')[0].toUpperCase();
    $('w-views').textContent = hint.view_count != null ? fmtViews(hint.view_count) + ' views' : '';
    $('w-desc').textContent = 'Loading stream…';
  }
  try {
    const data = await (await fetch('/api/stream?url=' + encodeURIComponent(url))).json();
    if (data.error) throw new Error(data.message || data.error);
    const ytId = (url.match(/[?&]v=([\w-]{6,})/) || url.match(/youtu\.be\/([\w-]{6,})/) || [])[1] || null;
    currentMedia = { ...data, isLocal: false, ytId, id: ytId ? 'yt:' + ytId : null, likes: 0 };
    currentStreams = (data.streams || []).filter(s => s.progressive);
    setWatchMeta({ ...data, ...((hint && !data.thumbnail) ? { thumbnail: hint.thumbnail } : {}) }, false);
    const sel = $('quality-select'); sel.innerHTML = '';
    currentStreams.forEach((s, i) => sel.appendChild(new Option(s.label, i)));
    const best = currentStreams[0] || data.best;
    if (best) playSrc('/api/proxy?url=' + encodeURIComponent(best.url), true);
    loadComments(currentMedia.id || data.webpage_url);
    $('w-sub').textContent = lsGet('nt-subs', []).includes(data.uploader) ? 'Subscribed' : 'Subscribe';
    loadRelated(data.title, data);
    recordHistory(data);
    setStatus(null);
  } catch (e) {
    setStatus(null);
    if (ytEmbedFallback(url)) {
      currentMedia = { isLocal: false, ytId: (url.match(/[?&]v=([\w-]{6,})/) || [])[1] || null,
        id: null, title: hint?.title || 'YouTube video', uploader: hint?.uploader || 'YouTube',
        description: '', duration_label: '', view_count: hint?.view_count, thumbnail: hint?.thumbnail,
        streams: [], webpage_url: url };
      setWatchMeta(currentMedia, false);
      loadComments(null);
      loadRelated(hint?.title || 'trending', {});
      recordHistory({ ...currentMedia, webpage_url: url });
    } else {
      showError('Playback failed: ' + e.message);
    }
  }
}

function playSrc(src, reset) {
  useNativePlayer();
  const v = $('video'); const t = reset ? 0 : v.currentTime;
  v.src = src; v.currentTime = t;
  v.play().catch(() => {
    // browser blocked unmuted autoplay -> muted autoplay + unmute hint
    v.muted = true; syncMute();
    v.play().catch(() => {});
    const hint = document.createElement('button');
    hint.id = 'unmute-hint';
    hint.className = 'absolute top-3 left-3 z-20 px-3 py-2 rounded-full text-xs font-semibold';
    hint.style.cssText = 'background:rgba(0,0,0,.8);color:#fff;border:1px solid rgba(255,255,255,.3)';
    hint.textContent = 'Tap to unmute';
    hint.addEventListener('click', () => {
      v.muted = false; syncMute(); hint.remove();
    });
    $('player-shell').appendChild(hint);
    setTimeout(() => hint.remove(), 12000);
  });
}
$('quality-select').addEventListener('change', e => {
  if (!currentMedia || currentMedia.isLocal) return;
  const s = currentStreams[+e.target.value];
  if (s) playSrc('/api/proxy?url=' + encodeURIComponent(s.url), false);
});

async function loadRelated(title, self) {
  const box = $('related-list'); box.innerHTML = '';
  try {
    const d = await (await fetch('/api/browse?q=' + encodeURIComponent(title.split(' ').slice(0, 5).join(' ')) + '&n=14')).json();
    (d.items || []).filter(v => v.url !== (self.url || self.webpage_url)).slice(0, 12).forEach(v => box.appendChild(compactCard(v)));
  } catch (e) {}
  try {
    const mine = await (await fetch('/api/videos')).json();
    mine.videos.slice(0, 2).forEach(v => { if (v.id !== self.id) box.appendChild(compactCard({ ...v, kind: 'local', url: '' })); });
  } catch (e) {}
}

/* like / dislike / subscribe / share / download */
function toggleLike() {
  if (!currentMedia) return;
  const liked = lsGet('nt-liked', []);
  if (currentMedia.isLocal && currentMedia.id) {
    fetch('/api/videos/' + currentMedia.id + '/like', { method: 'POST' })
      .then(r => r.json()).then(d => { if (d.likes != null) { $('w-likes').textContent = d.likes + ' likes'; currentMedia.likes = d.likes; } })
      .catch(() => {});
  } else {
    const i = liked.findIndex(x => x.title === currentMedia.title);
    if (i >= 0) { liked.splice(i, 1); $('w-likes').textContent = 'Like'; }
    else { liked.unshift({ title: currentMedia.title, uploader: currentMedia.uploader, url: currentMedia.webpage_url,
      thumbnail: currentMedia.thumbnail, view_count: currentMedia.view_count, duration_label: currentMedia.duration_label,
      published_at: '', kind: 'youtube' }); $('w-likes').textContent = 'Liked ✓'; }
    lsSet('nt-liked', liked.slice(0, 60));
  }
}
$('w-like').addEventListener('click', toggleLike);
$('w-dislike').addEventListener('click', () => showError('Noted! (feedback saved locally)'));
$('w-sub').addEventListener('click', () => {
  const subs = lsGet('nt-subs', []); const name = currentMedia?.uploader; if (!name) return;
  const i = subs.indexOf(name);
  if (i >= 0) { subs.splice(i, 1); $('w-sub').textContent = 'Subscribe'; }
  else { subs.push(name); $('w-sub').textContent = 'Subscribed'; }
  lsSet('nt-subs', subs);
});
$('w-share').addEventListener('click', async () => {
  const url = currentMedia?.webpage_url || location.href;
  if (navigator.share) { try { await navigator.share({ title: currentMedia?.title || 'NeuraStream', url }); return; } catch(e){} }
  try { await navigator.clipboard.writeText(url); showError && setStatus('Link copied!'); setTimeout(setStatus, 1500, null); }
  catch (e) { showError('Copy failed: ' + url); }
});
$('w-download').addEventListener('click', () => {
  if (!currentMedia || currentMedia.isLocal) {
    if (currentMedia?.isLocal) { window.open('/media/' + currentMedia.filename, '_blank'); return; }
    return;
  }
  const audio = (currentMedia.streams || []).find(s => !s.progressive && s.acodec !== 'none' && s.vcodec === 'none')
    || (currentMedia.streams || []).find(s => s.progressive);
  if (!audio) { showError('No downloadable stream found.'); return; }
  window.open('/api/proxy?url=' + encodeURIComponent(audio.url) + '&dl=1', '_blank');
});

/* ================= comments (own + YouTube videos) ================= */
async function loadComments(cid) {
  const box = $('c-list'); box.innerHTML = '';
  if (!cid) return;
  try {
    const d = await (await fetch('/api/videos/' + encodeURIComponent(cid) + '/comments')).json();
    $('c-count').textContent = (d.comments || []).length;
    (d.comments || []).forEach(c => {
      const el = document.createElement('div');
      el.className = 'msg-in flex gap-3';
      el.innerHTML = `<div class="ch-avatar" style="width:32px;height:32px;font-size:12px;background:${avatarColor()}">${esc((c.author||'?')[0].toUpperCase())}</div>
        <div class="min-w-0"><p class="text-xs font-medium">${esc(c.author)} <span style="color:var(--muted);font-weight:400">· ${timeAgo(c.created_at)}</span></p>
        <p class="text-sm mt-0.5 break-words">${esc(c.body)}</p></div>`;
      box.appendChild(el);
    });
  } catch (e) { $('c-count').textContent = '0'; }
}
$('c-form').addEventListener('submit', async e => {
  e.preventDefault();
  const body = $('c-body').value.trim(); if (!body) return;
  const cid = currentMedia?.id;
  if (!cid) { showError('Load a video first.'); return; }
  try {
    const d = await (await fetch('/api/videos/' + encodeURIComponent(cid) + '/comments', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ author: (currentUser && currentUser.name) || $('c-author').value.trim() || 'Guest', body }) })).json();
    if (d.ok) { $('c-body').value = ''; loadComments(cid); }
  } catch (err) { showError('Comment failed: ' + err.message); }
});

/* ================= player controls ================= */
const video = $('video');
$('big-play').addEventListener('click', () => video.paused ? video.play() : video.pause());
$('btn-play').addEventListener('click', () => video.paused ? video.play() : video.pause());
video.addEventListener('click', () => video.paused ? video.play() : video.pause());
video.addEventListener('play', () => { $('ic-play').classList.add('hidden'); $('ic-pause').classList.remove('hidden'); });
video.addEventListener('pause', () => { $('ic-pause').classList.add('hidden'); $('ic-play').classList.remove('hidden'); });
const seek = $('seek');
let seeking = false;
const paintSeek = (el) => el.style.setProperty('--fill', ((el.value - el.min) / (el.max - el.min)) * 100 + '%');
video.addEventListener('timeupdate', () => {
  if (!seeking) { seek.value = video.currentTime * (1000 / (video.duration || 1)); paintSeek(seek); }
  $('t-now').textContent = fmtTime(video.currentTime);
});
video.addEventListener('loadedmetadata', () => { $('t-dur').textContent = fmtTime(video.duration); });
seek.addEventListener('input', () => { seeking = true; paintSeek(seek); });
seek.addEventListener('change', () => { video.currentTime = seek.value * (video.duration || 0) / 1000; seeking = false; });
paintSeek(seek);
$('btn-mute').addEventListener('click', () => { video.muted = !video.muted; syncMute(); });
function syncMute() { const m = video.muted;
  $('ic-vol').classList.toggle('hidden', m); $('ic-muted').classList.toggle('hidden', !m); }
$('btn-pip').addEventListener('click', async () => {
  try { if (document.pictureInPictureElement) await document.exitPictureInPicture(); else await video.requestPictureInPicture(); }
  catch (e) { showError('PiP unavailable here.'); }
});
$('btn-fs').addEventListener('click', () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else if ($('player-shell').requestFullscreen) $('player-shell').requestFullscreen();
});
video.addEventListener('error', () => {
  const hint = document.getElementById('unmute-hint'); if (hint) hint.remove();
  const url = currentMedia && currentMedia.webpage_url;
  if (url && /youtu/.test(url)) {
    if (ytEmbedFallback(url)) return;
  }
  showError('Stream expired or blocked. Try again, or use the source link.');
});
video.addEventListener('waiting', () => { if (!$('iframe-wrap') || $('iframe-wrap').classList.contains('hidden')) setStatus('Buffering…'); });
video.addEventListener('playing', () => setStatus(null));

document.addEventListener('keydown', e => {
  if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
  if (e.code === 'Space') { e.preventDefault(); video.paused ? video.play() : video.pause(); }
  if (e.code === 'ArrowRight') video.currentTime = Math.min(video.duration || 1e9, video.currentTime + 5);
  if (e.code === 'ArrowLeft') video.currentTime = Math.max(0, video.currentTime - 5);
});

/* ================= shorts ================= */
async function loadReels() {
  const track = $('reel-track');
  try {
    const data = await (await fetch('/api/videos')).json();
    const vids = data.videos || [];
    $('reels-empty').classList.toggle('hidden', !!vids.length);
    track.innerHTML = '';
    vids.forEach(v => track.appendChild(buildReel(v)));
    if (reelsObserver) reelsObserver.disconnect();
    reelsObserver = new IntersectionObserver(entries => {
      entries.forEach(en => { const el = en.target.querySelector('video');
        if (en.isIntersecting) el.play().catch(()=>{}); else el.pause(); });
    }, { root: track, threshold: 0.6 });
    track.querySelectorAll('.reel-item').forEach(el => reelsObserver.observe(el));
  } catch (e) {}
}
function buildReel(v) {
  const item = document.createElement('div');
  item.className = 'reel-item relative w-full h-full flex items-center justify-center bg-black';
  item.innerHTML = `
    <video src="${esc((v.filename && v.filename.startsWith('http')) ? v.filename : '/media/' + v.filename)}" class="h-full max-h-full w-auto max-w-full object-contain" loop muted playsinline preload="metadata" ${v.thumb ? `poster="${esc(v.thumb)}"` : ''}></video>
    <div class="absolute inset-x-0 bottom-0 p-5 pb-6 bg-gradient-to-t from-black/85 via-black/30 to-transparent">
      <div class="max-w-[75%]"><p class="text-sm font-bold">${esc(v.title)}</p>
      <p class="text-xs text-gray-400 mt-0.5">${esc(v.uploader)} · ${fmtViews(v.views)} views</p></div>
    </div>
    <div class="absolute right-3 bottom-24 flex flex-col items-center gap-4">
      <button class="reel-like w-11 h-11 rounded-full bg-white/10 border border-white/20 grid place-items-center hover:scale-110 transition" title="Like">
        <svg class="w-5 h-5 text-pink-400" fill="currentColor" viewBox="0 0 24 24"><path d="M18.77 11h-4.23l1.52-4.94A1.54 1.54 0 0 0 14.6 4h-.2a1.54 1.54 0 0 0-1.34.77L8.92 12H6V4H4v16h14a2 2 0 0 0 1.95-1.55l1.66-6A2 2 0 0 0 19.6 11h-.83Z"/></svg>
      </button>
      <button class="reel-open w-11 h-11 rounded-full bg-white/10 border border-white/20 grid place-items-center hover:scale-110 transition" title="Open & comment">
        <svg class="w-5 h-5 text-white" fill="currentColor" viewBox="0 0 24 24"><path d="M21 12a8 8 0 0 1-8 8H5l-2 2V12a8 8 0 0 1 8-8h2a8 8 0 0 1 8 8Z"/></svg>
      </button>
    </div>`;
  const vid = item.querySelector('video');
  item.addEventListener('click', e => {
    if (e.target.closest('button')) return;
    if (vid.paused) { vid.muted = false; vid.play(); } else vid.pause();
  });
  item.querySelector('.reel-like').addEventListener('click', async () => {
    try { await fetch('/api/videos/' + v.id + '/like', { method: 'POST' }); } catch (e) {}
  });
  item.querySelector('.reel-open').addEventListener('click', () => openLocal(v.id));
  return item;
}
async function searchShorts(q) {
  if (!q.trim()) return;
  setStatus('Searching Shorts…');
  try {
    const d = await (await fetch('/api/browse?q=' + encodeURIComponent(q + ' shorts') + '&n=40')).json();
    const items = (d.items || []).filter(it => (it.duration || 0) <= 300);
    const grid = $('shorts-grid'); grid.innerHTML = '';
    if (!items.length) { grid.innerHTML = '<p class="col-span-full text-sm py-4" style="color:var(--muted)">No shorts found.</p>'; setStatus(null); return; }
    items.forEach(it => {
      const c = document.createElement('div');
      c.className = 'yt-card relative rounded-xl overflow-hidden cursor-pointer bg-black';
      c.innerHTML = `
        ${it.thumbnail ? `<img src="${esc(it.thumbnail)}" class="w-full aspect-[9/16] object-cover" loading="lazy">` : '<div class="w-full aspect-[9/16]"></div>'}
        <span class="dur-badge">${esc(it.duration_label || '')}</span>
        <div class="absolute inset-x-0 bottom-0 p-2 bg-gradient-to-t from-black/90 to-transparent">
          <p class="text-[11px] font-bold leading-tight line-clamp-2">${esc(it.title)}</p></div>`;
      c.addEventListener('click', () => openRemote(it.url, it));
      grid.appendChild(c);
    });
    setStatus(null);
  } catch (e) { setStatus(null); showError('Shorts search failed: ' + e.message); }
}
$('shorts-search').addEventListener('keydown', e => { if (e.key === 'Enter') $('shorts-btn').click(); });
$('shorts-btn').addEventListener('click', () => searchShorts($('shorts-search').value));
$('reel-url-input').addEventListener('change', async () => {
  const url = $('reel-url-input').value.trim(); if (!isUrl(url)) return;
  setStatus('Adding reel…');
  try {
    const data = await (await fetch('/api/stream?url=' + encodeURIComponent(url))).json();
    if (data.error) throw new Error(data.message);
    const best = (data.streams || []).find(s => s.progressive) || data.best;
    const item = document.createElement('div');
    item.className = 'reel-item relative w-full h-full flex items-center justify-center bg-black';
    item.innerHTML = `
      <video src="/api/proxy?url=${encodeURIComponent(best.url)}" class="h-full w-auto max-w-full object-contain" loop muted playsinline autoplay controls></video>
      <div class="absolute inset-x-0 bottom-0 p-5 pb-6 bg-gradient-to-t from-black/85 to-transparent pointer-events-none">
        <p class="text-sm font-bold">${esc(data.title)}</p>
        <p class="text-xs text-gray-400 mt-0.5">${esc(data.extractor)} · ${esc(data.uploader)}</p></div>`;
    $('reel-track').prepend(item); $('reel-url-input').value = '';
    $('reels-empty').classList.add('hidden'); setStatus(null);
  } catch (e) { setStatus(null); showError('Reel add failed: ' + e.message); }
});

/* ================= platform views ================= */
/* ================= explore ================= */
document.querySelectorAll('.sb-item[data-explore]').forEach(it =>
  it.addEventListener('click', () => {
    showView('home');
    document.querySelectorAll('#home-chips .chip-btn').forEach(x => x.classList.remove('on'));
    loadFeed(it.dataset.explore === 'trending' ? 'trending' : it.dataset.explore);
    document.querySelector(`#home-chips .chip-btn[data-feed="${it.dataset.explore}"]`)?.classList.add('on');
  }));

/* ================= liked / mine ================= */
function loadLiked() {
  const grid = $('liked-grid'); grid.innerHTML = '';
  const liked = lsGet('nt-liked', []);
  $('liked-empty').classList.toggle('hidden', !!liked.length);
  liked.forEach(v => grid.appendChild(ytCard({ ...v, kind: 'youtube' })));
}
async function loadMine() {
  const grid = $('mine-grid'); grid.innerHTML = '';
  try {
    const d = await (await fetch('/api/videos')).json();
    $('mine-empty').classList.toggle('hidden', !!(d.videos || []).length);
    (d.videos || []).forEach(v => grid.appendChild(ytCard({ ...v, kind: 'local', url: '' })));
  } catch (e) {}
}
$('mine-upload-btn').addEventListener('click', () => $('upload-modal').classList.remove('hidden'));

/* ================= upload ================= */
const openModal = () => $('upload-modal').classList.remove('hidden');
const closeModal = () => $('upload-modal').classList.add('hidden');
$('btn-upload').addEventListener('click', openModal);
$('upload-close').addEventListener('click', closeModal);
$('upload-modal').addEventListener('click', e => { if (e.target === $('upload-modal')) closeModal(); });
const dz = $('drop-zone');
dz.addEventListener('click', () => $('file-input').click());
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag'); });
dz.addEventListener('dragleave', () => dz.classList.remove('drag'));
dz.addEventListener('drop', e => { e.preventDefault(); dz.classList.remove('drag'); if (e.dataTransfer.files[0]) pickFile(e.dataTransfer.files[0]); });
$('file-input').addEventListener('change', e => { if (e.target.files[0]) pickFile(e.target.files[0]); });
function pickFile(f) {
  if (!f.type.startsWith('video/')) { showError('Video file choose karo.'); return; }
  if (f.size > 500 * 1024 * 1024) { showError('500 MB se zyada hai.'); return; }
  if (f.size > 200 * 1024 * 1024 && !window.__cloud) { showError('Bina cloud storage 200 MB tak hi hota hai (Supabase env vars set karo).'); return; }
  uploadCtx = { file: f, thumb: '', duration: 0 };
  $('drop-title').textContent = f.name + ' (' + (f.size/1048576).toFixed(1) + ' MB)';
  $('up-submit').disabled = false;
  const tv = $('thumb-video');
  tv.src = URL.createObjectURL(f); tv.muted = true;
  tv.onloadeddata = () => { try { tv.currentTime = Math.min(1, tv.duration / 3); } catch (e) {} };
  tv.onseeked = () => {
    try {
      const c = document.createElement('canvas');
      c.width = 320; c.height = Math.round(320 * (tv.videoHeight / (tv.videoWidth || 1))) || 180;
      c.getContext('2d').drawImage(tv, 0, 0, c.width, c.height);
      uploadCtx.thumb = c.toDataURL('image/jpeg', 0.72);
      const pv = $('thumb-preview'); pv.src = uploadCtx.thumb; pv.classList.remove('hidden');
    } catch (e) {}
    uploadCtx.duration = tv.duration || 0;
  };
}
$('up-submit').addEventListener('click', () => {
  if (!uploadCtx.file) return;
  const fd = new FormData();
  fd.append('file', uploadCtx.file);
  fd.append('title', $('up-title').value.trim() || uploadCtx.file.name);
  fd.append('description', $('up-desc').value.trim());
  fd.append('uploader', $('up-uploader').value.trim());
  fd.append('duration', String(Math.round(uploadCtx.duration || 0)));
  fd.append('thumbnail', uploadCtx.thumb || '');
  const btn = $('up-submit'); btn.disabled = true; btn.textContent = 'Uploading…';
  $('up-progress').classList.remove('hidden'); $('up-bar').style.width = '0%'; $('up-pct').textContent = '0%';
  const xhr = new XMLHttpRequest();
  xhr.open('POST', '/api/upload');
  xhr.upload.onprogress = e => { if (e.lengthComputable) { const p = Math.round(e.loaded / e.total * 100);
    $('up-bar').style.width = p + '%'; $('up-pct').textContent = p + '%'; } };
  xhr.onload = () => {
    btn.disabled = false; btn.textContent = 'Upload';
    try {
      const d = JSON.parse(xhr.responseText);
      if (d.error) { showError(d.message); return; }
      closeModal();
      $('up-title').value = ''; $('up-desc').value = '';
      if (!currentUser) $('up-uploader').value = '';
      $('thumb-preview').classList.add('hidden');
      $('drop-title').textContent = 'Drag & drop or click to select';
      uploadCtx = { file: null, thumb: '', duration: 0 }; $('up-submit').disabled = true;
      openLocal(d.video.id);
    } catch (e) { showError('Upload response error.'); }
  };
  xhr.onerror = () => { btn.disabled = false; btn.textContent = 'Upload'; showError('Network error.'); };
  xhr.send(fd);
});

/* ================= co-pilot ================= */
function openCopilot() { $('copilot-panel').classList.remove('hidden'); }
$('btn-copilot').addEventListener('click', openCopilot);
$('copilot-close').addEventListener('click', () => $('copilot-panel').classList.add('hidden'));
function appendMsg(role, text, sources) {
  const log = $('chat-log');
  const wrap = document.createElement('div');
  wrap.className = 'msg-in flex gap-2.5' + (role === 'user' ? ' flex-row-reverse' : '');
  const body = text.split('\n').map(l => esc(l)).join('<br>');
  const srcHtml = (sources && sources.length)
    ? '<div class="mt-2 pt-2 space-y-1" style="border-color:var(--line);border-top:1px solid">' + sources.map(s =>
        `<a href="${esc(s.url)}" target="_blank" rel="noopener" class="block text-[11px] truncate">${esc(s.title || s.url)}</a>`).join('') + '</div>'
    : '';
  wrap.innerHTML = role === 'user'
    ? `<div class="rounded-2xl rounded-tr-sm px-3.5 py-2.5 max-w-[85%]" style="background:var(--accent);color:#fff">${body}</div>`
    : `<div class="ch-avatar" style="width:28px;height:28px;font-size:11px;background:linear-gradient(135deg,#3ea6ff,#ff0033)">AI</div>
       <div class="rounded-2xl rounded-tl-sm px-3.5 py-2.5 max-w-[85%]" style="background:var(--bg2)">${body}${srcHtml}</div>`;
  log.appendChild(wrap); log.scrollTop = log.scrollHeight;
  return wrap;
}
async function sendChat(q) {
  if (!q.trim()) return;
  appendMsg('user', q);
  const holder = appendMsg('ai', '…');
  try {
    const data = await (await fetch('/api/chat', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: q,
        video: currentMedia ? { title: currentMedia.title, uploader: currentMedia.uploader,
          description: currentMedia.description || '', duration_label: currentMedia.duration_label,
          webpage_url: currentMedia.webpage_url } : null,
        search_results: []
      }) })).json();
    holder.remove();
    appendMsg('ai', data.answer || '…', data.sources);
  } catch (err) { holder.remove(); appendMsg('ai', 'Error: ' + err.message); }
}
$('chat-form').addEventListener('submit', e => { e.preventDefault(); const v = $('chat-input').value; $('chat-input').value = ''; sendChat(v); });
document.querySelectorAll('.chat-chip').forEach(c => c.addEventListener('click', () => {
  let q = c.textContent.trim();
  if (q.includes('related') && currentMedia) q = 'search ' + currentMedia.title + ' video';
  sendChat(q);
}));

/* ================= in-app chats (WhatsApp-style DMs) ================= */
let chatPeerId = null, chatPollTimer = null;

async function openChats() {
  if (!currentUser) { openAuth(); return; }
  $('chat-me-name').textContent = '@' + currentUser.name.split(' ')[0].toLowerCase();
  await loadChatUsers();
  if (chatPollTimer) clearInterval(chatPollTimer);
  chatPollTimer = setInterval(async () => {
    if (chatPeerId && $('view-chats').classList.contains('active')) refreshChat();
  }, 3000);
}

async function loadChatUsers() {
  const box = $('chat-users');
  try {
    const d = await (await fetch('/api/chat/users')).json();
    if (d.error) { box.innerHTML = '<p class="p-6 text-center text-xs" style="color:var(--muted)">' + esc(d.message) + '</p>'; return; }
    box.innerHTML = '';
    if (!d.users.length) {
      box.innerHTML = '<p class="p-6 text-center text-xs" style="color:var(--muted)">Koi aur user nahi hai — kisi aur device/browser se sign up karo (doosra email), phir yahan chat kar sakte ho.</p>';
      return;
    }
    d.users.forEach(u => {
      const el = document.createElement('div');
      el.className = 'flex items-center gap-3 px-4 py-3 cursor-pointer hover:brightness-125 transition';
      el.style.borderBottom = '1px solid var(--line)';
      el.innerHTML = `<div class="ch-avatar" style="width:40px;height:40px;background:${avatarColor()}">${esc(u.name[0].toUpperCase())}</div>
        <div class="min-w-0 flex-1"><p class="text-sm font-medium truncate">${esc(u.name)}</p>
        <p class="text-xs truncate" style="color:var(--muted)">${esc(u.last)}</p></div>
        <span class="text-[10px]" style="color:var(--muted)">${u.last_at ? timeAgo(u.last_at) : ''}</span>`;
      el.addEventListener('click', () => openConversation(u));
      box.appendChild(el);
    });
  } catch (e) { box.innerHTML = '<p class="p-6 text-center text-xs" style="color:var(--muted)">Load failed: ' + esc(e.message) + '</p>'; }
}

async function openConversation(u) {
  chatPeerId = u.id;
  $('chat-peer-name').textContent = u.name;
  $('chat-peer-mail').textContent = u.email || '';
  $('chat-peer-avatar').textContent = u.name[0].toUpperCase();
  await refreshChat();
}

async function refreshChat() {
  if (!chatPeerId) return;
  try {
    const d = await (await fetch('/api/chat/messages?with_user=' + chatPeerId)).json();
    if (d.error) return;
    const box = $('chat-msgs');
    const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 60;
    box.innerHTML = '';
    (d.messages || []).forEach(m => {
      const el = document.createElement('div');
      el.className = 'bubble ' + (m.mine ? 'me' : 'them');
      el.textContent = m.body;
      const t = document.createElement('span');
      t.className = 'btime'; t.textContent = new Date(m.created_at * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      el.appendChild(t);
      box.appendChild(el);
    });
    if (stick) box.scrollTop = box.scrollHeight;
  } catch (e) {}
}

$('dm-form').addEventListener('submit', async e => {
  e.preventDefault();
  const body = $('dm-input').value.trim();
  if (!body || !chatPeerId) return;
  $('dm-input').value = '';
  try {
    const d = await (await fetch('/api/chat/send', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: chatPeerId, body }) })).json();
    if (d.error) { showError(d.message); return; }
    refreshChat();
  } catch (err) { showError('Send failed: ' + err.message); }
});

/* ================= reels discovery (Instagram/Facebook, no API) ================= */
/* shorts search: tab-aware (binding above, after Enter handler) */

/* ================= history ================= */
function loadHistory() {
  const grid = $('history-grid'); grid.innerHTML = '';
  const h = lsGet('nt-history', []);
  $('history-empty').classList.toggle('hidden', !!h.length);
  h.forEach(v => grid.appendChild(ytCard({ ...v, kind: 'youtube' })));
}

async function detectCloud() {
  try { const d = await (await fetch('/api/storage-mode')).json(); window.__cloud = d.cloud; } catch (e) { window.__cloud = false; }
}
detectCloud();

/* ================= boot ================= */
refreshUser();
loadFeed('trending');
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FRONTEND)


@app.get("/api/storage-mode")
async def api_storage_mode():
    return {"cloud": bool(SUPABASE_URL and SUPABASE_KEY), "max_mb": 500 if (SUPABASE_URL and SUPABASE_KEY) else 200}


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "version": "7.0.0"}


# --------------------------------------------------------------------------- #
#  Entrypoint — Render binds $PORT dynamically                                 #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
