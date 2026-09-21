"""
================================================================================
 Neura Stream — Hybrid Video Platform (single file, zero API keys)
 YouTube-style home + Instagram-style reels + own video uploads + multi-platform
 extraction (YouTube / Instagram / Facebook / X / Telegram / WhatsApp links).
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

 NO API KEYS. NO PAID SERVICES. NO BUILD STEP. ONE FILE.
================================================================================
"""

import asyncio
import base64
import mimetypes
import os
import re
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
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
#  Application                                                                 #
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="Neura Stream",
    description="Hybrid video platform: uploads, reels, multi-platform extraction, "
    "live web search and an AI co-pilot — zero API keys.",
    version="3.0.0",
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

MAX_UPLOAD_BYTES = 200 * 1024 * 1024  # 200 MB cap (ephemeral free-tier disk)

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
        "webpage_url": f"/media/{row['filename']}",
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
def api_add_comment(vid: str, payload: CommentIn):
    with _db() as conn:
        exists = conn.execute("SELECT 1 FROM videos WHERE id = ?", (vid,)).fetchone()
        if not exists:
            return {"error": "not_found", "message": "Video not found."}
        conn.execute(
            "INSERT INTO comments (video_id, author, body, created_at) VALUES (?, ?, ?, ?)",
            (vid, payload.author.strip() or "Guest", payload.body.strip(), time.time()),
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


@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(""),
    uploader: str = Form("Guest Creator"),
    duration: float = Form(0.0),
    thumbnail: str = Form(""),
):
    title = title.strip()[:150] or file.filename or "Untitled upload"
    uploader = uploader.strip()[:60] or "Guest Creator"
    description = description.strip()[:2000]

    if file.content_type and not file.content_type.startswith("video/"):
        return {"error": "bad_type", "message": "Only video files are supported."}

    vid = uuid.uuid4().hex[:12]
    ext = Path(file.filename or "video.mp4").suffix.lower() or ".mp4"
    if not re.fullmatch(r"\.[a-z0-9]{2,5}", ext):
        ext = ".mp4"
    dest = UPLOAD_DIR / f"{vid}{ext}"
    written = 0
    try:
        with dest.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)  # 1 MB chunks -> flat RAM
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="File exceeds the 200 MB limit.")
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

    thumb_name = _save_data_url_thumbnail(thumbnail, vid)
    with _db() as conn:
        conn.execute(
            "INSERT INTO videos (id, title, description, uploader, filename, thumb, duration, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (vid, title, description, uploader, dest.name, thumb_name, float(duration or 0), time.time()),
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
    return StreamingResponse(relay(), status_code=upstream.status_code, headers=passthrough)


# --------------------------------------------------------------------------- #
#  Frontend — hybrid platform UI (embedded HTML/CSS/JS)                         #
# --------------------------------------------------------------------------- #

FRONTEND = r"""
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Neura Stream — watch, upload & share</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = { darkMode:'class', theme:{ extend:{
  fontFamily:{ sans:['Inter','system-ui','sans-serif'] },
  keyframes:{
    fadeUp:{ '0%':{opacity:0,transform:'translateY(10px)'},'100%':{opacity:1,transform:'translateY(0)'} },
    pulseGlow:{ '0%,100%':{opacity:.5},'50%':{opacity:.9} },
  },
  animation:{ fadeUp:'fadeUp .45s ease-out both', pulseGlow:'pulseGlow 2.4s ease-in-out infinite' },
}}}
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  ::-webkit-scrollbar{width:8px;height:8px}
  ::-webkit-scrollbar-track{background:transparent}
  ::-webkit-scrollbar-thumb{background:rgba(217,70,239,.25);border-radius:8px}
  ::-webkit-scrollbar-thumb:hover{background:rgba(217,70,239,.45)}
  body{background:#07070d}
  .glass{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);backdrop-filter:blur(18px)}
  .glow-blob{position:fixed;border-radius:9999px;filter:blur(110px);z-index:-1;pointer-events:none}
  input[type=range].forge-seek{-webkit-appearance:none;appearance:none;height:5px;border-radius:99px;
    background:linear-gradient(90deg,#d946ef var(--fill,0%),rgba(255,255,255,.14) var(--fill,0%));cursor:pointer}
  input[type=range].forge-seek::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:99px;
    background:#fff;box-shadow:0 0 12px rgba(217,70,239,.9);transition:transform .15s}
  input[type=range].forge-seek::-webkit-slider-thumb:hover{transform:scale(1.25)}
  input[type=range].forge-seek::-moz-range-thumb{width:14px;height:14px;border:none;border-radius:99px;background:#fff;box-shadow:0 0 12px rgba(217,70,239,.9)}
  .msg-in{animation:fadeUp .3s ease-out both}
  .chip{transition:all .18s}
  .chip:hover{transform:translateY(-1px);background:rgba(217,70,239,.15);border-color:rgba(217,70,239,.4)}
  .card-hover{transition:all .2s ease}
  .card-hover:hover{transform:translateY(-3px);border-color:rgba(217,70,239,.35);box-shadow:0 12px 40px -12px rgba(217,70,239,.25)}
  .no-scrollbar::-webkit-scrollbar{display:none}
  .view{display:none}
  .view.active{display:block}
  .reel-track{scroll-snap-type:y mandatory;-ms-overflow-style:none;scrollbar-width:none}
  .reel-track::-webkit-scrollbar{display:none}
  .reel-item{scroll-snap-align:start;scroll-snap-stop:always}
  .tab-btn.active{background:rgba(217,70,239,.15);color:#e879f9;border-color:rgba(217,70,239,.4)}
  .drop-zone.drag{border-color:#d946ef;background:rgba(217,70,239,.1)}
  .line-clamp-2{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
  .line-clamp-3{display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
</style>
</head>
<body class="text-gray-100 font-sans min-h-screen antialiased">

<div class="glow-blob w-[38rem] h-[38rem] bg-fuchsia-600/20 -top-40 -left-40 animate-pulseGlow"></div>
<div class="glow-blob w-[30rem] h-[30rem] bg-cyan-500/15 top-1/3 -right-40 animate-pulseGlow" style="animation-delay:.8s"></div>
<div class="glow-blob w-[26rem] h-[26rem] bg-violet-700/20 bottom-0 left-1/3"></div>

<!-- ================= HEADER ================= -->
<header class="sticky top-0 z-40 glass border-b border-white/10">
  <div class="max-w-[1600px] mx-auto px-3 sm:px-6 h-16 flex items-center gap-3">
    <button id="nav-home" class="flex items-center gap-2.5 shrink-0">
      <div class="w-9 h-9 rounded-xl bg-gradient-to-br from-fuchsia-500 to-cyan-400 grid place-items-center shadow-lg shadow-fuchsia-500/30">
        <svg style="width:18px;height:18px" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/><rect x="2" y="4" width="3.5" height="16" rx="1.5"/></svg>
      </div>
      <div class="leading-tight hidden sm:block">
        <h1 class="font-extrabold text-lg tracking-tight bg-gradient-to-r from-fuchsia-400 via-white to-cyan-300 bg-clip-text text-transparent">Neura Stream</h1>
        <p class="text-[10px] text-gray-500">watch · upload · share</p>
      </div>
    </button>

    <nav class="flex items-center gap-1.5 ml-1">
      <button id="tab-home" class="tab-btn active chip px-3 py-2 rounded-xl border border-white/10 bg-white/5 text-xs font-semibold text-gray-300">Home</button>
      <button id="tab-reels" class="tab-btn chip px-3 py-2 rounded-xl border border-white/10 bg-white/5 text-xs font-semibold text-gray-300">Reels</button>
    </nav>

    <form id="main-form" class="flex-1 max-w-xl mx-auto hidden md:flex">
      <div class="relative w-full">
        <svg class="w-4 h-4 absolute left-3.5 top-1/2 -translate-y-1/2 text-gray-500" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="m21 21-4.35-4.35M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0Z"/></svg>
        <input id="main-input" autocomplete="off" placeholder="Paste any video link or search…"
          class="w-full pl-10 pr-4 py-2.5 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 focus:ring-2 focus:ring-fuchsia-500/20 outline-none text-sm placeholder-gray-600 transition">
      </div>
    </form>

    <div class="flex items-center gap-2 ml-auto">
      <button id="btn-upload" class="flex items-center gap-2 px-3.5 py-2 rounded-xl font-semibold text-xs sm:text-sm bg-gradient-to-r from-fuchsia-500 to-fuchsia-600 hover:from-fuchsia-400 hover:to-fuchsia-500 shadow-lg shadow-fuchsia-500/30 transition-all active:scale-95">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2.2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 16V4m0 0L8 8m4-4 4 4M4 17v1a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-1"/></svg>
        <span class="hidden sm:inline">Upload</span>
      </button>
      <button id="chat-toggle" class="chip px-2.5 py-2.5 rounded-xl glass" title="AI Co-pilot">
        <svg class="w-4 h-4 text-fuchsia-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 10h8M8 14h5M21 12a9 9 0 1 1-4.4-7.7L21 3l-1 4.4A8.96 8.96 0 0 1 21 12Z"/></svg>
      </button>
    </div>
  </div>
  <form id="main-form-m" class="md:hidden px-3 pb-3">
    <input id="main-input-m" autocomplete="off" placeholder="Paste any video link or search…"
      class="w-full px-4 py-2.5 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 outline-none text-sm placeholder-gray-600">
  </form>
</header>

<!-- ================= STATUS / ERROR ================= -->
<div class="max-w-[1600px] mx-auto px-4 sm:px-6 pt-4 space-y-3">
  <div id="status-bar" class="hidden glass rounded-2xl px-5 py-3.5 text-sm flex items-center gap-3">
    <svg class="w-5 h-5 text-fuchsia-400 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 0 1 8-8v4a4 4 0 0 0-4 4H4Z"/></svg>
    <span id="status-text" class="text-gray-300">Working…</span>
  </div>
  <div id="error-bar" class="hidden glass rounded-2xl px-5 py-4 text-sm border-red-500/30 bg-red-500/10 flex items-start gap-3">
    <svg class="w-5 h-5 text-red-400 shrink-0 mt-0.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4m0 4h.01M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z"/></svg>
    <span id="error-text" class="text-red-200"></span>
  </div>
</div>

<!-- ================= VIEW: HOME ================= -->
<main id="view-home" class="view active max-w-[1600px] mx-auto px-4 sm:px-6 py-6 space-y-8">

  <section class="glass rounded-3xl p-6 sm:p-8 relative overflow-hidden">
    <div class="absolute inset-0 bg-gradient-to-br from-fuchsia-600/10 via-transparent to-cyan-500/10 pointer-events-none"></div>
    <div class="relative flex flex-col lg:flex-row lg:items-center gap-6">
      <div class="flex-1">
        <h2 class="text-2xl sm:text-3xl font-extrabold tracking-tight mb-2">
          One platform. <span class="bg-gradient-to-r from-fuchsia-400 to-cyan-300 bg-clip-text text-transparent">Every video.</span>
        </h2>
        <p class="text-sm text-gray-400 mb-4">Watch from YouTube, Instagram, X, Facebook & Telegram · upload your own · browse reels · ask the AI co-pilot. Zero API keys.</p>
        <div class="flex flex-wrap gap-2">
          <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="https://www.youtube.com/watch?v=aqz-KE-bpKQ">Try a YouTube link</button>
          <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="trending bollywood songs">Trending music</button>
          <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="isro latest mission">ISRO updates</button>
        </div>
      </div>
      <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-2 gap-2 shrink-0">
        <div class="glass rounded-xl px-3 py-2.5 text-center"><div class="text-lg font-extrabold text-fuchsia-400">6</div><div class="text-[10px] text-gray-500 uppercase tracking-wider">platforms</div></div>
        <div class="glass rounded-xl px-3 py-2.5 text-center"><div class="text-lg font-extrabold text-cyan-400" id="stat-uploads">0</div><div class="text-[10px] text-gray-500 uppercase tracking-wider">uploads</div></div>
        <div class="glass rounded-xl px-3 py-2.5 text-center"><div class="text-lg font-extrabold text-violet-400">0 keys</div><div class="text-[10px] text-gray-500 uppercase tracking-wider">API needed</div></div>
        <div class="glass rounded-xl px-3 py-2.5 text-center"><div class="text-lg font-extrabold text-emerald-400">∞</div><div class="text-[10px] text-gray-500 uppercase tracking-wider">reels</div></div>
      </div>
    </div>
  </section>

  <section>
    <div class="flex items-center justify-between mb-4">
      <h3 class="font-bold text-sm uppercase tracking-widest text-gray-400 flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400"></span> Community uploads
      </h3>
      <button id="btn-upload-2" class="text-xs text-fuchsia-400 hover:text-fuchsia-300 font-semibold">+ Upload yours</button>
    </div>
    <div id="uploads-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4"></div>
    <div id="uploads-empty" class="glass rounded-3xl p-10 text-center">
      <div class="w-14 h-14 mx-auto rounded-2xl bg-gradient-to-br from-fuchsia-500/20 to-cyan-400/20 grid place-items-center mb-4">
        <svg class="w-7 h-7 text-fuchsia-400" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 16V4m0 0L8 8m4-4 4 4M4 17v1a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-1"/></svg>
      </div>
      <p class="text-sm text-gray-400">No videos uploaded yet — be the first creator on Neura Stream.</p>
      <button class="mt-4 px-5 py-2.5 rounded-xl text-sm font-semibold bg-gradient-to-r from-fuchsia-500 to-fuchsia-600 shadow-lg shadow-fuchsia-500/30 hover:opacity-90 transition" id="empty-upload-btn">Upload a video</button>
    </div>
  </section>

  <section id="discover-section" class="hidden">
    <div class="flex items-center justify-between mb-4">
      <h3 class="font-bold text-sm uppercase tracking-widest text-gray-400 flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-cyan-400"></span> <span id="discover-title">Discover from the web</span>
      </h3>
    </div>
    <div id="discover-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4"></div>
  </section>
</main>

<!-- ================= VIEW: WATCH ================= -->
<main id="view-watch" class="view max-w-[1600px] mx-auto px-4 sm:px-6 py-6">
  <button id="btn-back" class="chip mb-4 inline-flex items-center gap-2 text-sm text-gray-400 hover:text-white">
    <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M15 18l-6-6 6-6"/></svg> Back to home
  </button>

  <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_380px] gap-6 items-start">
    <div class="min-w-0 space-y-5">
      <div id="player-card" class="glass rounded-3xl overflow-hidden">
        <div id="player-shell" class="relative group bg-black select-none">
          <video id="video" class="w-full aspect-video max-h-[70vh] bg-black" playsinline preload="metadata"></video>
          <button id="big-play" class="absolute inset-0 grid place-items-center bg-black/20 opacity-0 group-hover:opacity-100 transition">
            <span class="w-20 h-20 rounded-full bg-fuchsia-500/90 shadow-2xl shadow-fuchsia-500/50 grid place-items-center hover:scale-110 transition">
              <svg class="w-9 h-9 text-white ml-1" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg>
            </span>
          </button>
          <div class="absolute bottom-0 inset-x-0 px-4 pb-3 pt-10 bg-gradient-to-t from-black/85 via-black/40 to-transparent opacity-0 group-hover:opacity-100 focus-within:opacity-100 transition">
            <input id="seek" class="forge-seek w-full mb-2" type="range" min="0" max="1000" value="0" step="0.1" aria-label="Seek">
            <div class="flex items-center gap-2 sm:gap-3">
              <button id="btn-play" class="p-2 rounded-lg hover:bg-white/10 transition" aria-label="Play / pause">
                <svg id="ic-play" class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg>
                <svg id="ic-pause" class="w-5 h-5 hidden" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16" rx="1.5"/><rect x="14" y="4" width="4" height="16" rx="1.5"/></svg>
              </button>
              <button id="btn-mute" class="p-2 rounded-lg hover:bg-white/10 transition" aria-label="Mute">
                <svg id="ic-vol" class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor"><path d="M13 4.5v15a1 1 0 0 1-1.64.77L6.8 16.5H4a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h2.8l4.56-3.77A1 1 0 0 1 13 4.5Z"/><path d="M16 8.5a5 5 0 0 1 0 7" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg>
                <svg id="ic-muted" class="w-5 h-5 hidden" viewBox="0 0 24 24" fill="currentColor"><path d="M13 4.5v15a1 1 0 0 1-1.64.77L6.8 16.5H4a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h2.8l4.56-3.77A1 1 0 0 1 13 4.5Z"/><path d="m16 9 5 6m0-6-5 6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
              </button>
              <input id="volume" class="forge-seek w-20 hidden sm:block" type="range" min="0" max="1" step="0.01" value="1" aria-label="Volume">
              <span class="text-xs text-gray-300 tabular-nums font-medium ml-1"><span id="t-now">0:00</span> / <span id="t-dur">0:00</span></span>
              <div class="flex-1"></div>
              <select id="quality-select" class="bg-white/10 border border-white/10 rounded-lg text-xs px-2 py-1.5 outline-none hover:bg-white/15 transition max-w-[130px]"></select>
              <button id="btn-pip" class="p-2 rounded-lg hover:bg-white/10 transition" aria-label="Picture in picture">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><rect x="2" y="4" width="20" height="16" rx="2"/><rect x="12" y="12" width="8" height="6" rx="1" fill="currentColor" stroke="none"/></svg>
              </button>
              <button id="btn-fs" class="p-2 rounded-lg hover:bg-white/10 transition" aria-label="Fullscreen">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3M3 16v3a2 2 0 0 0 2 2h3m13-5v5a2 2 0 0 1-2 2h-3"/></svg>
              </button>
            </div>
          </div>
        </div>
        <div class="p-5">
          <h3 id="w-title" class="font-bold text-base sm:text-lg leading-snug"></h3>
          <div class="flex flex-wrap items-center gap-x-4 gap-y-2 mt-2.5 text-xs text-gray-400">
            <span id="w-meta"></span>
            <div class="flex-1"></div>
            <button id="w-like" class="chip inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-pink-300 text-xs font-semibold">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M7 22V11l5-9a3 3 0 0 1 3 3v4h4.5a2 2 0 0 1 2 2.4l-1.6 8A2 2 0 0 1 18 22H7Z"/><path d="M7 11H4v11h3"/></svg>
              <span id="w-likes">0</span>
            </button>
          </div>
          <p id="w-desc" class="text-sm text-gray-500 mt-3 leading-relaxed"></p>
        </div>
      </div>

      <div class="glass rounded-3xl p-5">
        <h4 class="font-bold text-sm mb-4 flex items-center gap-2 text-gray-300">
          <svg class="w-4 h-4 text-cyan-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M21 12a8 8 0 0 1-8 8H5l-2 2V12a8 8 0 0 1 8-8h2a8 8 0 0 1 8 8Z"/></svg>
          Comments <span id="c-count" class="text-gray-600 font-normal"></span>
        </h4>
        <form id="c-form" class="flex gap-2 mb-4">
          <input id="c-author" placeholder="Your name (optional)" class="w-36 sm:w-44 px-3 py-2 rounded-xl bg-white/5 border border-white/10 focus:border-cyan-400/60 outline-none text-xs placeholder-gray-600">
          <input id="c-body" placeholder="Add a comment…" required class="flex-1 px-3 py-2 rounded-xl bg-white/5 border border-white/10 focus:border-cyan-400/60 outline-none text-xs placeholder-gray-600">
          <button class="px-4 py-2 rounded-xl bg-cyan-400/20 border border-cyan-400/30 text-cyan-300 text-xs font-semibold hover:bg-cyan-400/30 transition">Post</button>
        </form>
        <div id="c-list" class="space-y-3"></div>
      </div>
    </div>

    <aside id="chat-panel" class="glass rounded-3xl flex-col overflow-hidden lg:sticky lg:top-24 h-[70vh] lg:h-[calc(100vh-8rem)] hidden lg:flex">
      <div class="px-5 py-4 border-b border-white/10 flex items-center gap-3">
        <div class="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-400 to-fuchsia-500 grid place-items-center shadow-lg shadow-cyan-500/20">
          <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 3l1.9 4.6L18.5 9.5l-4.6 1.9L12 16l-1.9-4.6L5.5 9.5l4.6-1.9L12 3Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M19 15l.9 2.1L22 18l-2.1.9L19 21l-.9-2.1L16 18l2.1-.9L19 15Z"/></svg>
        </div>
        <div><h3 class="font-bold text-sm">Co-pilot</h3><p class="text-[11px] text-gray-500">video & live-web synthesis</p></div>
        <div class="flex-1"></div>
        <button id="chat-close" class="lg:hidden p-2 rounded-lg hover:bg-white/10"><svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="M6 18 18 6M6 6l12 12"/></svg></button>
      </div>
      <div id="chat-log" class="flex-1 overflow-y-auto p-4 space-y-4">
        <div class="msg-in flex gap-3">
          <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-400 to-fuchsia-500 shrink-0 grid place-items-center text-[10px] font-bold">AI</div>
          <div class="glass rounded-2xl rounded-tl-sm px-4 py-3 text-sm text-gray-200 leading-relaxed max-w-[85%]">
            I see what you're watching. Ask me to summarize it, explain a part, or search the wider web.
          </div>
        </div>
      </div>
      <div class="px-4 pb-2 flex gap-2 overflow-x-auto no-scrollbar">
        <button class="chat-chip chip shrink-0 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400">Summarize this video</button>
        <button class="chat-chip chip shrink-0 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400">Find related videos</button>
      </div>
      <form id="chat-form" class="p-3 border-t border-white/10 flex gap-2">
        <input id="chat-input" autocomplete="off" placeholder="Ask anything…" class="flex-1 px-4 py-3 rounded-xl bg-white/5 border border-white/10 focus:border-cyan-400/60 focus:ring-2 focus:ring-cyan-400/20 outline-none text-sm placeholder-gray-600 transition">
        <button class="px-4 py-3 rounded-xl bg-gradient-to-r from-cyan-400 to-fuchsia-500 hover:opacity-90 active:scale-95 transition shadow-lg shadow-fuchsia-500/20" aria-label="Send">
          <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="m22 2-7 20-4-9-9-4Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M22 2 11 13"/></svg>
        </button>
      </form>
    </aside>
  </div>
</main>

<!-- ================= VIEW: REELS ================= -->
<main id="view-reels" class="view max-w-[1600px] mx-auto px-4 sm:px-6 py-6">
  <div class="glass rounded-2xl px-4 py-3.5 mb-5 flex flex-col sm:flex-row items-center gap-3">
    <svg class="w-5 h-5 text-fuchsia-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 6l4 5m-4-5v13a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1V6M8 11l4 6 4-6"/></svg>
    <p class="text-xs text-gray-400 flex-1 text-center sm:text-left">Vertical short-video feed — community uploads play here Instagram-style. Scroll or use arrows.</p>
    <input id="reel-url-input" placeholder="Paste any reel URL to add…" class="w-full sm:w-72 px-3.5 py-2 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 outline-none text-xs placeholder-gray-600">
  </div>
  <div id="reel-track" class="reel-track glass rounded-3xl overflow-y-auto h-[75vh] snap-y relative"></div>
  <p id="reels-empty" class="hidden text-center text-sm text-gray-500 py-16">No reels yet — upload a short video and it lands here too.</p>
</main>

<!-- ================= UPLOAD MODAL ================= -->
<div id="upload-modal" class="hidden fixed inset-0 z-50 grid place-items-center p-4 bg-black/70 backdrop-blur-sm">
  <div class="glass rounded-3xl w-full max-w-lg p-6 animate-fadeUp max-h-[92vh] overflow-y-auto">
    <div class="flex items-center justify-between mb-5">
      <h3 class="font-bold text-lg">Upload a video</h3>
      <button id="upload-close" class="p-2 rounded-lg hover:bg-white/10"><svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="M6 18 18 6M6 6l12 12"/></svg></button>
    </div>

    <div id="drop-zone" class="drop-zone border-2 border-dashed border-white/15 rounded-2xl p-8 text-center cursor-pointer hover:border-fuchsia-500/50 transition mb-4">
      <input id="file-input" type="file" accept="video/*" class="hidden">
      <div class="w-14 h-14 mx-auto rounded-2xl bg-gradient-to-br from-fuchsia-500/20 to-cyan-400/20 grid place-items-center mb-3">
        <svg class="w-7 h-7 text-fuchsia-400" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 16V4m0 0L8 8m4-4 4 4M4 17v1a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-1"/></svg>
      </div>
      <p class="text-sm text-gray-300 font-semibold" id="drop-title">Drop your video here or click to browse</p>
      <p class="text-xs text-gray-600 mt-1">MP4 / WebM / MKV · up to 200 MB · poster frame is auto-captured</p>
      <video id="thumb-video" class="hidden"></video>
      <img id="thumb-preview" class="hidden w-48 aspect-video object-cover rounded-xl mx-auto mt-4 border border-white/10">
    </div>

    <div class="space-y-3">
      <input id="up-title" placeholder="Title *" class="w-full px-4 py-3 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 outline-none text-sm placeholder-gray-600">
      <input id="up-uploader" placeholder="Channel name (optional)" class="w-full px-4 py-3 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 outline-none text-sm placeholder-gray-600">
      <textarea id="up-desc" rows="2" placeholder="Description (optional)" class="w-full px-4 py-3 rounded-xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 outline-none text-sm placeholder-gray-600 resize-none"></textarea>
    </div>

    <div id="up-progress" class="hidden mt-4">
      <div class="h-2 rounded-full bg-white/10 overflow-hidden"><div id="up-bar" class="h-full w-0 bg-gradient-to-r from-fuchsia-500 to-cyan-400 transition-all"></div></div>
      <p id="up-pct" class="text-xs text-gray-400 mt-1.5 text-center">0%</p>
    </div>

    <button id="up-submit" class="w-full mt-5 py-3.5 rounded-xl font-semibold text-sm bg-gradient-to-r from-fuchsia-500 to-fuchsia-600 shadow-lg shadow-fuchsia-500/30 hover:opacity-90 active:scale-[.98] transition disabled:opacity-40 disabled:cursor-not-allowed" disabled>Upload</button>
    <p class="text-[10px] text-gray-600 text-center mt-3">Free-tier note: storage is ephemeral — uploads reset when the server restarts.</p>
  </div>
</div>

<footer class="max-w-[1600px] mx-auto px-6 py-8 text-center text-[11px] text-gray-600">
  Neura Stream · single-file FastAPI · yt-dlp + DDG + heuristic co-pilot · uploads on SQLite · Render free tier
</footer>
<script>
/* ================= state & helpers ================= */
const $ = (id) => document.getElementById(id);
let currentMedia = null;        // watch context (local or remote)
let currentStreams = [];        // remote formats
let lastSearchResults = [];
let reelsObserver = null;
let uploadCtx = { file: null, thumb: '', duration: 0 };

const isUrl = (s) => /^https?:\/\/\S+\.\S+/i.test(s.trim());
const esc = (s) => { const d = document.createElement('div'); d.textContent = s ?? ''; return d.innerHTML; };
const fmtTime = (s) => { if (!isFinite(s) || s < 0) return '0:00'; s = Math.floor(s);
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), x = s%60;
  return h ? h+':'+String(m).padStart(2,'0')+':'+String(x).padStart(2,'0') : m+':'+String(x).padStart(2,'0'); };
const fmtViews = (n) => n == null ? '' : (n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(0)+'K' : String(n));
const timeAgo = (ts) => { const d = Date.now()/1000 - ts;
  if (d < 60) return 'just now'; if (d < 3600) return Math.floor(d/60)+'m ago';
  if (d < 86400) return Math.floor(d/3600)+'h ago'; if (d < 2592000) return Math.floor(d/86400)+'d ago';
  return Math.floor(d/2592000)+'mo ago'; };

function setStatus(msg) { if (!msg) { $('status-bar').classList.add('hidden'); return; }
  $('status-text').textContent = msg; $('status-bar').classList.remove('hidden'); }
function showError(msg) { $('error-text').textContent = msg; $('error-bar').classList.remove('hidden');
  setTimeout(() => $('error-bar').classList.add('hidden'), 9000); }

/* ================= view router ================= */
function showView(v) {
  ['home','watch','reels'].forEach(x => $('view-'+x).classList.toggle('active', x === v));
  $('tab-home').classList.toggle('active', v === 'home');
  $('tab-reels').classList.toggle('active', v === 'reels');
  if (v === 'home') $('video').pause();
  window.scrollTo({ top: 0 });
}
$('tab-home').addEventListener('click', () => showView('home'));
$('tab-reels').addEventListener('click', () => { showView('reels'); loadReels(); });
$('nav-home').addEventListener('click', () => showView('home'));
$('btn-back').addEventListener('click', () => showView('home'));

/* ================= search router ================= */
function handleQuery(v) {
  v = v.trim(); if (!v) return;
  $('error-bar').classList.add('hidden');
  if (isUrl(v)) openRemote(v); else doSearch(v);
}
$('main-form').addEventListener('submit', e => { e.preventDefault(); handleQuery($('main-input').value); $('main-input').value=''; });
$('main-form-m').addEventListener('submit', e => { e.preventDefault(); handleQuery($('main-input-m').value); $('main-input-m').value=''; });
document.querySelectorAll('.demo').forEach(b => b.addEventListener('click', () => handleQuery(b.dataset.q)));

/* ================= home: uploads grid ================= */
async function loadHome() {
  try {
    const res = await fetch('/api/videos');
    const data = await res.json();
    $('stat-uploads').textContent = data.total;
    const grid = $('uploads-grid'); grid.innerHTML = '';
    const has = data.videos && data.videos.length;
    $('uploads-empty').classList.toggle('hidden', !!has);
    (data.videos || []).forEach(v => grid.appendChild(videoCard(v)));
  } catch (e) { showError('Could not load uploads: ' + e.message); }
}

function videoCard(v) {
  const card = document.createElement('div');
  card.className = 'glass card-hover rounded-2xl overflow-hidden cursor-pointer group';
  const thumbInner = v.thumb
    ? `<img src="${esc(v.thumb)}" class="w-full aspect-video object-cover group-hover:scale-[1.03] transition duration-300" loading="lazy">`
    : `<div class="w-full aspect-video bg-gradient-to-br from-fuchsia-600/30 via-violet-700/20 to-cyan-500/25 grid place-items-center">
         <svg class="w-10 h-10 text-white/50" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg></div>`;
  card.innerHTML = `
    <div class="relative">${thumbInner}
      <span class="absolute bottom-2 right-2 px-1.5 py-0.5 rounded-md bg-black/80 text-[10px] font-semibold tabular-nums">${esc(v.duration_label || '0:00')}</span>
    </div>
    <div class="p-3.5">
      <h4 class="text-sm font-semibold leading-snug line-clamp-2 group-hover:text-fuchsia-300 transition">${esc(v.title)}</h4>
      <p class="text-xs text-gray-500 mt-1.5">${esc(v.uploader)}</p>
      <p class="text-xs text-gray-600 mt-0.5">${fmtViews(v.views)} views · ${timeAgo(v.created_at)} · <span class="text-pink-400">${v.likes} likes</span></p>
    </div>`;
  card.addEventListener('click', () => openLocal(v.id));
  return card;
}

/* ================= watch: local video ================= */
async function openLocal(id) {
  setStatus('Loading video…');
  try {
    const res = await fetch('/api/videos/' + id);
    const v = await res.json();
    if (v.error) throw new Error(v.message);
    showView('watch');
    currentMedia = { ...v, isLocal: true };
    currentStreams = [];
    $('w-title').textContent = v.title;
    $('w-meta').textContent = [v.uploader, fmtViews(v.views) + ' views', timeAgo(v.created_at)].filter(Boolean).join(' · ');
    $('w-desc').textContent = v.description || '';
    $('w-likes').textContent = v.likes;
    const sel = $('quality-select'); sel.innerHTML = '';
    const o = document.createElement('option'); o.textContent = 'original'; o.value = 'original'; sel.appendChild(o);
    playSrc('/media/' + v.filename, true);
    loadComments(id);
    setStatus(null);
  } catch (e) { setStatus(null); showError(e.message); }
}

/* ================= watch: remote (extracted) video ================= */
async function openRemote(url) {
  setStatus('Extracting direct CDN stream (yt-dlp)…');
  try {
    const res = await fetch('/api/stream?url=' + encodeURIComponent(url));
    const data = await res.json();
    if (data.error) throw new Error(data.message || data.error);
    showView('watch');
    currentMedia = { ...data, isLocal: false, id: null, likes: 0 };
    currentStreams = (data.streams || []).filter(s => s.progressive);
    $('w-title').textContent = data.title;
    $('w-meta').textContent = [data.extractor, data.uploader, data.duration_label,
      data.view_count ? fmtViews(data.view_count) + ' views' : ''].filter(Boolean).join(' · ');
    $('w-desc').textContent = (data.description || '').slice(0, 400);
    $('w-likes').textContent = '—';
    const sel = $('quality-select'); sel.innerHTML = '';
    currentStreams.forEach((s, i) => { const o = document.createElement('option'); o.value = i; o.textContent = s.label; sel.appendChild(o); });
    const best = currentStreams[0] || data.best;
    if (best) playSrc('/api/proxy?url=' + encodeURIComponent(best.url), true);
    setStatus(null);
  } catch (e) { setStatus(null); showError('Stream extraction failed: ' + e.message); }
}

function playSrc(src, reset) {
  const v = $('video'); const t = reset ? 0 : v.currentTime;
  v.src = src; v.currentTime = t; v.play().catch(() => {});
}

$('quality-select').addEventListener('change', e => {
  if (!currentMedia || currentMedia.isLocal) return;
  const src = currentStreams[+e.target.value];
  if (src) playSrc('/api/proxy?url=' + encodeURIComponent(src.url), false);
});

/* ================= likes & comments ================= */
$('w-like').addEventListener('click', async () => {
  if (!currentMedia || !currentMedia.isLocal || !currentMedia.id) {
    showError('Liking is available for community uploads.'); return;
  }
  try {
    const res = await fetch('/api/videos/' + currentMedia.id + '/like', { method: 'POST' });
    const d = await res.json();
    if (d.likes != null) { $('w-likes').textContent = d.likes; currentMedia.likes = d.likes; }
  } catch (e) { showError('Could not like: ' + e.message); }
});

async function loadComments(id) {
  try {
    const res = await fetch('/api/videos/' + id + '/comments');
    const d = await res.json();
    renderComments(d.comments || []);
  } catch (e) { renderComments([]); }
}
function renderComments(list) {
  $('c-count').textContent = list.length ? '(' + list.length + ')' : '';
  const box = $('c-list'); box.innerHTML = '';
  if (!list.length) { box.innerHTML = '<p class="text-xs text-gray-600 text-center py-4">No comments yet — start the conversation.</p>'; return; }
  list.forEach(c => {
    const el = document.createElement('div');
    el.className = 'msg-in flex gap-3';
    el.innerHTML = `<div class="w-8 h-8 rounded-full shrink-0 bg-gradient-to-br from-fuchsia-500 to-cyan-400 grid place-items-center text-[10px] font-bold">${esc((c.author||'?')[0].toUpperCase())}</div>
      <div class="glass rounded-xl rounded-tl-sm px-3.5 py-2.5 max-w-[85%]">
        <p class="text-xs font-semibold text-fuchsia-300">${esc(c.author)} <span class="text-gray-600 font-normal">· ${timeAgo(c.created_at)}</span></p>
        <p class="text-sm text-gray-300 mt-0.5 break-words">${esc(c.body)}</p>
      </div>`;
    box.appendChild(el);
  });
}
$('c-form').addEventListener('submit', async e => {
  e.preventDefault();
  if (!currentMedia || !currentMedia.isLocal) { showError('Comments are available on community uploads.'); return; }
  const body = $('c-body').value.trim(); if (!body) return;
  try {
    const res = await fetch('/api/videos/' + currentMedia.id + '/comments', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ author: $('c-author').value.trim() || 'Guest', body })
    });
    if ((await res.json()).ok) { $('c-body').value = ''; loadComments(currentMedia.id); }
  } catch (err) { showError('Could not post comment: ' + err.message); }
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
const volume = $('volume'); paintSeek(volume);
volume.addEventListener('input', () => { video.volume = +volume.value; video.muted = false; syncMute(); paintSeek(volume); });
$('btn-mute').addEventListener('click', () => { video.muted = !video.muted; syncMute(); });
function syncMute() { const m = video.muted || video.volume === 0;
  $('ic-vol').classList.toggle('hidden', m); $('ic-muted').classList.toggle('hidden', !m); }
$('btn-pip').addEventListener('click', async () => {
  try { if (document.pictureInPictureElement) await document.exitPictureInPicture(); else await video.requestPictureInPicture(); }
  catch (e) { showError('Picture-in-picture is not available here.'); }
});
$('btn-fs').addEventListener('click', () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else if ($('player-shell').requestFullscreen) $('player-shell').requestFullscreen();
});
document.addEventListener('keydown', e => {
  if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
  if (e.code === 'Space') { e.preventDefault(); video.paused ? video.play() : video.pause(); }
  if (e.code === 'ArrowRight') video.currentTime = Math.min(video.duration || 1e9, video.currentTime + 5);
  if (e.code === 'ArrowLeft') video.currentTime = Math.max(0, video.currentTime - 5);
});

/* ================= web search / discover ================= */
async function doSearch(q) {
  setStatus('Searching the live web (DuckDuckGo)…');
  try {
    const res = await fetch('/api/search?q=' + encodeURIComponent(q));
    const data = await res.json();
    if (data.error && !(data.results||[]).length && !(data.news||[]).length) throw new Error(data.message || 'search failed');
    lastSearchResults = data.results || [];
    const items = [...(data.results||[]).map(r => ({kind:'web',...r})), ...(data.news||[]).map(r => ({kind:'news',...r}))];
    const grid = $('discover-grid'); grid.innerHTML = '';
    items.forEach(item => {
      const isVideoish = /youtu\.?be|instagram|fb\.watch|facebook\.com\/.*\/videos|twitter\.com|x\.com|t\.me/i.test(item.url || '');
      const card = document.createElement('div');
      card.className = 'glass card-hover rounded-2xl p-4 cursor-pointer flex flex-col gap-2';
      card.innerHTML = `
        <div class="flex items-center gap-2 text-[10px] uppercase tracking-wider">
          <span class="px-2 py-0.5 rounded-full ${item.kind==='news'?'bg-cyan-500/15 text-cyan-300':'bg-fuchsia-500/15 text-fuchsia-300'}">${item.kind}</span>
          <span class="text-gray-600 truncate">${esc(item.source||'')}</span>
        </div>
        <h4 class="text-sm font-semibold leading-snug line-clamp-2">${esc(item.title)}</h4>
        <p class="text-xs text-gray-500 line-clamp-3 leading-relaxed">${esc(item.body||'')}</p>
        <div class="mt-auto pt-2 flex items-center gap-2">
          ${isVideoish ? '<button class="play-here px-3 py-1.5 rounded-lg text-[11px] font-semibold bg-fuchsia-500/20 text-fuchsia-300 border border-fuchsia-500/30 hover:bg-fuchsia-500/30 transition">Play here</button>' : ''}
          <a href="${esc(item.url)}" target="_blank" rel="noopener" class="ml-auto text-[11px] text-gray-500 hover:text-cyan-300 underline underline-offset-2">open</a>
        </div>`;
      if (isVideoish) card.querySelector('.play-here').addEventListener('click', ev => { ev.stopPropagation(); openRemote(item.url); });
      card.addEventListener('click', ev => { if (ev.target.tagName === 'A' || ev.target.closest('button')) return; window.open(item.url, '_blank', 'noopener'); });
      grid.appendChild(card);
    });
    $('discover-title').textContent = 'Web results for ' + q;
    $('discover-section').classList.remove('hidden');
    $('discover-section').scrollIntoView({ behavior: 'smooth' });
    setStatus(null);
  } catch (err) { setStatus(null); showError('Search failed: ' + err.message); }
}

/* ================= reels (Instagram-style) ================= */
async function loadReels() {
  const track = $('reel-track');
  try {
    const res = await fetch('/api/videos');
    const data = await res.json();
    const vids = data.videos || [];
    $('reels-empty').classList.toggle('hidden', !!vids.length);
    track.innerHTML = '';
    vids.forEach(v => track.appendChild(buildReel(v)));
    if (reelsObserver) reelsObserver.disconnect();
    reelsObserver = new IntersectionObserver(entries => {
      entries.forEach(en => {
        const el = en.target.querySelector('video');
        if (en.isIntersecting) { el.play().catch(()=>{}); } else { el.pause(); }
      });
    }, { root: track, threshold: 0.6 });
    track.querySelectorAll('.reel-item').forEach(el => reelsObserver.observe(el));
  } catch (e) { showError('Could not load reels: ' + e.message); }
}

function buildReel(v) {
  const item = document.createElement('div');
  item.className = 'reel-item relative w-full h-full flex items-center justify-center bg-black';
  item.innerHTML = `
    <video src="/media/${esc(v.filename)}" class="h-full max-h-full w-auto max-w-full object-contain" loop muted playsinline preload="metadata" ${v.thumb ? `poster="${esc(v.thumb)}"` : ''}></video>
    <div class="absolute inset-x-0 bottom-0 p-5 pb-6 bg-gradient-to-t from-black/85 via-black/30 to-transparent">
      <div class="max-w-[75%]">
        <p class="text-sm font-bold">${esc(v.title)}</p>
        <p class="text-xs text-gray-400 mt-0.5">${esc(v.uploader)} · ${fmtViews(v.views)} views</p>
      </div>
    </div>
    <div class="absolute right-3 bottom-24 flex flex-col items-center gap-4">
      <button class="reel-like w-11 h-11 rounded-full glass grid place-items-center hover:scale-110 transition" title="Like">
        <svg class="w-5 h-5 text-pink-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M7 22V11l5-9a3 3 0 0 1 3 3v4h4.5a2 2 0 0 1 2 2.4l-1.6 8A2 2 0 0 1 18 22H7Z"/><path d="M7 11H4v11h3"/></svg>
      </button>
      <button class="reel-open w-11 h-11 rounded-full glass grid place-items-center hover:scale-110 transition" title="Open & comment">
        <svg class="w-5 h-5 text-cyan-300" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M21 12a8 8 0 0 1-8 8H5l-2 2V12a8 8 0 0 1 8-8h2a8 8 0 0 1 8 8Z"/></svg>
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

/* ================= reel from remote URL ================= */
$('reel-url-input').addEventListener('change', async () => {
  const url = $('reel-url-input').value.trim();
  if (!isUrl(url)) return;
  setStatus('Adding reel from link…');
  try {
    const res = await fetch('/api/stream?url=' + encodeURIComponent(url));
    const data = await res.json();
    if (data.error) throw new Error(data.message);
    const best = (data.streams || []).find(s => s.progressive) || data.best;
    if (!best) throw new Error('no playable stream');
    const track = $('reel-track');
    const item = document.createElement('div');
    item.className = 'reel-item relative w-full h-full flex items-center justify-center bg-black';
    item.innerHTML = `
      <video src="/api/proxy?url=${encodeURIComponent(best.url)}" class="h-full w-auto max-w-full object-contain" loop muted playsinline autoplay controls></video>
      <div class="absolute inset-x-0 bottom-0 p-5 pb-6 bg-gradient-to-t from-black/85 via-black/30 to-transparent pointer-events-none">
        <p class="text-sm font-bold">${esc(data.title)}</p>
        <p class="text-xs text-gray-400 mt-0.5">${esc(data.extractor)} · ${esc(data.uploader)}</p>
      </div>`;
    track.prepend(item);
    $('reel-url-input').value = '';
    $('reels-empty').classList.add('hidden');
    setStatus(null);
  } catch (e) { setStatus(null); showError('Reel add failed: ' + e.message); }
});

/* ================= upload ================= */
const openModal = () => { $('upload-modal').classList.remove('hidden'); };
const closeModal = () => { $('upload-modal').classList.add('hidden'); };
$('btn-upload').addEventListener('click', openModal);
$('btn-upload-2').addEventListener('click', openModal);
$('empty-upload-btn').addEventListener('click', openModal);
$('upload-close').addEventListener('click', closeModal);
$('upload-modal').addEventListener('click', e => { if (e.target === $('upload-modal')) closeModal(); });
const dz = $('drop-zone');
dz.addEventListener('click', () => $('file-input').click());
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag'); });
dz.addEventListener('dragleave', () => dz.classList.remove('drag'));
dz.addEventListener('drop', e => { e.preventDefault(); dz.classList.remove('drag'); if (e.dataTransfer.files[0]) pickFile(e.dataTransfer.files[0]); });
$('file-input').addEventListener('change', e => { if (e.target.files[0]) pickFile(e.target.files[0]); });

function pickFile(f) {
  if (!f.type.startsWith('video/')) { showError('Please choose a video file.'); return; }
  if (f.size > 200 * 1024 * 1024) { showError('File is over the 200 MB limit.'); return; }
  uploadCtx = { file: f, thumb: '', duration: 0 };
  $('drop-title').textContent = f.name + ' (' + (f.size/1048576).toFixed(1) + ' MB)';
  $('up-submit').disabled = false;
  const tv = $('thumb-video');
  tv.src = URL.createObjectURL(f);
  tv.muted = true;
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
  xhr.upload.onprogress = e => {
    if (e.lengthComputable) { const p = Math.round(e.loaded / e.total * 100);
      $('up-bar').style.width = p + '%'; $('up-pct').textContent = p + '%'; }
  };
  xhr.onload = () => {
    btn.disabled = false; btn.textContent = 'Upload';
    try {
      const d = JSON.parse(xhr.responseText);
      if (d.error) { showError(d.message); return; }
      closeModal();
      $('up-title').value = ''; $('up-desc').value = ''; $('up-uploader').value = '';
      $('thumb-preview').classList.add('hidden');
      $('drop-title').textContent = 'Drop your video here or click to browse';
      uploadCtx = { file: null, thumb: '', duration: 0 }; $('up-submit').disabled = true;
      loadHome();
      openLocal(d.video.id);
    } catch (e) { showError('Upload response error.'); }
  };
  xhr.onerror = () => { btn.disabled = false; btn.textContent = 'Upload'; showError('Upload failed — network error.'); };
  xhr.send(fd);
});

/* ================= co-pilot chat ================= */
function appendMsg(role, text, sources) {
  const log = $('chat-log');
  const wrap = document.createElement('div');
  wrap.className = 'msg-in flex gap-3' + (role === 'user' ? ' flex-row-reverse' : '');
  const body = text.split('\n').map(l => esc(l)).join('<br>');
  const srcHtml = (sources && sources.length)
    ? '<div class="mt-2 pt-2 border-t border-white/10 space-y-1">' + sources.map(s =>
        `<a href="${esc(s.url)}" target="_blank" rel="noopener" class="block text-[11px] text-cyan-400/80 hover:text-cyan-300 truncate">${esc(s.title || s.url)}</a>`).join('') + '</div>'
    : '';
  wrap.innerHTML = role === 'user'
    ? `<div class="rounded-2xl rounded-tr-sm px-4 py-3 text-sm bg-fuchsia-500/20 border border-fuchsia-500/30 max-w-[85%] leading-relaxed">${body}</div>`
    : `<div class="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-400 to-fuchsia-500 shrink-0 grid place-items-center text-[10px] font-bold">AI</div>
       <div class="glass rounded-2xl rounded-tl-sm px-4 py-3 text-sm text-gray-200 leading-relaxed max-w-[85%]">${body}${srcHtml}</div>`;
  log.appendChild(wrap);
  log.scrollTop = log.scrollHeight;
  return wrap;
}
async function sendChat(q) {
  if (!q.trim()) return;
  appendMsg('user', q);
  const holder = appendMsg('ai', '…');
  try {
    const res = await fetch('/api/chat', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: q,
        video: currentMedia ? {
          title: currentMedia.title, uploader: currentMedia.uploader,
          description: currentMedia.description || '', duration_label: currentMedia.duration_label,
          webpage_url: currentMedia.webpage_url
        } : null,
        search_results: lastSearchResults.slice(0, 6).map(r => ({ title: r.title, url: r.url, body: r.body }))
      })
    });
    const data = await res.json();
    holder.remove();
    appendMsg('ai', data.answer || '…', data.sources);
  } catch (err) { holder.remove(); appendMsg('ai', 'Co-pilot error: ' + err.message); }
}
$('chat-form').addEventListener('submit', e => { e.preventDefault(); const v = $('chat-input').value; $('chat-input').value = ''; sendChat(v); });
document.querySelectorAll('.chat-chip').forEach(c => c.addEventListener('click', () => {
  let q = c.textContent.trim();
  if (q === 'Find related videos' && currentMedia) q = 'search ' + currentMedia.title + ' video';
  sendChat(q);
}));
$('chat-toggle').addEventListener('click', () => {
  const p = $('chat-panel');
  if (getComputedStyle(p).display === 'none') { p.classList.remove('hidden'); }
  else { p.classList.add('hidden'); }
  showView('watch');
});
$('chat-close').addEventListener('click', () => $('chat-panel').classList.add('hidden'));

/* ================= boot ================= */
loadHome();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FRONTEND)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "version": "3.0.0"}


# --------------------------------------------------------------------------- #
#  Entrypoint — Render binds $PORT dynamically                                 #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
