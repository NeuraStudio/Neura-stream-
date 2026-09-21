"""
================================================================================
 Neura Stream — Zero-API Media Extractor, Live Search & AI Co-pilot
 Single-file FastAPI application built for Render's free tier (512 MB RAM).
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

 STACK / FOOTPRINT
 -----------------
   * yt-dlp            -> CDN stream extraction (YouTube, Instagram, X, Facebook)
   * duckduckgo_search -> zero-token live web + news search
   * heuristic engine  -> pure-Python extractive summarizer (< 50 MB RAM)
   * httpx             -> chunked (64 KB) media proxy so direct CDN URLs,
                          which are IP-locked to the server, actually play in
                          the browser without buffering the file in memory.

 NO API KEYS. NO PAID SERVICES. NO BUILD STEP. ONE FILE.
================================================================================
"""

import asyncio
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
import uvicorn
import yt_dlp
try:  # package was renamed: works with both `duckduckgo-search` and `ddgs`
    from duckduckgo_search import DDGS
except ImportError:  # pragma: no cover
    from ddgs import DDGS
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
#  Application                                                                 #
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="Neura Stream",
    description="Zero-API media extraction, live web search and co-pilot chat.",
    version="1.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

YDL_OPTS: Dict[str, Any] = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "skip_download": True,
    "socket_timeout": 20,
    "retries": 2,
    "nocheckcertificate": True,
    "cachedir": False,
    # android + web clients expose direct progressive MP4 URLs most reliably
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    "http_headers": {"User-Agent": USER_AGENT},
}

# Hosts the proxy is allowed to fetch from (SSRF guard for the free tier).
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

# Fallback ladder of YouTube player clients — some get bot-checked more
# aggressively than others depending on the host IP, so we retry once.
YDL_CLIENT_LADDERS = [
    {"youtube": {"player_client": ["android", "web"]}},
    {"youtube": {"player_client": ["tv", "web_safari"]}},
]


# --------------------------------------------------------------------------- #
#  1) Scraping & media engine (yt-dlp, download=False)                         #
# --------------------------------------------------------------------------- #

def _pick_streams(info: Dict[str, Any]) -> Dict[str, Any]:
    """Distill yt-dlp's format list into a small, playable JSON structure."""
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
            "id": f.get("format_id"),
            "url": url,
            "ext": ext,
            "mime": f.get("mime_type") or ("video/mp4" if has_video else "audio/mp4"),
            "vcodec": vcodec,
            "acodec": acodec,
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "progressive": has_video and has_audio,
        }
        if has_video and has_audio:
            entry.update(
                label=f"{height or '?'}p" + (" HD" if height and height >= 720 else ""),
                quality=height or 1,
            )
            progressive.append(entry)
        elif has_video:
            entry.update(label=f"{height or '?'}p (video)", quality=height or 1)
            video_only.append(entry)
        else:
            entry.update(
                label=f"audio {f.get('abr') or '?'}kbps {ext.upper()}",
                quality=int(f.get("abr") or 0),
            )
            audio_only.append(entry)

    # Some extractors (Instagram, Twitter, Facebook) return a single top URL.
    if not progressive and not video_only and not audio_only and info.get("url"):
        progressive.append(
            {
                "id": info.get("format_id") or "0",
                "url": info["url"],
                "ext": (info.get("ext") or "mp4").lower(),
                "mime": info.get("mime_type") or "video/mp4",
                "vcodec": info.get("vcodec") or "unknown",
                "acodec": info.get("acodec") or "unknown",
                "filesize": info.get("filesize"),
                "progressive": True,
                "label": "source",
                "quality": info.get("height") or 1,
            }
        )

    progressive.sort(key=lambda f: f["quality"], reverse=True)
    video_only.sort(key=lambda f: f["quality"], reverse=True)
    audio_only.sort(key=lambda f: f["quality"], reverse=True)

    streams = (progressive + video_only + audio_only)[:18]
    return {
        "streams": streams,
        "best": progressive[0] if progressive else (video_only[0] if video_only else (audio_only[0] if audio_only else None)),
        "audio": audio_only[0] if audio_only else None,
    }


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

    # Playlist / multi-entry pages: focus on the first media item.
    if "entries" in info:
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise RuntimeError("This playlist/link contains no playable media.")
        info = entries[0]

    picks = _pick_streams(info)
    duration = info.get("duration") or 0
    description = (info.get("description") or info.get("title") or "")[:2000]

    payload: Dict[str, Any] = {
        "title": info.get("title") or "Untitled media",
        "webpage_url": info.get("webpage_url") or info.get("url") or url,
        "extractor": info.get("extractor_key") or (info.get("extractor") or "generic"),
        "uploader": info.get("uploader") or info.get("channel") or info.get("uploader_id") or "unknown",
        "duration": duration,
        "duration_label": _fmt_duration(duration),
        "thumbnail": info.get("thumbnail"),
        "is_live": bool(info.get("is_live")),
        "view_count": info.get("view_count") or info.get("like_count"),
        "description": description,
        "streams": picks["streams"],
        "best": picks["best"],
        "audio": picks["audio"],
    }
    if not payload["best"]:
        raise RuntimeError("No playable stream format was found for this media.")
    return payload


def _fmt_duration(seconds: Optional[float]) -> str:
    try:
        seconds = int(seconds or 0)
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        return "live" if seconds == 0 else "0:00"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


@app.get("/api/stream")
async def api_stream(url: str = Query(..., min_length=8, description="Media page URL")):
    """Extract direct ad-free CDN stream URLs (yt-dlp, download=False)."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return {"error": "invalid_url", "message": "Please provide a valid http(s) media URL."}
    try:
        payload = await asyncio.to_thread(_extract_media_sync, url)
        return payload
    except yt_dlp.utils.DownloadError as exc:
        msg = str(exc).replace("ERROR:", "").strip()[:400]
        if "Sign in to confirm" in msg or "not a bot" in msg:
            msg = (
                "The platform is bot-checking this server's IP for this video. "
                "Wait a minute and retry, or try a different link."
            )
        return {"error": "extraction_failed", "message": msg or "The media could not be extracted."}
    except Exception as exc:  # defensive: never leak a raw traceback
        return {"error": "extraction_failed", "message": f"Extraction failed: {str(exc)[:300]}"}


# --------------------------------------------------------------------------- #
#  2) Zero-API live web search (duckduckgo_search)                             #
# --------------------------------------------------------------------------- #

def _ddg_search_sync(query: str, max_results: int = 10) -> Dict[str, Any]:
    web_results: List[Dict[str, Any]] = []
    news_results: List[Dict[str, Any]] = []
    errors: List[str] = []

    with DDGS(timeout=15) as ddgs:
        try:
            for r in ddgs.text(query, region="wt-wt", max_results=max_results):
                web_results.append(
                    {
                        "title": r.get("title") or "",
                        "url": r.get("href") or r.get("url") or "",
                        "body": (r.get("body") or "")[:500],
                        "source": (r.get("href") or r.get("url") or "").split("/")[2]
                        if "://" in (r.get("href") or r.get("url") or "")
                        else "web",
                    }
                )
        except Exception as exc:
            errors.append(f"web: {str(exc)[:120]}")
        try:
            for r in ddgs.news(query, region="wt-wt", max_results=5):
                news_results.append(
                    {
                        "title": r.get("title") or "",
                        "url": r.get("url") or "",
                        "body": (r.get("body") or r.get("excerpt") or "")[:500],
                        "source": r.get("source") or "news",
                        "date": r.get("date") or "",
                    }
                )
        except Exception as exc:
            errors.append(f"news: {str(exc)[:120]}")

    if not web_results and not news_results:
        return {
            "query": query,
            "results": [],
            "news": [],
            "error": "search_failed",
            "message": "No results right now (rate-limited or empty). Try again in a moment."
            + (f" Detail: {'; '.join(errors)}" if errors else ""),
        }
    return {"query": query, "results": web_results, "news": news_results, "error": None}


@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=2, description="Search query")):
    """Live web + news search with zero API tokens."""
    return await asyncio.to_thread(_ddg_search_sync, q, 10)


# --------------------------------------------------------------------------- #
#  3) Lightweight heuristic AI / chatbot engine                                #
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
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if 25 <= len(p.strip()) <= 400]


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
    """Rank sentences from (title, body) blocks against the query tokens."""
    qtokens = _tokenize(query)
    scored: List[tuple] = []
    for block in corpus:
        for position, sentence in enumerate(_sentences(block.get("body", ""))):
            score = _score(sentence, qtokens, position, 8)
            if qtokens:
                tmatch = len(qtokens & _tokenize(block.get("title", "")))
                score += 0.15 * min(tmatch, 3) / 3
            scored.append((score, sentence, block.get("title", ""), block.get("url", "")))
    scored.sort(key=lambda x: x[0], reverse=True)

    picked, seen, answer_parts = [], set(), []
    for score, sentence, title, url in scored:
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
    video: Optional[Dict[str, Any]] = None          # current media context
    search_results: Optional[List[Dict[str, Any]]] = None  # last search context
    auto_search: bool = True


def _chat_sync(payload: ChatRequest) -> Dict[str, Any]:
    query = payload.query.strip()
    ql = query.lower()
    video = payload.video or {}
    search_results = (payload.search_results or [])[:8]
    used_search = False

    # -- smalltalk ---------------------------------------------------------- #
    if ql in GREETINGS:
        return {
            "answer": (
                "Hey! I'm your co-pilot. Paste any YouTube / Instagram / X / Facebook "
                "link in the search bar and I'll pull it up ad-free, or ask me anything "
                "and I'll search the live web for you."
            ),
            "sources": [],
            "used_search": False,
        }
    if ql in THANKS:
        return {"answer": "Anytime! Ask me anything else — about the video playing or the wider web.", "sources": [], "used_search": False}

    # -- intent: summarize the current video -------------------------------- #
    wants_summary = bool(
        re.search(r"\b(summar|recap|what('| i)?s (this|it) about|overview|tl;?dr)\b", ql)
    )
    if wants_summary and video:
        title = video.get("title") or ""
        uploader = video.get("uploader") or ""
        desc = video.get("description") or ""
        corpus = [{"title": title, "body": f"{title}. {desc}", "url": video.get("webpage_url", "")}]
        result = _summarize_corpus(query or "summarize this video", corpus)
        head = f"Now playing: {title} by {uploader}. "
        if result["answer"]:
            return {
                "answer": head + "Key points:\n" + result["answer"],
                "sources": result["sources"],
                "used_search": False,
            }
        return {
            "answer": head + "The source has no long description to summarize, but it's "
            f"{video.get('duration_label', 'unknown length')} long. Ask me to search the "
            "web for more background on it.",
            "sources": [],
            "used_search": False,
        }

    # -- intent: related content / explicit web search ----------------------- #
    wants_web = bool(re.search(r"\b(search|look ?up|find|google|news|latest|trending)\b", ql))
    if not search_results and (wants_web or not video) and payload.auto_search:
        found = _ddg_search_sync(query, 8)
        search_results = found.get("results", [])[:6] + found.get("news", [])[:2]
        used_search = True

    # -- build corpus from available context -------------------------------- #
    corpus: List[Dict[str, Any]] = []
    if video:
        corpus.append(
            {
                "title": video.get("title") or "current video",
                "body": f"{video.get('title', '')} by {video.get('uploader', '')}. "
                f"{video.get('description', '')}",
                "url": video.get("webpage_url") or "",
            }
        )
    for r in search_results:
        corpus.append({"title": r.get("title", ""), "body": f"{r.get('title', '')}. {r.get('body', '')}", "url": r.get("url", "")})

    if corpus:
        result = _summarize_corpus(query, corpus)
        if result["answer"]:
            prefix = "Based on the video and live web context" if (video and used_search) else (
                "Based on the live web search" if used_search else "Based on the current context"
            )
            return {"answer": f"{prefix}:\n{result['answer']}", "sources": result["sources"], "used_search": used_search}

    if used_search:
        return {
            "answer": "I searched but found nothing solid for that. Try rephrasing, or add a "
            "couple of keywords (e.g. include a year or a name).",
            "sources": [],
            "used_search": True,
        }

    return {
        "answer": (
            "I don't have context for that yet. Paste a video URL or run a web search first, "
            "then ask me — I synthesize answers from the video details and the live search "
            "results on this page."
        ),
        "sources": [],
        "used_search": False,
    }


@app.post("/api/chat")
async def api_chat(payload: ChatRequest):
    """Heuristic co-pilot: summarizes snippets and video context (< 50 MB RAM)."""
    return await asyncio.to_thread(_chat_sync, payload)


# --------------------------------------------------------------------------- #
#  4) Chunked media proxy (keeps free-tier RAM flat)                           #
# --------------------------------------------------------------------------- #

def _host_allowed(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return False
    return any(host == s or host.endswith("." + s) for s in ALLOWED_PROXY_SUFFIXES)


@app.get("/api/proxy")
async def api_proxy(url: str, request: Request):
    """Stream CDN media through the server in 64 KB chunks.

    Direct CDN links extracted by yt-dlp are IP-locked to the server, so the
    browser can't always fetch them directly. We relay bytes without ever
    holding the media in memory, forwarding Range requests for seeking.
    """
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

    passthrough = {
        k: v
        for k, v in upstream.headers.items()
        if k.lower() in ("content-type", "content-length", "accept-ranges", "content-range", "etag", "last-modified")
    }
    return StreamingResponse(relay(), status_code=upstream.status_code, headers=passthrough)


# --------------------------------------------------------------------------- #
#  5) Frontend — single embedded HTML/CSS/JS page                              #
# --------------------------------------------------------------------------- #

FRONTEND = r"""
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Neura Stream — ad-free media + live search co-pilot</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = {
  darkMode: 'class',
  theme: { extend: {
    colors: { forge: { 500:'#d946ef', 600:'#c026d3', 400:'#e879f9', cyan:'#22d3ee' } },
    fontFamily: { sans: ['Inter','system-ui','sans-serif'] },
    keyframes: {
      floaty: { '0%,100%': { transform:'translateY(0px)' }, '50%': { transform:'translateY(-14px)' } },
      fadeUp: { '0%': { opacity:0, transform:'translateY(10px)' }, '100%': { opacity:1, transform:'translateY(0)' } },
      pulseGlow: { '0%,100%': { opacity:.5 }, '50%': { opacity:.9 } },
    },
    animation: { floaty:'floaty 7s ease-in-out infinite', fadeUp:'fadeUp .45s ease-out both', pulseGlow:'pulseGlow 2.4s ease-in-out infinite' },
  }}
}
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  ::-webkit-scrollbar { width: 8px; height: 8px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: rgba(217,70,239,.25); border-radius: 8px; }
  ::-webkit-scrollbar-thumb:hover { background: rgba(217,70,239,.45); }
  body { background:#07070d; }
  .glass { background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08); backdrop-filter: blur(18px); }
  .glow-blob { position:fixed; border-radius:9999px; filter: blur(110px); z-index:-1; pointer-events:none; }
  input[type=range].forge-seek { -webkit-appearance:none; appearance:none; height:5px; border-radius:99px;
    background:linear-gradient(90deg,#d946ef var(--fill,0%), rgba(255,255,255,.14) var(--fill,0%)); cursor:pointer; }
  input[type=range].forge-seek::-webkit-slider-thumb { -webkit-appearance:none; width:14px; height:14px; border-radius:99px;
    background:#fff; box-shadow:0 0 12px rgba(217,70,239,.9); transition: transform .15s; }
  input[type=range].forge-seek::-webkit-slider-thumb:hover { transform:scale(1.25); }
  input[type=range].forge-seek::-moz-range-thumb { width:14px; height:14px; border:none; border-radius:99px; background:#fff; box-shadow:0 0 12px rgba(217,70,239,.9); }
  .msg-in { animation: fadeUp .3s ease-out both; }
  .chip { transition: all .18s; }
  .chip:hover { transform: translateY(-1px); background: rgba(217,70,239,.15); border-color: rgba(217,70,239,.4); }
  .card-hover { transition: all .2s ease; }
  .card-hover:hover { transform: translateY(-3px); border-color: rgba(217,70,239,.35); box-shadow: 0 12px 40px -12px rgba(217,70,239,.25); }
  video::cue { background: rgba(0,0,0,.7); }
  .no-scrollbar::-webkit-scrollbar { display:none; }
</style>
</head>
<body class="text-gray-100 font-sans min-h-screen antialiased">

<div class="glow-blob w-[38rem] h-[38rem] bg-fuchsia-600/20 -top-40 -left-40 animate-pulseGlow"></div>
<div class="glow-blob w-[30rem] h-[30rem] bg-cyan-500/15 top-1/3 -right-40 animate-pulseGlow" style="animation-delay:.8s"></div>
<div class="glow-blob w-[26rem] h-[26rem] bg-violet-700/20 bottom-0 left-1/3"></div>

<!-- ============ HEADER ============ -->
<header class="sticky top-0 z-40 glass border-b border-white/10">
  <div class="max-w-[1600px] mx-auto px-4 sm:px-6 h-16 flex items-center gap-4">
    <div class="flex items-center gap-3">
      <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-fuchsia-500 to-cyan-400 grid place-items-center shadow-lg shadow-fuchsia-500/30">
        <svg class="w-5 h-5 text-white" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/><rect x="2" y="4" width="3.5" height="16" rx="1.5"/></svg>
      </div>
      <div class="leading-tight">
        <h1 class="font-extrabold text-lg tracking-tight bg-gradient-to-r from-fuchsia-400 via-white to-cyan-300 bg-clip-text text-transparent">Neura Stream</h1>
        <p class="text-[11px] text-gray-500 hidden sm:block">zero-API media engine + co-pilot</p>
      </div>
    </div>
    <div class="flex-1"></div>
    <div class="hidden md:flex items-center gap-2 text-[11px] text-gray-500">
      <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span> no API keys · no ads · no tracking
    </div>
    <button id="chat-toggle" class="lg:hidden chip px-3 py-2 rounded-xl glass text-sm flex items-center gap-2">
      <svg class="w-4 h-4 text-fuchsia-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 10h8M8 14h5M21 12a9 9 0 1 1-4.4-7.7L21 3l-1 4.4A8.96 8.96 0 0 1 21 12Z"/></svg>
      Co-pilot
    </button>
  </div>
</header>

<!-- ============ MAIN ============ -->
<main class="max-w-[1600px] mx-auto px-4 sm:px-6 py-6 grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_400px] gap-6">

  <!-- ---------- LEFT / MAIN COLUMN ---------- -->
  <section class="min-w-0 space-y-6">

    <!-- Hero search -->
    <div class="glass rounded-3xl p-6 sm:p-8 relative overflow-hidden animate-fadeUp">
      <div class="absolute inset-0 bg-gradient-to-br from-fuchsia-600/10 via-transparent to-cyan-500/10 pointer-events-none"></div>
      <h2 class="text-2xl sm:text-3xl font-extrabold tracking-tight mb-2">
        Paste a link. <span class="bg-gradient-to-r from-fuchsia-400 to-cyan-300 bg-clip-text text-transparent">Or ask the web.</span>
      </h2>
      <p class="text-sm text-gray-400 mb-5">YouTube · Instagram Reels · Twitter/X · Facebook — direct ad-free CDN streams, live search & an AI co-pilot. No keys, no accounts.</p>
      <form id="main-form" class="flex flex-col sm:flex-row gap-3">
        <div class="relative flex-1">
          <svg class="w-5 h-5 absolute left-4 top-1/2 -translate-y-1/2 text-gray-500" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="m21 21-4.35-4.35M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0Z"/></svg>
          <input id="main-input" autocomplete="off" placeholder="https://youtube.com/watch?v=… or a search query"
            class="w-full pl-12 pr-4 py-3.5 rounded-2xl bg-white/5 border border-white/10 focus:border-fuchsia-500/60 focus:ring-2 focus:ring-fuchsia-500/20 outline-none text-sm placeholder-gray-600 transition">
        </div>
        <button type="submit" class="px-6 py-3.5 rounded-2xl font-semibold text-sm bg-gradient-to-r from-fuchsia-500 to-fuchsia-600 hover:from-fuchsia-400 hover:to-fuchsia-500 shadow-lg shadow-fuchsia-500/30 transition-all hover:shadow-fuchsia-500/50 active:scale-95">
          Forge it
        </button>
      </form>
      <div class="flex flex-wrap gap-2 mt-4">
        <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="lofi hip hop radio">lofi beats</button>
        <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="https://www.youtube.com/watch?v=aqz-KE-bpKQ">Big Buck Bunny</button>
        <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="isro latest news">ISRO news</button>
        <button class="chip demo px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400" data-q="best budget earphones 2026">budget earphones</button>
      </div>
    </div>

    <!-- Status / loading line -->
    <div id="status-bar" class="hidden glass rounded-2xl px-5 py-3.5 text-sm flex items-center gap-3 animate-fadeUp">
      <svg class="w-5 h-5 text-fuchsia-400 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 0 1 8-8v4a4 4 0 0 0-4 4H4Z"/></svg>
      <span id="status-text" class="text-gray-300">Working…</span>
    </div>

    <!-- Error toast -->
    <div id="error-bar" class="hidden glass rounded-2xl px-5 py-4 text-sm border-red-500/30 bg-red-500/10 flex items-start gap-3 animate-fadeUp">
      <svg class="w-5 h-5 text-red-400 shrink-0 mt-0.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4m0 4h.01M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z"/></svg>
      <span id="error-text" class="text-red-200"></span>
    </div>

    <!-- Player card -->
    <div id="player-card" class="hidden glass rounded-3xl overflow-hidden animate-fadeUp">
      <div id="player-shell" class="relative group bg-black select-none">
        <video id="video" class="w-full aspect-video max-h-[70vh] bg-black" playsinline preload="metadata"></video>

        <!-- big center play -->
        <button id="big-play" class="absolute inset-0 grid place-items-center bg-black/20 opacity-0 group-hover:opacity-100 transition">
          <span class="w-20 h-20 rounded-full bg-fuchsia-500/90 shadow-2xl shadow-fuchsia-500/50 grid place-items-center hover:scale-110 transition">
            <svg class="w-9 h-9 text-white ml-1" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v13.72c0 .8.87 1.3 1.56.9l11.1-6.86a1.05 1.05 0 0 0 0-1.8L9.56 4.24A1.05 1.05 0 0 0 8 5.14Z"/></svg>
          </span>
        </button>

        <!-- controls -->
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

      <!-- media info -->
      <div class="p-5 sm:p-6">
        <div class="flex items-start gap-4">
          <img id="m-thumb" class="w-28 sm:w-40 aspect-video object-cover rounded-xl border border-white/10 bg-white/5" alt="">
          <div class="min-w-0 flex-1">
            <h3 id="m-title" class="font-bold text-base sm:text-lg leading-snug line-clamp-2"></h3>
            <p id="m-meta" class="text-xs text-gray-400 mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1"></p>
            <p id="m-desc" class="text-xs text-gray-500 mt-2 line-clamp-3"></p>
          </div>
        </div>
      </div>
    </div>

    <!-- Feed / results -->
    <div id="feed-section" class="hidden animate-fadeUp">
      <div class="flex items-center justify-between mb-4">
        <h3 id="feed-title" class="font-bold text-sm uppercase tracking-widest text-gray-400">Feed</h3>
        <span id="feed-count" class="text-xs text-gray-600"></span>
      </div>
      <div id="feed" class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-4"></div>
    </div>
  </section>

  <!-- ---------- RIGHT: AI CO-PILOT DRAWER ---------- -->
  <aside id="chat-panel" class="glass rounded-3xl flex flex-col overflow-hidden lg:sticky lg:top-24 lg:h-[calc(100vh-8rem)] h-[70vh] hidden lg:flex">
    <div class="px-5 py-4 border-b border-white/10 flex items-center gap-3">
      <div class="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-400 to-fuchsia-500 grid place-items-center shadow-lg shadow-cyan-500/20">
        <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 3l1.9 4.6L18.5 9.5l-4.6 1.9L12 16l-1.9-4.6L5.5 9.5l4.6-1.9L12 3Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M19 15l.9 2.1L22 18l-2.1.9L19 21l-.9-2.1L16 18l2.1-.9L19 15Z"/></svg>
      </div>
      <div>
        <h3 class="font-bold text-sm">Co-pilot</h3>
        <p class="text-[11px] text-gray-500">video & live-web synthesis engine</p>
      </div>
      <div class="flex-1"></div>
      <button id="chat-close" class="lg:hidden p-2 rounded-lg hover:bg-white/10">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" d="M6 18 18 6M6 6l12 12"/></svg>
      </button>
    </div>

    <div id="chat-log" class="flex-1 overflow-y-auto p-4 space-y-4">
      <div class="msg-in flex gap-3">
        <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-400 to-fuchsia-500 shrink-0 grid place-items-center text-[10px] font-bold">AI</div>
        <div class="glass rounded-2xl rounded-tl-sm px-4 py-3 text-sm text-gray-200 leading-relaxed max-w-[85%]">
          Hey, I'm your co-pilot. Load a video and ask me to summarize it, or fire any question and I'll synthesize live web results.
        </div>
      </div>
    </div>

    <div class="px-4 pb-2 flex gap-2 overflow-x-auto no-scrollbar">
      <button class="chat-chip chip shrink-0 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400">Summarize this video</button>
      <button class="chat-chip chip shrink-0 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400">What is this about?</button>
      <button class="chat-chip chip shrink-0 px-3 py-1.5 rounded-full border border-white/10 bg-white/5 text-xs text-gray-400">Find related videos</button>
    </div>

    <form id="chat-form" class="p-3 border-t border-white/10 flex gap-2">
      <input id="chat-input" autocomplete="off" placeholder="Ask about the video or the web…"
        class="flex-1 px-4 py-3 rounded-xl bg-white/5 border border-white/10 focus:border-cyan-400/60 focus:ring-2 focus:ring-cyan-400/20 outline-none text-sm placeholder-gray-600 transition">
      <button class="px-4 py-3 rounded-xl bg-gradient-to-r from-cyan-400 to-fuchsia-500 hover:opacity-90 active:scale-95 transition shadow-lg shadow-fuchsia-500/20" aria-label="Send">
        <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="m22 2-7 20-4-9-9-4Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M22 2 11 13"/></svg>
      </button>
    </form>
  </aside>
</main>

<footer class="max-w-[1600px] mx-auto px-6 py-8 text-center text-[11px] text-gray-600">
  Neura Stream · single-file FastAPI app · yt-dlp + DDG + heuristic co-pilot · built for Render free tier
</footer>

<script>
/* ================= state ================= */
let currentMedia = null;      // info about loaded video
let currentStreams = [];      // available formats
let lastSearchResults = [];   // chat context
const $ = (id) => document.getElementById(id);

/* ================= helpers ================= */
const isUrl = (s) => /^https?:\/\/\S+\.\S+/i.test(s.trim());
const fmtTime = (s) => {
  if (!isFinite(s) || s < 0) return '0:00';
  s = Math.floor(s);
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), x = s%60;
  return h ? `${h}:${String(m).padStart(2,'0')}:${String(x).padStart(2,'0')}` : `${m}:${String(x).padStart(2,'0')}`;
};
const esc = (s) => { const d = document.createElement('div'); d.textContent = s ?? ''; return d.innerHTML; };

function setStatus(msg) {
  if (!msg) { $('status-bar').classList.add('hidden'); return; }
  $('status-text').textContent = msg;
  $('status-bar').classList.remove('hidden');
}
function showError(msg) {
  $('error-text').textContent = msg;
  $('error-bar').classList.remove('hidden');
  setTimeout(() => $('error-bar').classList.add('hidden'), 9000);
}

/* ================= main input router ================= */
$('main-form').addEventListener('submit', (e) => {
  e.preventDefault();
  const v = $('main-input').value.trim();
  if (!v) return;
  $('error-bar').classList.add('hidden');
  if (isUrl(v)) loadStream(v); else doSearch(v);
});
document.querySelectorAll('.demo').forEach(b => b.addEventListener('click', () => {
  $('main-input').value = b.dataset.q;
  $('main-form').requestSubmit();
}));

/* ================= stream loading ================= */
async function loadStream(url) {
  setStatus('Extracting direct CDN stream (yt-dlp)…');
  try {
    const res = await fetch(`/api/stream?url=${encodeURIComponent(url)}`);
    const data = await res.json();
    if (data.error) throw new Error(data.message || data.error);
    playMedia(data);
  } catch (err) {
    setStatus(null);
    showError('Stream extraction failed: ' + err.message);
  }
}

function playMedia(data) {
  setStatus(null);
  currentMedia = data;
  currentStreams = (data.streams || []).filter(s => s.progressive);

  $('m-title').textContent = data.title;
  $('m-meta').innerHTML = [
    `<span class="text-fuchsia-400 font-semibold">${esc(data.extractor)}</span>`,
    esc(data.uploader),
    data.duration_label ? esc(data.duration_label) : '',
    data.view_count ? esc((data.view_count > 1e6 ? (data.view_count/1e6).toFixed(1)+'M' : data.view_count > 1e3 ? (data.view_count/1e3).toFixed(0)+'K' : data.view_count) + ' views') : ''
  ].filter(Boolean).map(x => `<span>${x}</span>`).join('');
  $('m-desc').textContent = (data.description || '').slice(0, 260);
  if (data.thumbnail) $('m-thumb').src = data.thumbnail;
  $('player-card').classList.remove('hidden');
  $('player-card').scrollIntoView({ behavior: 'smooth', block: 'start' });

  // quality selector
  const sel = $('quality-select');
  sel.innerHTML = '';
  currentStreams.forEach((s, i) => {
    const o = document.createElement('option');
    o.value = i; o.textContent = s.label;
    sel.appendChild(o);
  });
  const audio = (data.streams || []).find(s => !s.progressive && s.acodec !== 'none' && s.vcodec === 'none');
  if (audio) { const o = document.createElement('option'); o.value = 'audio'; o.textContent = 'audio only'; sel.appendChild(o); }

  const best = currentStreams[0] || data.best;
  setVideoSource(best.url, true);
}

function setVideoSource(cdnUrl, resetTime) {
  const v = $('video');
  const t = resetTime ? 0 : v.currentTime;
  v.src = `/api/proxy?url=${encodeURIComponent(cdnUrl)}`;
  v.currentTime = t;
  v.play().catch(() => {});
}

$('quality-select').addEventListener('change', (e) => {
  if (!currentMedia) return;
  const val = e.target.value;
  const src = val === 'audio'
    ? (currentMedia.streams || []).find(s => !s.progressive && s.acodec !== 'none' && s.vcodec === 'none')
    : currentStreams[+val];
  if (src) setVideoSource(src.url, false);
});

/* ================= player ================= */
const video = $('video');
$('big-play').addEventListener('click', () => video.paused ? video.play() : video.pause());
$('btn-play').addEventListener('click', () => video.paused ? video.play() : video.pause());
video.addEventListener('click', () => video.paused ? video.play() : video.pause());
video.addEventListener('play',  () => { $('ic-play').classList.add('hidden'); $('ic-pause').classList.remove('hidden'); });
video.addEventListener('pause', () => { $('ic-pause').classList.add('hidden'); $('ic-play').classList.remove('hidden'); });

const seek = $('seek');
function paintSeek(el) {
  const pct = ((el.value - el.min) / (el.max - el.min)) * 100;
  el.style.setProperty('--fill', pct + '%');
}
video.addEventListener('timeupdate', () => {
  if (!seeking) { seek.value = video.currentTime * (1000 / (video.duration || 1)); paintSeek(seek); }
  $('t-now').textContent = fmtTime(video.currentTime);
});
video.addEventListener('loadedmetadata', () => { $('t-dur').textContent = fmtTime(video.duration); });
let seeking = false;
seek.addEventListener('input', () => { seeking = true; paintSeek(seek); });
seek.addEventListener('change', () => {
  video.currentTime = seek.value * (video.duration || 0) / 1000;
  seeking = false;
});
paintSeek(seek);

const volume = $('volume'); volume.value = video.volume; paintSeek(volume);
volume.addEventListener('input', () => { video.volume = +volume.value; video.muted = false; syncMuteIcon(); paintSeek(volume); });
$('btn-mute').addEventListener('click', () => { video.muted = !video.muted; syncMuteIcon(); });
function syncMuteIcon() {
  const muted = video.muted || video.volume === 0;
  $('ic-vol').classList.toggle('hidden', muted);
  $('ic-muted').classList.toggle('hidden', !muted);
}

$('btn-pip').addEventListener('click', async () => {
  try {
    if (document.pictureInPictureElement) await document.exitPictureInPicture();
    else await video.requestPictureInPicture();
  } catch (e) { showError('Picture-in-picture is not available here.'); }
});
$('btn-fs').addEventListener('click', () => {
  const shell = $('player-shell');
  if (document.fullscreenElement) document.exitFullscreen();
  else shell.requestFullscreen && shell.requestFullscreen();
});
document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
  if (e.code === 'Space') { e.preventDefault(); video.paused ? video.play() : video.pause(); }
  if (e.code === 'ArrowRight') video.currentTime = Math.min(video.duration || 1e9, video.currentTime + 5);
  if (e.code === 'ArrowLeft') video.currentTime = Math.max(0, video.currentTime - 5);
});

/* ================= web search ================= */
async function doSearch(q) {
  setStatus('Searching the live web (DuckDuckGo)…');
  try {
    const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    if (data.error && !data.results.length) throw new Error(data.message || 'search failed');
    lastSearchResults = data.results;
    renderFeed(
      [...data.results.map(r => ({ kind: 'web', ...r })),
       ...data.news.map(r => ({ kind: 'news', ...r }))],
      `Results for “${q}”`
    );
    setStatus(null);
  } catch (err) {
    setStatus(null);
    showError('Search failed: ' + err.message);
  }
}

function renderFeed(items, title) {
  const feed = $('feed');
  feed.innerHTML = '';
  $('feed-title').textContent = title;
  $('feed-count').textContent = `${items.length} items`;
  items.forEach(item => {
    const isVideoish = /youtu\.?be|instagram|fb\.watch|twitter\.com|x\.com/i.test(item.url || '');
    const card = document.createElement('div');
    card.className = 'glass card-hover rounded-2xl p-4 cursor-pointer flex flex-col gap-2';
    card.innerHTML = `
      <div class="flex items-center gap-2 text-[10px] uppercase tracking-wider">
        <span class="px-2 py-0.5 rounded-full ${item.kind === 'news' ? 'bg-cyan-500/15 text-cyan-300' : 'bg-fuchsia-500/15 text-fuchsia-300'}">${item.kind}</span>
        <span class="text-gray-600 truncate">${esc(item.source || '')}</span>
      </div>
      <h4 class="text-sm font-semibold leading-snug line-clamp-2 text-gray-100">${esc(item.title)}</h4>
      <p class="text-xs text-gray-500 line-clamp-3 leading-relaxed">${esc(item.body || '')}</p>
      <div class="mt-auto pt-2 flex items-center gap-2">
        ${isVideoish ? '<button class="play-here px-3 py-1.5 rounded-lg text-[11px] font-semibold bg-fuchsia-500/20 text-fuchsia-300 border border-fuchsia-500/30 hover:bg-fuchsia-500/30 transition">▶ Play here</button>' : ''}
        <a href="${esc(item.url)}" target="_blank" rel="noopener" class="ml-auto text-[11px] text-gray-500 hover:text-cyan-300 transition underline underline-offset-2">open ↗</a>
      </div>`;
    if (isVideoish) card.querySelector('.play-here').addEventListener('click', () => loadStream(item.url));
    card.addEventListener('click', (e) => { if (e.target.tagName === 'A' || e.target.closest('button')) return; window.open(item.url, '_blank', 'noopener'); });
    feed.appendChild(card);
  });
  $('feed-section').classList.remove('hidden');
}

/* ================= co-pilot chat ================= */
function appendMsg(role, text, sources) {
  const log = $('chat-log');
  const wrap = document.createElement('div');
  wrap.className = 'msg-in flex gap-3 ' + (role === 'user' ? 'flex-row-reverse' : '');
  const body = text.split('\n').map(l => esc(l)).join('<br>');
  const srcHtml = (sources && sources.length)
    ? '<div class="mt-2 pt-2 border-t border-white/10 space-y-1">' +
      sources.map(s => `<a href="${esc(s.url)}" target="_blank" rel="noopener" class="block text-[11px] text-cyan-400/80 hover:text-cyan-300 truncate">↗ ${esc(s.title || s.url)}</a>`).join('') + '</div>'
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
  const holder = appendMsg('ai', '<span class="inline-flex gap-1.5 items-center text-gray-500"><span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400 animate-bounce"></span><span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400 animate-bounce" style="animation-delay:.15s"></span><span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400 animate-bounce" style="animation-delay:.3s"></span></span>');
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: q,
        video: currentMedia ? {
          title: currentMedia.title, uploader: currentMedia.uploader,
          description: currentMedia.description, duration_label: currentMedia.duration_label,
          webpage_url: currentMedia.webpage_url
        } : null,
        search_results: lastSearchResults.slice(0, 6).map(r => ({ title: r.title, url: r.url, body: r.body }))
      })
    });
    const data = await res.json();
    holder.remove();
    appendMsg('ai', data.answer || '…', data.sources);
  } catch (err) {
    holder.remove();
    appendMsg('ai', 'The co-pilot hit a snag reaching the server: ' + esc(err.message));
  }
}

$('chat-form').addEventListener('submit', (e) => { e.preventDefault(); const v = $('chat-input').value; $('chat-input').value = ''; sendChat(v); });
document.querySelectorAll('.chat-chip').forEach(c => c.addEventListener('click', () => {
  let q = c.textContent.trim();
  if (q === 'Find related videos' && currentMedia) q = 'search ' + currentMedia.title + ' video';
  sendChat(q);
}));

/* ================= mobile drawer ================= */
$('chat-toggle').addEventListener('click', () => {
  const p = $('chat-panel');
  const open = p.classList.toggle('hidden');
  p.classList.toggle('flex', !open || window.innerWidth >= 1024);
  if (!open) p.scrollIntoView({ behavior: 'smooth' });
});
$('chat-close').addEventListener('click', () => $('chat-panel').classList.add('hidden'));
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FRONTEND)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
#  Entrypoint — Render binds $PORT dynamically                                 #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
