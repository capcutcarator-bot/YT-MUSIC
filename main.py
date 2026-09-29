import json, os, re, tempfile, time
import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from yt_dlp import YoutubeDL

BASE = os.path.dirname(os.path.abspath(__file__))
COOKIE_JSON = os.path.join(BASE, "cookies.json")
COOKIE_TXT = os.path.join(tempfile.gettempdir(), "yt_cookies.txt")
CACHE_TTL = 1800  # seconds
CACHE = {}

app = FastAPI(title="YouTube Music (Audio Only) API")


def build_cookie_file():
    """cookies.json (browser export) -> Netscape cookies.txt for yt-dlp"""
    lines = ["# Netscape HTTP Cookie File"]
    for c in json.load(open(COOKIE_JSON, encoding="utf-8")):
        dom = c["domain"]
        lines.append("\t".join([
            dom,
            "TRUE" if dom.startswith(".") else "FALSE",
            c.get("path", "/"),
            "TRUE" if c.get("secure") else "FALSE",
            str(int(c.get("expirationDate") or 0)),
            c["name"],
            c["value"],
        ]))
    with open(COOKIE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


build_cookie_file()


def opts(**extra):
    o = {"quiet": True, "no_warnings": True, "cookiefile": COOKIE_TXT,
         "noplaylist": True, "skip_download": True}
    o.update(extra)
    return o


def resolve_id(id=None, url=None):
    if id and re.fullmatch(r"[\w-]{11}", id):
        return id
    if url:
        m = re.search(r"(?:v=|youtu\.be/|shorts/|embed/|music\.youtube\.com/watch\?v=)([\w-]{11})", url)
        if m:
            return m.group(1)
    raise HTTPException(400, "Valid id or url required")


def fmt_dur(s):
    if not s:
        return None
    s = int(s)
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def get_audio(vid):
    hit = CACHE.get(vid)
    if hit and hit["exp"] > time.time():
        return hit["data"]
    try:
        with YoutubeDL(opts(format="bestaudio[ext=m4a]/bestaudio/best")) as y:
            info = y.extract_info(f"https://www.youtube.com/watch?v={vid}", download=False)
    except Exception as e:
        raise HTTPException(502, f"Extract failed: {str(e)[:200]}")
    ext = info.get("ext") or "m4a"
    data = {
        "id": vid,
        "title": info.get("track") or info.get("title"),
        "artist": info.get("artist") or info.get("channel") or info.get("uploader"),
        "duration": info.get("duration"),
        "duration_text": fmt_dur(info.get("duration")),
        "views": info.get("view_count"),
        "likes": info.get("like_count"),
        "thumbnail": info.get("thumbnail") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "ext": ext,
        "mime": "audio/mp4" if ext == "m4a" else f"audio/{ext}",
        "bitrate_kbps": int(info.get("abr") or 0) or None,
        "filesize": info.get("filesize") or info.get("filesize_approx"),
        "stream_url": info["url"],
        "headers": info.get("http_headers", {}),
    }
    CACHE[vid] = {"exp": time.time() + CACHE_TTL, "data": data}
    return data


@app.get("/")
def home():
    return {"status": "ok", "endpoints": ["/search?q=", "/audio?id=|url=", "/stream?id=|url="]}


@app.get("/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=25)):
    try:
        with YoutubeDL(opts(extract_flat=True)) as y:
            r = y.extract_info(f"ytsearch{limit}:{q}", download=False)
    except Exception as e:
        raise HTTPException(502, f"Search failed: {str(e)[:200]}")
    results = []
    for e in r.get("entries") or []:
        if not e or not e.get("id"):
            continue
        vid = e["id"]
        results.append({
            "id": vid,
            "title": e.get("title"),
            "artist": e.get("channel") or e.get("uploader"),
            "duration": e.get("duration"),
            "duration_text": fmt_dur(e.get("duration")),
            "views": e.get("view_count"),
            "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            "url": f"https://youtu.be/{vid}",
        })
    return {"query": q, "count": len(results), "results": results}


@app.get("/audio")
def audio(request: Request, id: str = None, url: str = None):
    vid = resolve_id(id, url)
    d = get_audio(vid)
    out = {k: v for k, v in d.items() if k not in ("stream_url", "headers")}
    out["download_url"] = f"{str(request.base_url).rstrip('/')}/stream?id={vid}"
    return out


@app.get("/stream")
async def stream(request: Request, id: str = None, url: str = None):
    vid = resolve_id(id, url)
    a = await run_in_threadpool(get_audio, vid)
    h = dict(a["headers"])
    if request.headers.get("range"):
        h["Range"] = request.headers["range"]
    client = httpx.AsyncClient(timeout=None, follow_redirects=True)
    r = await client.send(client.build_request("GET", a["stream_url"], headers=h), stream=True)
    if r.status_code >= 400:
        await r.aclose()
        await client.aclose()
        CACHE.pop(vid, None)
        raise HTTPException(502, f"Upstream returned {r.status_code}")
    headers = {k: r.headers[k] for k in ("content-length", "content-range", "accept-ranges") if k in r.headers}
    safe = re.sub(r"[^A-Za-z0-9 _.-]", "", a["title"] or "")[:60].strip() or vid
    headers["Content-Disposition"] = f'attachment; filename="{safe}.{a["ext"]}"'

    async def gen():
        try:
            async for chunk in r.aiter_bytes(65536):
                yield chunk
        finally:
            await r.aclose()
            await client.aclose()

    return StreamingResponse(gen(), status_code=r.status_code, media_type=a["mime"], headers=headers)
