#!/usr/bin/env python3
# TuTaLab — máy đọc chạy trên GitHub Actions (di trú 25/11/2026).
# Nhận job từ Cloudflare (payload KV ghjob:<batch>, hàng đợi D1 gh_jobs),
# tổng hợp từng unit bằng VieNeu, ghép + loudnorm, ghi kết quả về KV nhà
# (ghres:<batch>, ghbin:<batch>:mp3|wav) và cập nhật D1.
# Biến môi trường bắt buộc: CF_API_TOKEN, CF_ACCOUNT_ID.
# Tuỳ chọn: ENGINE=python (mặc định, pip vieneu 3.8.5) — nhánh Rust để dành
# khi chốt đo đối đầu; JOB_ID=<batch> xử lý 1 job, không đặt = quét job chờ.
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
import urllib.parse

ACCOUNT = os.environ["CF_ACCOUNT_ID"]
TOKEN = os.environ["CF_API_TOKEN"]
D1_ID = "c09b97d6-04bb-447e-a9c8-c5efdbfe02f9"
KV_ID = "34f47b0eccd24017b46be3848893ea16"
API = "https://api.cloudflare.com/client/v4"
WORK = "/tmp/tljob"
os.makedirs(WORK, exist_ok=True)


def http(method, url, data=None, ctype="application/json", raw=False):
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", "Bearer " + TOKEN)
    body = None
    if data is not None:
        body = data if isinstance(data, (bytes, bytearray)) else json.dumps(data).encode()
        req.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(req, body, timeout=120) as r:
            b = r.read()
            return b if raw else json.loads(b or b"{}")
    except urllib.error.HTTPError as e:
        if raw:
            return None
        raise RuntimeError(f"HTTP {e.code} {url}: {e.read()[:200]!r}")


def d1(sql, params=None):
    out = http("POST", f"{API}/accounts/{ACCOUNT}/d1/database/{D1_ID}/query",
               {"sql": sql, "params": params or []})
    if not out.get("success"):
        raise RuntimeError(f"D1 error: {out.get('errors')}")
    return out["result"][0]


def kv_get(key, binary=False):
    url = f"{API}/accounts/{ACCOUNT}/storage/kv/namespaces/{KV_ID}/values/{urllib.parse.quote(key, safe='')}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            b = r.read()
            return b if binary else b.decode()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def kv_put(key, value):
    url = f"{API}/accounts/{ACCOUNT}/storage/kv/namespaces/{KV_ID}/values/{urllib.parse.quote(key, safe='')}"
    data = value if isinstance(value, (bytes, bytearray)) else str(value).encode()
    http("PUT", url, data, ctype="application/octet-stream", raw=True)


def run(cmd, **kw):
    print("+", " ".join(str(c) for c in cmd), flush=True)
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def ff_dur(path):
    r = run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", path])
    try:
        return float(r.stdout.strip())
    except Exception:
        return 0.0


_tts = None


def tts():
    global _tts
    if _tts is None:
        from vieneu import Vieneu
        _tts = Vieneu()
    return _tts


def synth_unit(idx, unit, jobdir):
    """Trả đường dẫn wav 48k mono của unit, hoặc None nếu lỗi sau 2 lượt."""
    out = os.path.join(jobdir, f"u{idx:03d}.wav")
    if unit.get("kind") == "pause":
        dur = float(unit.get("dur") or 1)
        run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
             "anullsrc=r=48000:cl=mono", "-t", str(dur), out])
        return out
    text = unit["text"]
    voice = unit.get("voice") or {"type": "preset", "name": "Trúc Ly"}
    last_err = None
    for attempt in range(2):
        try:
            eng = tts()
            if voice.get("type") == "preset":
                audio = eng.infer(text, voice=voice["name"])
            elif voice.get("type") == "uvpreset":
                presets = json.loads(kv_get("ghuv:presets") or "{}")
                entry = presets[voice["name"]]
                audio = eng.infer(text, voice=entry)
            elif voice.get("type") == "clone":
                ref = os.path.join(jobdir, f"ref_{voice['cid']}.bin")
                if not os.path.exists(ref):
                    data = kv_get(f"ghclone:{voice['cid']}:ref", binary=True)
                    if not data:
                        raise RuntimeError("clone ref missing")
                    with open(ref, "wb") as f:
                        f.write(data)
                refwav = os.path.join(jobdir, f"ref_{voice['cid']}.wav")
                if not os.path.exists(refwav):
                    r = run(["ffmpeg", "-y", "-v", "error", "-i", ref,
                             "-t", "12", "-ac", "1", "-ar", "48000", refwav])
                    if r.returncode != 0:
                        raise RuntimeError("ref convert failed")
                audio = eng.infer(text, ref_audio=refwav)
            else:
                audio = eng.infer(text, voice="Trúc Ly")
            eng.save(audio, out)
            # Chuẩn hoá mọi unit về 48kHz mono.
            tmp = out + ".n.wav"
            r = run(["ffmpeg", "-y", "-v", "error", "-i", out, "-ac", "1",
                     "-ar", "48000", tmp])
            if r.returncode == 0:
                os.replace(tmp, out)
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            print(f"unit {idx} attempt {attempt} failed: {e}", flush=True)
            time.sleep(2)
    print(f"unit {idx} FAILED: {last_err}", flush=True)
    return None


def apply_speed_pitch(path, speed, pitch):
    if abs(speed - 1) < 0.01 and abs(pitch) < 0.01:
        return path
    out = path + ".fx.wav"
    filters = []
    if abs(pitch) > 0.01:
        f = 2 ** (pitch / 12.0)
        filters.append(f"asetrate=48000*{f:.5f},aresample=48000,atempo={1 / f:.5f}")
    if abs(speed - 1) > 0.01:
        # atempo chỉ nhận 0.5–2.0 mỗi chặng; tốc đã kẹp ở tầng nộp job.
        filters.append(f"atempo={speed:.4f}")
    r = run(["ffmpeg", "-y", "-v", "error", "-i", path, "-af",
             ",".join(filters), out])
    if r.returncode != 0:
        print("fx failed:", r.stderr[-300:], flush=True)
        return path
    return out


def process(batch):
    raw = kv_get(f"ghjob:{batch}")
    if not raw:
        print("no payload for", batch)
        return
    job = json.loads(raw)
    claim = d1("UPDATE gh_jobs SET status='running', updated_at=? WHERE id=? AND status='queued'",
               [int(time.time()), batch])
    changed = (claim.get("meta") or {}).get("changes", 0)
    if not changed:
        print("job not claimable (already taken/done):", batch)
        return
    jobdir = os.path.join(WORK, batch)
    os.makedirs(jobdir, exist_ok=True)
    units = job.get("units") or []
    speed = float(job.get("speed") or 1)
    pitch = float(job.get("pitch") or 0)
    results = []
    wavs = []
    cues = []
    tcur = 0.0
    n_err = 0
    for i, u in enumerate(units):
        w = synth_unit(i, u, jobdir)
        entry = {"id": f"{batch}:{i}", "kind": u.get("kind", "tts"),
                 "text": u.get("text", "")}
        if w is None:
            # Unit lỗi: thay bằng 0,5s lặng để batch không treo, ghi error.
            w = os.path.join(jobdir, f"u{i:03d}.wav")
            run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
                 "anullsrc=r=48000:cl=mono", "-t", "0.5", w])
            entry["status"] = "error"
            entry["error"] = "synth failed"
            n_err += 1
        else:
            entry["status"] = "done"
        w = apply_speed_pitch(w, speed, pitch)
        d = ff_dur(w)
        entry["dur"] = round(d, 3)
        cues.append({"i": i, "start": round(tcur, 3), "dur": round(d, 3),
                     "kind": entry["kind"], "text": entry["text"]})
        tcur += d + 0.22
        results.append(entry)
        wavs.append(w)
    all_err = len(units) > 0 and n_err >= len(units)
    if all_err:
        kv_put(f"ghres:{batch}", json.dumps(
            {"status": "error", "jobs": results, "cues": cues,
             "error": "all units failed"}))
        d1("UPDATE gh_jobs SET status='error', done=?, err=?, cues_json=?, error='all units failed', updated_at=? WHERE id=?",
           [len(units), n_err, json.dumps(cues), int(time.time()), batch])
        return
    # Ghép: chèn 0,22s lặng giữa các unit như bản PHP.
    sil = os.path.join(jobdir, "sil.wav")
    run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
         "anullsrc=r=48000:cl=mono", "-t", "0.22", sil])
    seq = []
    for i, w in enumerate(wavs):
        if i:
            seq.append(sil)
        seq.append(w)
    lst = os.path.join(jobdir, "list.txt")
    with open(lst, "w") as f:
        for p in seq:
            f.write(f"file '{p}'\n")
    joined = os.path.join(jobdir, "joined.wav")
    r = run(["ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0",
             "-i", lst, "-ac", "1", "-ar", "48000", joined])
    if r.returncode != 0:
        raise RuntimeError("concat failed: " + r.stderr[-300:])
    final_wav = os.path.join(jobdir, "final.wav")
    r = run(["ffmpeg", "-y", "-v", "error", "-i", joined, "-af",
             "loudnorm=I=-14:TP=-1:LRA=11", "-ac", "1", "-ar", "48000",
             final_wav])
    if r.returncode != 0:
        raise RuntimeError("loudnorm failed: " + r.stderr[-300:])
    final_mp3 = os.path.join(jobdir, "final.mp3")
    run(["ffmpeg", "-y", "-v", "error", "-i", final_wav, "-codec:a",
         "libmp3lame", "-b:a", "128k", final_mp3])
    with open(final_mp3, "rb") as f:
        kv_put(f"ghbin:{batch}:mp3", f.read())
    try:
        with open(final_wav, "rb") as f:
            kv_put(f"ghbin:{batch}:wav", f.read())
    except Exception as e:  # KV giới hạn 25MB — bản dài chỉ giữ MP3.
        print("wav put skipped:", e)
    kv_put(f"ghres:{batch}", json.dumps(
        {"status": "done", "jobs": results, "cues": cues}))
    d1("UPDATE gh_jobs SET status='done', done=?, err=?, cues_json=?, updated_at=? WHERE id=?",
       [len(units), n_err, json.dumps(cues), int(time.time()), batch])
    print("DONE", batch, "units", len(units), "err", n_err)


def sweep():
    rows = d1("SELECT id FROM gh_jobs WHERE status='queued' ORDER BY created_at LIMIT 3")["results"]
    return [r["id"] for r in rows]


def main():
    job_id = os.environ.get("JOB_ID") or ""
    ids = [job_id] if job_id else sweep()
    if not ids:
        print("no queued jobs")
        return
    for b in ids:
        try:
            process(b)
        except Exception as e:  # noqa: BLE001
            print("JOB FAILED", b, e, flush=True)
            try:
                kv_put(f"ghres:{b}", json.dumps(
                    {"status": "error", "jobs": [], "error": str(e)[:200]}))
                d1("UPDATE gh_jobs SET status='error', error=?, updated_at=? WHERE id=?",
                   [str(e)[:200], int(time.time()), b])
            except Exception:
                pass


if __name__ == "__main__":
    main()
