#!/usr/bin/env python3
# TuTaLab — máy đọc chạy trên GitHub Actions (di trú 25/11/2026).
# Nhận job từ Cloudflare (payload KV ghjob:<batch>, hàng đợi D1 gh_jobs),
# tổng hợp từng unit bằng VieNeu, ghép + loudnorm, ghi kết quả về KV
# (ghres:<batch>, ghbin:<batch>:mp3|wav) và cập nhật D1.
# Hộp thư KV QUAY VÒNG 18 kho (bổ sung 25/11/2026, lệnh Sếp): ghim theo
# batch — home = fnv1a(batch) % 18 trên bảng STORES cố định dưới đây (khớp
# 1-1 GH_KV_STORES trong functions/_kvrot.js). Đọc: quét vòng từ kho nhà
# (hoặc từ gh_jobs.kv_home nếu có) -> bắt được cả bản failover và payload
# legacy nằm ở kho 0. Ghi kết quả: ghi vào ĐÚNG kho đã tìm thấy payload,
# đồng thời chốt chỉ số kho đó vào gh_jobs.kv_home lúc claim.
# Biến môi trường bắt buộc: CF_API_TOKEN, CF_ACCOUNT_ID (acc phu — D1 +
# kho 0). CF_KV_TOKENS: JSON mảng 18 token theo thứ tự STORES (secret
# repo); thiếu thì chỉ dùng được kho 0 bằng CF_API_TOKEN (chế độ legacy).
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
API = "https://api.cloudflare.com/client/v4"
WORK = "/tmp/tljob"
os.makedirs(WORK, exist_ok=True)

# Bảng 18 kho KV cố định: (account_id, namespace_id) — khớp GH_KV_STORES.
STORES = [
    ("26f3739dd5a7ee1211f5e8f42afcf9ac", "34f47b0eccd24017b46be3848893ea16"),
    ("95ed31bf4dde5572246974a2fcb04a45", "bbb837109b754f9197f7fe8796065ea4"),
    ("75a7fb0671ff1bbbcdeca841b8ad8dca", "d580abd16d384474845945e3e0490383"),
    ("feb7574f992c27d766e75c321e084e94", "b3773c325eef4b6ca0760a9c75150293"),
    ("c6548079187561f080539f63557df1ac", "76e39c5b93454910975eefddcf480846"),
    ("1e71728a7b4c5925285090e9e0ca3752", "a4ab1b5df11b418ea11771e252766454"),
    ("829448d351fbf6ea0f091caa41c75b6b", "177d324051484879b9967636e7aeda59"),
    ("41ab824cc4d71bc8442d96173094a853", "f57b429d89904464979143278184572c"),
    ("203145c493bf67f61dae552dbdeeec7e", "61d2994925164c9c8d3c0e01577729a7"),
    ("b372bed878310ef8b1cd79c74e8d783c", "e934723180014f6fad8e7df637875d4d"),
    ("afe3af72dcf6c79d782c1d296340d014", "729b22a4165e43d7b64783dd1e0348a5"),
    ("ded6043c18fb4657e1610fabbc3cb76a", "30dab3892b88436eaf55913d6ea794b9"),
    ("aefb4933c03e20c1484a381c98f32ea9", "87123a2a0bab48bea461fe47661c28d6"),
    ("66a4e47f6b8f4c9fee9e1608766fc7a0", "ce4a7164a63446f48cc5c8056aec30a2"),
    ("360087a92d9f847f644f9e9532f6a473", "47c4fc7d04e142c6b8effa9a28620b6b"),
    ("5e4523005580433e1039e6ce920dff1b", "fb1bce981048448097eb94ea9d051b62"),
    ("12fe4b00fa45d8d4c6210c512c4b4b56", "fbdec58c1eb047fead6d15563a08d546"),
    ("3794ac518d2bd7b0d2140aa999e12206", "4e13a5ac0363481c9c722c0aae7d28c3"),
]
try:
    _rot = json.loads(os.environ.get("CF_KV_TOKENS") or "[]")
except Exception:
    _rot = []
ROT_TOKENS = [(_rot[i] if i < len(_rot) and _rot[i] else None)
              for i in range(len(STORES))]
if ROT_TOKENS[0] is None:
    ROT_TOKENS[0] = TOKEN


def fnv1a(s):
    """FNV-1a 32-bit — khớp bản JS trong _kvrot.js (khóa ASCII)."""
    h = 0x811C9DC5
    for ch in str(s):
        h ^= ord(ch)
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def home_idx(pin):
    return fnv1a(pin) % len(STORES)


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


def _kv_url(idx, key):
    acct, ns = STORES[idx]
    return (f"{API}/accounts/{acct}/storage/kv/namespaces/{ns}"
            f"/values/{urllib.parse.quote(key, safe='')}")


def kv_get_at(idx, key, binary=False):
    tok = ROT_TOKENS[idx]
    if not tok:
        return None
    req = urllib.request.Request(_kv_url(idx, key), method="GET")
    req.add_header("Authorization", "Bearer " + tok)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            b = r.read()
            return b if binary else b.decode()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def kv_put_at(idx, key, value):
    tok = ROT_TOKENS[idx]
    if not tok:
        raise RuntimeError(f"no token for store {idx}")
    data = value if isinstance(value, (bytes, bytearray)) else str(value).encode()
    req = urllib.request.Request(_kv_url(idx, key), method="PUT", data=data)
    req.add_header("Authorization", "Bearer " + tok)
    req.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(req, timeout=120) as r:
        r.read()


def _ring(start):
    n = len(STORES)
    return [(start + i) % n for i in range(n)]


def kv_get_pinned(pin, key, binary=False, start=None):
    """Quét vòng từ kho nhà của pin (hoặc từ start). Trả (value, idx)."""
    s0 = home_idx(pin) if start is None else start
    for idx in _ring(s0):
        if not ROT_TOKENS[idx]:
            continue
        try:
            v = kv_get_at(idx, key, binary=binary)
        except Exception as e:  # noqa: BLE001
            print(f"kv get store {idx} failed: {e}", flush=True)
            continue
        if v is not None:
            return v, idx
    return None, None


def kv_put_pinned(pin, key, value):
    """Ghi từ kho nhà của pin, failover vòng tròn. Trả idx đã ghi."""
    last = None
    for idx in _ring(home_idx(pin)):
        if not ROT_TOKENS[idx]:
            continue
        try:
            kv_put_at(idx, key, value)
            return idx
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"kv put store {idx} failed: {e}", flush=True)
    raise RuntimeError(f"kv put failed all stores: {last}")


# Kho chứa payload của job đang xử lý (process() chốt) — kết quả ghi về
# đúng kho này; synth_unit đọc ref clone theo ghim cid.
JOB_STORE = None


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


# ---------- Engine Rust (vieneu-rs int8) — MẶC ĐỊNH từ 25/11/2026 ----------
# Đo đối đầu job truyện 8.179 ký tự trên Actions (run 45462790733): Rust
# thắng Python 3.8.5 — wall_est 18,99s vs 24,96s, RTF 0,526 vs 0,636 ->
# Sếp chốt engine thắng làm mặc định toàn hệ. Phạm vi: unit giọng preset
# chạy Rust theo NHÓM cùng giọng (worker của dist nhận 1 giọng/lượt);
# clone/uvpreset vẫn đi Python; unit nào Rust không ra file thì tự lùi
# Python (synth_unit bên dưới) nên job không chết vì engine.
ENGINE = (os.environ.get("ENGINE") or "rust").lower()
_RUST_DIST = None


def rust_dist():
    global _RUST_DIST
    if _RUST_DIST:
        return _RUST_DIST
    url = os.environ.get("RUST_DIST_URL") or ""
    if not url:
        return None
    try:
        tar = "/tmp/vieneu-rust-dist.tar"
        if not os.path.exists(tar):
            urllib.request.urlretrieve(url, tar)
        dest = "/tmp/rustdist"
        if not os.path.exists(os.path.join(dest, "vieneu-rs")):
            os.makedirs(dest, exist_ok=True)
            subprocess.run(["tar", "-xf", tar, "-C", dest], check=True)
        for dp, _dn, fn in os.walk(dest):
            if "vieneu-rs" in fn:
                _RUST_DIST = dp
                return dp
    except Exception as e:  # noqa: BLE001
        print("rust dist unavailable:", e, flush=True)
    return None


def rust_synth_group(voice_name, items, jobdir):
    """items: [(idx, unit)] cùng 1 giọng preset. Trả {idx: wav48k}."""
    dist = rust_dist()
    if not dist:
        return {}
    work = os.path.join(jobdir, "rsgrp")
    os.makedirs(work, exist_ok=True)
    upath = os.path.join(work, "units.json")
    with open(upath, "w", encoding="utf-8") as f:
        json.dump({"voice": voice_name,
                   "units": [{"id": str(idx), "text": u["text"]}
                             for idx, u in items]}, f, ensure_ascii=False)
    loader = os.path.join(dist, "syslibs", "ld-linux-x86-64.so.2")
    # Hạn chót của worker là EPOCH tuyệt đối (bài học bench 25/11).
    deadline = str(int(time.time()) + 1500)
    cmd = [loader, "--library-path", os.path.join(dist, "syslibs"),
           os.path.join(dist, "vieneu-rs"),
           "--model-dir", os.path.join(dist, "model", "int8"),
           "--codec-dir", os.path.join(dist, "model", "codec"),
           "--model-root", os.path.join(dist, "model"),
           "--dict", os.path.join(dist, "assets", "sea_g2p.bin"),
           "--voices-file", os.path.join(dist, "assets", "voices_v3_turbo.json"),
           "worker", upath, "", "", work, deadline, "0"]
    env = dict(os.environ, OMP_NUM_THREADS="2", LC_ALL="C.UTF-8",
               LANG="C.UTF-8")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, cwd=dist,
                           env=env, timeout=1600)
        print("rust group", voice_name, "rc", r.returncode,
              (r.stdout or "")[-200:], flush=True)
    except Exception as e:  # noqa: BLE001
        print("rust group failed:", e, flush=True)
        return {}
    out = {}
    for idx, _u in items:
        seg = os.path.join(work, f"seg_{idx}.wav")
        if os.path.exists(seg) and os.path.getsize(seg) > 1000:
            dst = os.path.join(jobdir, f"u{idx:03d}.wav")
            rr = run(["ffmpeg", "-y", "-v", "error", "-i", seg,
                      "-ac", "1", "-ar", "48000", dst])
            if rr.returncode == 0:
                out[idx] = dst
    return out


def synth_unit(idx, unit, jobdir, prefill=None):
    """Trả đường dẫn wav 48k mono của unit, hoặc None nếu lỗi sau 2 lượt."""
    if prefill and idx in prefill:
        return prefill[idx]
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
                presets = json.loads(kv_get_at(0, "ghuv:presets") or "{}")
                entry = presets[voice["name"]]
                audio = eng.infer(text, voice=entry)
            elif voice.get("type") == "clone":
                ref = os.path.join(jobdir, f"ref_{voice['cid']}.bin")
                if not os.path.exists(ref):
                    data, _found = kv_get_pinned(
                        voice["cid"], f"ghclone:{voice['cid']}:ref",
                        binary=True, start=JOB_STORE)
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
    global JOB_STORE
    hint = None
    try:
        rows = d1("SELECT kv_home FROM gh_jobs WHERE id=?", [batch])["results"]
        if rows and rows[0].get("kv_home") is not None:
            hint = int(rows[0]["kv_home"])
    except Exception as e:  # noqa: BLE001
        print("kv_home hint read failed:", e, flush=True)
    raw, found = kv_get_pinned(batch, f"ghjob:{batch}", start=hint)
    if not raw:
        print("no payload for", batch)
        return
    JOB_STORE = found
    print(f"payload {batch} found in store {found} (home={home_idx(batch)})",
          flush=True)

    def put_result(key, value):
        try:
            kv_put_at(found, key, value)
        except Exception as e:  # noqa: BLE001
            print(f"result put store {found} failed ({e}); failover", flush=True)
            kv_put_pinned(batch, key, value)

    job = json.loads(raw)
    claim = d1("UPDATE gh_jobs SET status='running', kv_home=?, updated_at=? WHERE id=? AND status='queued'",
               [found, int(time.time()), batch])
    changed = (claim.get("meta") or {}).get("changes", 0)
    if not changed:
        print("job not claimable (already taken/done):", batch)
        return
    jobdir = os.path.join(WORK, batch)
    os.makedirs(jobdir, exist_ok=True)
    units = job.get("units") or []
    speed = float(job.get("speed") or 1)
    pitch = float(job.get("pitch") or 0)
    # Engine mặc định Rust: đọc trước các unit giọng preset theo nhóm giọng.
    prefill = {}
    if ENGINE == "rust":
        groups = {}
        for i, u in enumerate(units):
            if u.get("kind") == "pause":
                continue
            v = u.get("voice") or {"type": "preset", "name": "Trúc Ly"}
            if v.get("type") == "preset":
                groups.setdefault(v.get("name") or "Trúc Ly", []).append((i, u))
        for vname, items in groups.items():
            try:
                prefill.update(rust_synth_group(vname, items, jobdir))
            except Exception as e:  # noqa: BLE001
                print("rust prefill failed for", vname, e, flush=True)
        print(f"rust prefill: {len(prefill)}/{len(units)} units", flush=True)
    results = []
    wavs = []
    cues = []
    tcur = 0.0
    n_err = 0
    for i, u in enumerate(units):
        w = synth_unit(i, u, jobdir, prefill)
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
        put_result(f"ghres:{batch}", json.dumps(
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
        put_result(f"ghbin:{batch}:mp3", f.read())
    try:
        with open(final_wav, "rb") as f:
            kv_put_at(found, f"ghbin:{batch}:wav", f.read())
    except Exception as e:  # KV giới hạn 25MB — bản dài chỉ giữ MP3.
        print("wav put skipped:", e)
    put_result(f"ghres:{batch}", json.dumps(
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
                kv_put_pinned(b, f"ghres:{b}", json.dumps(
                    {"status": "error", "jobs": [], "error": str(e)[:200]}))
                d1("UPDATE gh_jobs SET status='error', error=?, updated_at=? WHERE id=?",
                   [str(e)[:200], int(time.time()), b])
            except Exception:
                pass


if __name__ == "__main__":
    main()
