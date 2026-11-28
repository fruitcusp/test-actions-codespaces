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
# ENGINE: Rust thuần từ 26/11/2026 (lệnh Sếp "gỡ sạch") — không còn bản
# Python; JOB_ID=<batch> xử lý 1 job, không đặt = quét job chờ.
import json
import os
import re
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


# ---------- Engine Rust (vieneu-rs int8) — DUY NHẤT từ 26/11/2026 ----------
# Đo đối đầu job truyện 8.179 ký tự trên Actions (run 45462790733): Rust
# thắng Python 3.8.5 — wall_est 18,99s vs 24,96s, RTF 0,526 vs 0,636 ->
# Sếp chốt engine thắng làm mặc định toàn hệ, rồi lệnh "gỡ sạch"
# 25/11/2026: bỏ hẳn bản Python. Mọi unit đều chạy Rust: preset theo
# NHÓM cùng giọng; clone qua vieneu-rs enroll; uvpreset gộp entry vào
# voices file tạm rồi chạy Rust. Rust lỗi thì unit lỗi thẳng (0,5s lặng
# + entry error như cũ), không còn đường lùi Python.
ENGINE = "rust"
_RUST_DIST = None

# ---------- Chia đoạn SONG SONG (Sếp chốt 25/11 08:20) ----------
# Batch có tổng ký tự >= SPLIT_MIN_CHARS thì LUÔN chia nhiều luồng
# Actions chạy song song (kế hoạch do job "plan" tính tất định từ
# payload, các job "synth" đọc từng nhóm unit, job "assemble" ghép lại
# và chốt kết quả đúng hợp đồng cũ). Dưới ngưỡng -> đường đơn process()
# như cũ. Số luồng: cân bằng unit theo ký tự, mỗi luồng nhắm tới
# GROUP_TARGET_CHARS, trần MAX_STREAMS luồng (trần song song của tài
# khoản GitHub). Muốn đổi hành vi chia thì chỉ sửa 3 hằng số này.
SPLIT_MIN_CHARS = 1000
# Trần máy cho MỘT batch — TỐC ĐỘ TỐI ĐA, KHÔNG TIẾT KIỆM MÁY (lệnh
# Sếp 17:40 25/11/2026): 1 job được dùng tới trần 20 khe toàn tài
# khoản, GitHub tự lấp đầy khe còn trống tại thời điểm chạy; KHÔNG
# giữ khe dự phòng. (Bản trước: trần 8 + nhóm 500 ký tự.)
MAX_STREAMS = 20
# Chia mạnh tay: mỗi nhóm nhắm ~350 ký tự — unit tối đa 450 ký tự nên
# thực tế gần như mỗi unit một máy khi máy đang trống.
GROUP_TARGET_CHARS = 350
# Số threads cho binary Rust (vieneu-rs/ONNX Runtime): lấy từ biến môi
# trường RUST_THREADS nếu có (đặt bằng repo variable sau bench threads),
# mặc định 2 như bản đã kiểm chứng 25/11.
RUST_OMP_THREADS = os.environ.get("RUST_THREADS") or "2"


def rust_dist():
    global _RUST_DIST
    if _RUST_DIST:
        return _RUST_DIST
    t0 = time.time()
    dest = "/tmp/rustdist"

    def _find():
        for dp, _dn, fn in os.walk(dest):
            if "vieneu-rs" in fn:
                return dp
        return None

    try:
        # Ưu tiên thư mục đã giải nén (actions/cache restore sẵn): bỏ qua
        # cả tải tar lẫn giải nén. Toàn vẹn đã đối chiếu ở bước verify
        # trong workflow; ở đây hỏng/thiếu thì _find() không thấy và tự
        # lùi về đường tải Release như cũ.
        hit = _find()
        if hit:
            _RUST_DIST = hit
            print(f"rust dist: extracted dir ready (cache) in "
                  f"{time.time() - t0:.1f}s -> {hit}", flush=True)
            return hit
        url = os.environ.get("RUST_DIST_URL") or ""
        if not url:
            return None
        tar = "/tmp/vieneu-rust-dist.tar"
        src = "cached tar"
        if not os.path.exists(tar):
            urllib.request.urlretrieve(url, tar)
            src = "release download"
        if not os.path.exists(os.path.join(dest, "vieneu-rs")):
            os.makedirs(dest, exist_ok=True)
            subprocess.run(["tar", "-xf", tar, "-C", dest], check=True)
        hit = _find()
        if hit:
            _RUST_DIST = hit
            print(f"rust dist: ready via {src} + extract in "
                  f"{time.time() - t0:.1f}s -> {hit}", flush=True)
            return hit
    except Exception as e:  # noqa: BLE001
        print("rust dist unavailable:", e, flush=True)
    return None


def rust_synth_group(voice_name, items, jobdir, voices_file=None):
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
           "--voices-file", voices_file or os.path.join(dist, "assets", "voices_v3_turbo.json"),
           "worker", upath, "", "", work, deadline, "0"]
    env = dict(os.environ, OMP_NUM_THREADS=RUST_OMP_THREADS,
               LC_ALL="C.UTF-8", LANG="C.UTF-8")
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



_CLONE_CACHE = {}


def _rust_base_cmd(dist, voices_file):
    loader = os.path.join(dist, "syslibs", "ld-linux-x86-64.so.2")
    return [loader, "--library-path", os.path.join(dist, "syslibs"),
            os.path.join(dist, "vieneu-rs"),
            "--model-dir", os.path.join(dist, "model", "int8"),
            "--codec-dir", os.path.join(dist, "model", "codec"),
            "--model-root", os.path.join(dist, "model"),
            "--dict", os.path.join(dist, "assets", "sea_g2p.bin"),
            "--voices-file", voices_file]


def _base_presets(dist):
    p = os.path.join(dist, "assets", "voices_v3_turbo.json")
    with open(p, encoding="utf-8") as f:
        j = json.load(f)
    return j.get("presets") or {}


def _clone_voices_file(cid, jobdir):
    """Enroll ref clone bang vieneu-rs -> (ten giong clone, voices file gop).
    Cache theo cid trong 1 job. Raise neu enroll that bai."""
    if cid in _CLONE_CACHE:
        return _CLONE_CACHE[cid]
    dist = rust_dist()
    if not dist:
        raise RuntimeError("rust dist unavailable")
    ref = os.path.join(jobdir, f"ref_{cid}.bin")
    if not os.path.exists(ref):
        data, _found = kv_get_pinned(cid, f"ghclone:{cid}:ref",
                                     binary=True, start=JOB_STORE)
        if not data:
            raise RuntimeError("clone ref missing")
        with open(ref, "wb") as f:
            f.write(data)
    refwav = os.path.join(jobdir, f"ref_{cid}.wav")
    if not os.path.exists(refwav):
        r = run(["ffmpeg", "-y", "-v", "error", "-i", ref,
                 "-t", "12", "-ac", "1", "-ar", "48000", refwav])
        if r.returncode != 0:
            raise RuntimeError("ref convert failed")
    cname = "clone-" + re.sub(r"[^A-Za-z0-9_-]", "-", str(cid))
    base_vf = os.path.join(dist, "assets", "voices_v3_turbo.json")
    env = dict(os.environ, OMP_NUM_THREADS=RUST_OMP_THREADS,
               LC_ALL="C.UTF-8", LANG="C.UTF-8")
    r = subprocess.run(_rust_base_cmd(dist, base_vf) +
                       ["enroll", "--ref", refwav, "--name", cname],
                       capture_output=True, text=True, cwd=dist, env=env,
                       timeout=1200)
    if "ENROLLED" not in (r.stdout or ""):
        raise RuntimeError("enroll-no-confirm: " +
                           ((r.stdout or "") + (r.stderr or ""))[-200:])
    cands = []
    m = re.search(r"ENROLLED .*?->\s*(\S+)", r.stdout)
    if m:
        p = m.group(1)
        cands += [p, os.path.join(dist, p),
                  os.path.join(os.path.dirname(p), "voices_clone.json")]
    cands += [os.path.join(dist, "assets", "voices_clone.json"),
              os.path.join(dist, "voices_clone.json"),
              os.path.join(jobdir, "voices_clone.json"),
              os.path.join(os.getcwd(), "voices_clone.json")]
    clone_file = next((c for c in cands if c and os.path.isfile(c)), None)
    if clone_file is None:
        raise RuntimeError("enroll-no-clone-file")
    cj = json.load(open(clone_file, encoding="utf-8"))
    cpresets = cj.get("presets") if isinstance(cj, dict) else None
    if cpresets is None and isinstance(cj, dict):
        cpresets = cj if cname in cj else {cname: cj}
    if not cpresets or cname not in cpresets:
        raise RuntimeError("enroll-clone-name-missing")
    merged = {"meta": {"note": "base + clone"}, "default_voice": cname,
              "presets": dict(_base_presets(dist))}
    merged["presets"].update(cpresets)
    vf = os.path.join(jobdir, f"voices_clone_{cname}.json")
    with open(vf, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)
    _CLONE_CACHE[cid] = (cname, vf)
    return _CLONE_CACHE[cid]


_UV_CACHE = {}


def _uv_voices_file(name, jobdir):
    """Gop entry uvpreset (giong clone may cu) vao voices file tam -> path.
    Entry dung chung cau truc preset VieNeu v3 (codes/speaker_emb)."""
    if name in _UV_CACHE:
        return _UV_CACHE[name]
    dist = rust_dist()
    if not dist:
        raise RuntimeError("rust dist unavailable")
    presets = json.loads(kv_get_at(0, "ghuv:presets") or "{}")
    entry = presets[name]
    merged = {"meta": {"note": "base + uvpreset"}, "default_voice": name,
              "presets": dict(_base_presets(dist))}
    merged["presets"][name] = entry
    vf = os.path.join(jobdir, "voices_uv.json")
    with open(vf, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)
    _UV_CACHE[name] = vf
    return vf


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
    voice = unit.get("voice") or {"type": "preset", "name": "Trúc Ly"}
    last_err = None
    for attempt in range(2):
        try:
            vtype = voice.get("type")
            if vtype == "clone":
                cname, vf = _clone_voices_file(voice["cid"], jobdir)
                got = rust_synth_group(cname, [(idx, unit)], jobdir,
                                       voices_file=vf)
            elif vtype == "uvpreset":
                vf = _uv_voices_file(voice["name"], jobdir)
                got = rust_synth_group(voice["name"], [(idx, unit)],
                                       jobdir, voices_file=vf)
            elif vtype == "preset":
                got = rust_synth_group(voice.get("name") or "Trúc Ly",
                                       [(idx, unit)], jobdir)
            else:
                got = rust_synth_group("Trúc Ly", [(idx, unit)], jobdir)
            if idx in got:
                return got[idx]
            raise RuntimeError("rust synth produced no file")
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


# NHẠC NỀN (khôi phục đợt 2, 25/11/2026): bản cũ trộn nhạc ở tầng lấy
# file trên máy chủ cũ — khi di trú sang Actions, tầng đó mất nên tham
# số music của /api/audio bị bỏ qua (đợt 1 đo: file có nhạc MD5 y hệt
# bản không nhạc). Nay worker trộn SẴN 3 biến thể MP3 ngay khi chốt
# batch, khóa ghbin:<batch>:mp3:m<n> (n = số cuối của mã bản nhạc);
# 3 file nhạc nằm trong image tại /opt/music (sinh tổng hợp thay cho
# 3 bản gốc đã mất cùng máy cũ — cùng tên hiển thị trên trang: Nhẹ
# nhàng / Mưa rơi / Piano êm). Âm lượng trộn mặc định 0.15 khớp giá
# trị mặc định của ô chọn nhạc trên trang. Mọi lỗi ở khâu này đều
# nuốt: mất nhạc nền không bao giờ được làm hỏng bản đọc chính.
MUSIC_IDS = ["b100000000000001", "b100000000000002", "b100000000000003"]
MUSIC_VOL = 0.15
MUSIC_DIRS = ["/opt/music",
              os.path.join(os.path.dirname(os.path.abspath(__file__)), "music")]


def mix_music_variants(batch, final_mp3, jobdir, put):
    for mid in MUSIC_IDS:
        try:
            src = None
            for d in MUSIC_DIRS:
                p = os.path.join(d, mid + ".mp3")
                if os.path.isfile(p):
                    src = p
                    break
            if not src:
                print("music track not in image:", mid, flush=True)
                return
            out = os.path.join(jobdir, f"final_m{mid[-1]}.mp3")
            r = run(["ffmpeg", "-y", "-v", "error", "-i", final_mp3,
                     "-stream_loop", "-1", "-i", src,
                     "-filter_complex",
                     f"[1:a]volume={MUSIC_VOL}[m];"
                     "[0:a][m]amix=inputs=2:duration=first:"
                     "dropout_transition=0[a]",
                     "-map", "[a]", "-codec:a", "libmp3lame",
                     "-b:a", "128k", out])
            if r.returncode != 0:
                print("music mix failed", mid, (r.stderr or "")[-200:],
                      flush=True)
                continue
            with open(out, "rb") as f:
                put(f"ghbin:{batch}:mp3:m{mid[-1]}", f.read())
            print("music variant ok:", mid, flush=True)
        except Exception as e:  # noqa: BLE001
            print("music variant skipped", mid, e, flush=True)


def process(batch, claimed=False):
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
    if not changed and not claimed:
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
    mix_music_variants(batch, final_mp3, jobdir, put_result)
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


# ---------- Khối SONG SONG: plan -> synth (matrix) -> assemble ----------
# Hợp đồng với Pages giữ nguyên tuyệt đối: ghres:<batch> chỉ được ghi
# MỘT lần ở khâu chốt cuối (assemble, hoặc process() đường đơn) với đúng
# hình dạng cũ; ghbin/D1 cập nhật y hệt đường đơn; ví đã trừ 1 lần lúc
# nộp ở Pages, worker không bao giờ trừ nữa. Đoạn con trao đổi qua kho
# KV đã ghim của batch: ghseg:<batch>:<i> (wav 48k từng unit) +
# ghsegmeta:<batch>:<g> (JSON entries của nhóm g) + ghplan:<batch>.

def load_job(batch):
    """Đọc payload ghjob:<batch>. Trả (job, store_idx) hoặc (None, None)."""
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
        return None, None
    JOB_STORE = found
    print(f"payload {batch} found in store {found} (home={home_idx(batch)})",
          flush=True)
    return json.loads(raw), found


# Job kẹt ở trạng thái 'running' quá CLAIM_TTL_S giây (worker chết giữa
# chừng, run bị huỷ...) thì coi như claim đã hết hạn: worker khác được
# nhận lại bằng chính câu UPDATE có điều kiện này (nguyên tử trong D1,
# hai worker tranh nhau chỉ một bên thắng). 900s >> thời gian xử lý thật
# của batch dài nhất (5.000 ký tự, vài phút) nên không cướp việc oan.
CLAIM_TTL_S = 900


def claim_batch(batch, found):
    now = int(time.time())
    claim = d1("UPDATE gh_jobs SET status='running', kv_home=?, updated_at=? WHERE id=? AND (status='queued' OR (status='running' AND updated_at<?))",
               [found, now, batch, now - CLAIM_TTL_S])
    return (claim.get("meta") or {}).get("changes", 0) > 0


def put_result_g(batch, found, key, value):
    try:
        kv_put_at(found, key, value)
    except Exception as e:  # noqa: BLE001
        print(f"result put store {found} failed ({e}); failover", flush=True)
        kv_put_pinned(batch, key, value)


def batch_chars(job):
    return sum(len(u.get("text") or "") for u in job.get("units") or [])


def plan_groups(job):
    """Chia chỉ số unit thành các nhóm cân bằng theo ký tự (thuật toán
    LPT tất định: unit nặng nhất vào nhóm nhẹ nhất trước, hoà thì theo
    chỉ số — mọi job tính lại đều ra cùng một kế hoạch)."""
    units = job.get("units") or []
    total = batch_chars(job)
    g = max(1, min(MAX_STREAMS, len(units),
                   -(-total // GROUP_TARGET_CHARS)))
    order = sorted(range(len(units)),
                   key=lambda i: (-(len(units[i].get("text") or "") or 1), i))
    groups = [[] for _ in range(g)]
    loads = [0] * g
    for i in order:
        k = min(range(g), key=lambda k: (loads[k], k))
        groups[k].append(i)
        loads[k] += len(units[i].get("text") or "") or 1
    return [{"g": k, "units": sorted(v)} for k, v in enumerate(groups) if v]


def synth_indices(batch, job, found, jobdir, indices):
    """Đọc các unit có chỉ số trong indices (đúng ngữ nghĩa đường đơn:
    Rust prefill theo nhóm giọng preset, unit lỗi -> 0,5s lặng + entry
    error), chuẩn hoá file uNNN.wav đã gồm tốc độ/cao độ. Trả entries."""
    units = job.get("units") or []
    speed = float(job.get("speed") or 1)
    pitch = float(job.get("pitch") or 0)
    prefill = {}
    if ENGINE == "rust":
        groups = {}
        for i in indices:
            u = units[i]
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
        print(f"rust prefill: {len(prefill)}/{len(indices)} units", flush=True)
    entries = []
    for i in indices:
        u = units[i]
        w = synth_unit(i, u, jobdir, prefill)
        entry = {"i": i, "id": f"{batch}:{i}", "kind": u.get("kind", "tts"),
                 "text": u.get("text", "")}
        if w is None:
            w = os.path.join(jobdir, f"u{i:03d}.wav")
            run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
                 "anullsrc=r=48000:cl=mono", "-t", "0.5", w])
            entry["status"] = "error"
            entry["error"] = "synth failed"
        else:
            entry["status"] = "done"
        w = apply_speed_pitch(w, speed, pitch)
        canon = os.path.join(jobdir, f"u{i:03d}.wav")
        if os.path.abspath(w) != os.path.abspath(canon):
            os.replace(w, canon)
        entry["dur"] = round(ff_dur(canon), 3)
        entries.append(entry)
    return entries


def finalize_par(batch, job, found, jobdir, entries):
    """Chốt batch từ entries (mọi unit, file uNNN.wav đã có ở jobdir):
    ghép + loudnorm + tải ghbin/ghres + cập nhật D1 — y hệt đuôi của
    process() đường đơn."""
    units = job.get("units") or []
    entries = sorted(entries, key=lambda e: e["i"])
    results, cues = [], []
    tcur, n_err = 0.0, 0
    for e in entries:
        r = {"id": e["id"], "kind": e["kind"], "text": e["text"],
             "status": e["status"], "dur": e["dur"]}
        if "error" in e:
            r["error"] = e["error"]
        if e["status"] == "error":
            n_err += 1
        results.append(r)
        cues.append({"i": e["i"], "start": round(tcur, 3), "dur": e["dur"],
                     "kind": e["kind"], "text": e["text"]})
        tcur += e["dur"] + 0.22
    if len(units) > 0 and n_err >= len(units):
        put_result_g(batch, found, f"ghres:{batch}", json.dumps(
            {"status": "error", "jobs": results, "cues": cues,
             "error": "all units failed"}))
        d1("UPDATE gh_jobs SET status='error', done=?, err=?, cues_json=?, error='all units failed', updated_at=? WHERE id=?",
           [len(units), n_err, json.dumps(cues), int(time.time()), batch])
        return
    sil = os.path.join(jobdir, "sil.wav")
    run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
         "anullsrc=r=48000:cl=mono", "-t", "0.22", sil])
    seq = []
    for i in range(len(units)):
        if i:
            seq.append(sil)
        seq.append(os.path.join(jobdir, f"u{i:03d}.wav"))
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
        put_result_g(batch, found, f"ghbin:{batch}:mp3", f.read())
    mix_music_variants(batch, final_mp3, jobdir,
                       lambda k, v: put_result_g(batch, found, k, v))
    try:
        with open(final_wav, "rb") as f:
            kv_put_at(found, f"ghbin:{batch}:wav", f.read())
    except Exception as e:  # KV giới hạn 25MB — bản dài chỉ giữ MP3.
        print("wav put skipped:", e)
    put_result_g(batch, found, f"ghres:{batch}", json.dumps(
        {"status": "done", "jobs": results, "cues": cues}))
    d1("UPDATE gh_jobs SET status='done', done=?, err=?, cues_json=?, updated_at=? WHERE id=?",
       [len(units), n_err, json.dumps(cues), int(time.time()), batch])
    print("DONE", batch, "units", len(units), "err", n_err)


def plan_mode():
    job_id = os.environ.get("JOB_ID") or ""
    batch = job_id
    if not batch:
        rows = sweep()
        batch = rows[0] if rows else ""
    out = {"mode": "none", "batch": batch or "", "groups": "[]"}
    if batch:
        job, found = load_job(batch)
        if job and claim_batch(batch, found):
            total = batch_chars(job)
            if total >= SPLIT_MIN_CHARS:
                groups = plan_groups(job)
                put_result_g(batch, found, f"ghplan:{batch}", json.dumps(
                    {"groups": groups, "total_chars": total}))
                out = {"mode": "parallel", "batch": batch,
                       "groups": json.dumps(groups)}
            else:
                out = {"mode": "single", "batch": batch, "groups": "[]"}
            print(f"plan: batch={batch} chars={total} mode={out['mode']} "
                  f"groups={out['groups']}", flush=True)
        else:
            print("plan: batch not claimable:", batch, flush=True)
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"mode={out['mode']}\nbatch={out['batch']}\n"
                    f"groups={out['groups']}\n")
    print("PLAN", json.dumps(out), flush=True)
    return out


def planrun_mode():
    """plan + đường ngắn GỘP 1 job (image hoá 25/11): plan xong, nếu là
    batch ngắn (mode=single) thì xử lý luôn trong cùng tiến trình/run —
    đường ngắn chỉ mở máy 1 lần thay vì plan rồi single hai lượt. Batch
    dài: chỉ plan như cũ, synth/assemble chạy tiếp theo outputs."""
    out = plan_mode()
    if out.get("mode") == "single" and out.get("batch"):
        b = out["batch"]
        try:
            process(b, claimed=True)
        except Exception as e:  # noqa: BLE001
            print("JOB FAILED", b, e, flush=True)
            mark_error(b, e)


def synth_mode():
    batch = os.environ["JOB_ID"]
    g = int(os.environ["GROUP_INDEX"])
    job, found = load_job(batch)
    if not job:
        raise RuntimeError("no payload for " + batch)
    groups = None
    raw, _f = kv_get_pinned(batch, f"ghplan:{batch}", start=found)
    if raw:
        groups = json.loads(raw)["groups"]
    if groups is None:
        groups = plan_groups(job)
    grp = next(x for x in groups if x["g"] == g)
    jobdir = os.path.join(WORK, batch)
    os.makedirs(jobdir, exist_ok=True)
    entries = synth_indices(batch, job, found, jobdir, grp["units"])
    for e in entries:
        with open(os.path.join(jobdir, f"u{e['i']:03d}.wav"), "rb") as fh:
            put_result_g(batch, found, f"ghseg:{batch}:{e['i']}", fh.read())
    put_result_g(batch, found, f"ghsegmeta:{batch}:{g}",
                 json.dumps({"g": g, "entries": entries}))
    print(f"synth group {g} done: {[e['i'] for e in entries]}", flush=True)


def assemble_mode():
    batch = os.environ["JOB_ID"]
    job, found = load_job(batch)
    if not job:
        raise RuntimeError("no payload for " + batch)
    units = job.get("units") or []
    groups = None
    raw, _f = kv_get_pinned(batch, f"ghplan:{batch}", start=found)
    if raw:
        groups = json.loads(raw)["groups"]
    if groups is None:
        groups = plan_groups(job)
    jobdir = os.path.join(WORK, batch)
    os.makedirs(jobdir, exist_ok=True)
    entries = []
    for grp in groups:
        rawm, _f2 = kv_get_pinned(batch, f"ghsegmeta:{batch}:{grp['g']}",
                                  start=found)
        if rawm:
            entries += json.loads(rawm)["entries"]
    have = {e["i"] for e in entries}
    for i in range(len(units)):
        if i not in have:
            u = units[i]
            entries.append({"i": i, "id": f"{batch}:{i}",
                            "kind": u.get("kind", "tts"),
                            "text": u.get("text", ""), "status": "error",
                            "error": "segment missing", "dur": 0.5})
    for e in entries:
        dst = os.path.join(jobdir, f"u{e['i']:03d}.wav")
        if os.path.exists(dst):
            continue
        data, _f3 = kv_get_pinned(batch, f"ghseg:{batch}:{e['i']}",
                                  binary=True, start=found)
        if data:
            with open(dst, "wb") as fh:
                fh.write(data)
        if not os.path.exists(dst):
            run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i",
                 "anullsrc=r=48000:cl=mono", "-t", "0.5", dst])
            if e["status"] != "error":
                e["status"] = "error"
                e["error"] = "segment file missing"
                e["dur"] = 0.5
    finalize_par(batch, job, found, jobdir, entries)


# ---------- MÁY CHỜ THEO PHIÊN SOẠN (MODE=warmclaim, phiếu tốc độ 25/11) ----------
# Khác hẳn bể ấm cũ (đã tắt hẳn): đây là máy chờ ĐÁNH THỨC THEO SỰ KIỆN —
# khách bắt đầu soạn ở mục đọc thì Pages dispatch đúng 1 run warmclaim
# cho đúng chủ đó. Máy khởi động, kéo image xong thì poll D1 tìm job
# NGẮN (< SPLIT_MIN_CHARS) của đúng chủ trong cửa sổ tối đa
# WARMCLAIM_WINDOW_S giây; có job thì claim nguyên tử rồi đọc luôn trong
# máy nóng này (process đường đơn). Job dài bỏ qua — để nguyên cho run
# song song của chính nó. Hết cửa sổ không có việc: tự kết thúc sạch.
# Trần toàn hệ: tối đa WARMCLAIM_MAX_ACTIVE dòng 'waiting' trong bảng
# gh_warm (Pages cũng chặn trước khi dispatch; đây là chốt chặn thứ 2).
WARMCLAIM_WINDOW_S = 120
WARMCLAIM_POLL_S = 3
# TỐC ĐỘ TỐI ĐA (lệnh Sếp 17:40 25/11/2026): nới trần máy chờ toàn hệ
# từ 2 lên 6 (vẫn giữ dedupe 1 máy/chủ + cửa sổ 120s + không trừ gì).
WARMCLAIM_MAX_ACTIVE = 6


def _warm_set(wid, owner, state, started):
    try:
        d1("INSERT OR REPLACE INTO gh_warm(id,owner,state,started_at,updated_at) "
           "VALUES(?,?,?,?,?)",
           [wid, owner, state, started, int(time.time())])
    except Exception as e:  # noqa: BLE001
        print("warmclaim state set failed:", e, flush=True)


def warmclaim_mode():
    owner = os.environ.get("OWNER_HINT") or ""
    wid = os.environ.get("WARM_ID") or ""
    started = int(time.time())
    if not owner or not wid:
        print("WARMCLAIM missing OWNER_HINT/WARM_ID, exit", flush=True)
        return
    try:
        d1("CREATE TABLE IF NOT EXISTS gh_warm (id TEXT PRIMARY KEY, "
           "owner TEXT, state TEXT, started_at INTEGER, updated_at INTEGER)")
        # Đếm máy chờ KHÁC (trừ chính mình): Pages đã ghi dòng waiting
        # của chính máy này trước khi dispatch — vòng đo cap 25/11 cho
        # thấy 2 máy chờ đồng thời tự tính cả nhau nên 1 máy tự cap oan.
        rows = d1("SELECT COUNT(*) AS n FROM gh_warm WHERE state='waiting' "
                  "AND started_at>? AND id<>?", [started - 180, wid])["results"]
        active = int(rows[0]["n"]) if rows else 0
    except Exception as e:  # noqa: BLE001
        print("WARMCLAIM cap check failed, exit:", e, flush=True)
        return
    if active >= WARMCLAIM_MAX_ACTIVE:
        _warm_set(wid, owner, "capped", started)
        print(f"WARMCLAIM cap reached ({active} waiting), exit", flush=True)
        return
    _warm_set(wid, owner, "waiting", started)
    print(f"WARMCLAIM {wid} waiting for owner {owner} "
          f"window {WARMCLAIM_WINDOW_S}s", flush=True)
    skipped = set()
    deadline = started + WARMCLAIM_WINDOW_S
    # Lọc chỉ theo owner + queued, KHÔNG lọc created_at: vòng đo đầu
    # 25/11 (run 45586417385) máy chờ poll suốt 120s không thấy job dù
    # job nằm đúng cửa sổ — nghi đồng hồ runner lệch/lag đọc D1 làm
    # điều kiện thời gian giết mọi lượt khớp. Nhận cả job queued cũ
    # hơn của đúng chủ này vẫn an toàn: claim nguyên tử, run riêng của
    # job đó sẽ tự thấy "not claimable" và thoát.
    while int(time.time()) < deadline:
        try:
            rows = d1("SELECT id FROM gh_jobs WHERE owner=? AND "
                      "status='queued' ORDER BY "
                      "created_at LIMIT 5", [owner])["results"]
        except Exception as e:  # noqa: BLE001
            print("warmclaim poll failed:", e, flush=True)
            rows = []
        print(f"WARMCLAIM poll t+{int(time.time()) - started}s "
              f"rows={len(rows)}", flush=True)
        for r in rows:
            b = r["id"]
            if b in skipped:
                continue
            job, found = load_job(b)
            if not job:
                print(f"WARMCLAIM load_job miss {b}", flush=True)
                skipped.add(b)
                continue
            if batch_chars(job) >= SPLIT_MIN_CHARS:
                # Job dài: để run song song của chính nó xử lý.
                skipped.add(b)
                continue
            if not claim_batch(b, found):
                print(f"WARMCLAIM claim lost {b}", flush=True)
                skipped.add(b)
                continue
            print(f"WARMCLAIM claimed {b} for {owner} after "
                  f"{int(time.time()) - started}s", flush=True)
            try:
                process(b, claimed=True)
                _warm_set(wid, owner, "done", started)
            except Exception as e:  # noqa: BLE001
                print("WARMCLAIM JOB FAILED", b, e, flush=True)
                mark_error(b, e)
                _warm_set(wid, owner, "error", started)
            return
        time.sleep(WARMCLAIM_POLL_S)
    _warm_set(wid, owner, "expired", started)
    print(f"WARMCLAIM {wid} expired, no job in window", flush=True)


# ---------- BỂ ẤM (MODE=warm): worker sống dài theo ca relay ----------
# Mỗi slot là 1 run của workflow tutalab-warm.yml: nạp dist/model một
# lần rồi vòng lặp canh hàng đợi (cùng D1/KV + claim nguyên tử như mọi
# đường khác). Trước khi hết ca một khoảng RELAY_MARGIN, worker TỰ
# dispatch lượt thay thế cho đúng slot của mình (concurrency group theo
# slot ở workflow giữ cho hai ca không bao giờ chạy trùng). Nhịp tim ghi
# vào kho 0, khoá ghwarm:<slot>, tối thiểu mỗi WARM_HEARTBEAT_S giây.
# Tắt khẩn cấp: disable workflow tutalab-warm + cancel các run đang
# trực — hệ quay về đường dispatch/cron cũ ngay, không mất job nào.
WARM_POLL_S = 10
WARM_HEARTBEAT_S = 300


def warm_beat(slot, state, jobs_done, started):
    try:
        kv_put_at(0, f"ghwarm:{slot}", json.dumps({
            "slot": slot, "run": os.environ.get("GITHUB_RUN_ID") or "",
            "state": state, "jobs_done": jobs_done,
            "started_at": started, "beat_at": int(time.time())}))
    except Exception as e:  # noqa: BLE001
        print("heartbeat failed:", e, flush=True)


def relay_dispatch(slot, shift_s):
    token = os.environ.get("GH_TOKEN") or ""
    if not token:
        print("WARM relay: no GH_TOKEN, cannot dispatch replacement",
              flush=True)
        return False
    body = {"ref": "main", "inputs": {"slot": str(slot),
                                      "shift_seconds": str(shift_s)}}
    req = urllib.request.Request(
        "https://api.github.com/repos/fruitcusp/test-actions-codespaces"
        "/actions/workflows/tutalab-warm.yml/dispatches",
        data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"WARM relay: dispatched replacement slot {slot} "
                  f"(HTTP {r.status})", flush=True)
            return True
    except Exception as e:  # noqa: BLE001
        print("WARM relay dispatch failed:", e, flush=True)
        return False


def sweep_warm():
    now = int(time.time())
    rows = d1("SELECT id FROM gh_jobs WHERE status='queued' OR (status='running' AND updated_at<?) ORDER BY created_at LIMIT 3",
              [now - CLAIM_TTL_S])["results"]
    return [r["id"] for r in rows]


def warm_process(batch):
    job, found = load_job(batch)
    if not job:
        return False
    if not claim_batch(batch, found):
        print("warm: batch not claimable:", batch, flush=True)
        return False
    jobdir = os.path.join(WORK, batch)
    os.makedirs(jobdir, exist_ok=True)
    entries = synth_indices(batch, job, found, jobdir,
                            list(range(len(job.get("units") or []))))
    finalize_par(batch, job, found, jobdir, entries)
    return True


def warm_mode():
    slot = os.environ.get("SLOT") or "0"
    shift_s = int(os.environ.get("SHIFT_SECONDS") or "18000")
    started = int(time.time())
    deadline = started + shift_s
    margin = min(300, shift_s // 3)
    jobs_done = 0
    relayed = False
    next_relay_try = 0
    last_beat = 0
    print(f"WARM slot {slot} shift {shift_s}s margin {margin}s "
          f"run {os.environ.get('GITHUB_RUN_ID')}", flush=True)
    warm_beat(slot, "starting", jobs_done, started)
    last_beat = time.time()
    while time.time() < deadline:
        now = time.time()
        if not relayed and now >= deadline - margin and now >= next_relay_try:
            relayed = relay_dispatch(slot, shift_s)
            if not relayed:
                next_relay_try = now + 60
        if now - last_beat >= WARM_HEARTBEAT_S:
            warm_beat(slot, "idle", jobs_done, started)
            last_beat = now
        try:
            rows = sweep_warm()
        except Exception as e:  # noqa: BLE001
            print("warm sweep failed:", e, flush=True)
            rows = []
        if not rows:
            time.sleep(WARM_POLL_S)
            continue
        for b in rows:
            if time.time() >= deadline:
                break
            warm_beat(slot, "working:" + b, jobs_done, started)
            last_beat = time.time()
            try:
                if warm_process(b):
                    jobs_done += 1
            except Exception as e:  # noqa: BLE001
                print("WARM JOB FAILED", b, e, flush=True)
                mark_error(b, e)
        warm_beat(slot, "idle", jobs_done, started)
        last_beat = time.time()
    warm_beat(slot, "exiting", jobs_done, started)
    print(f"WARM slot {slot} shift done, jobs={jobs_done}", flush=True)


def sweep():
    # Đợt 2 sửa TuTaLab (25/11/2026): trước khi nhặt việc —
    # (1) chốt lỗi job 'running' treo quá CLAIM_TTL_S: worker chết/run
    # bị huỷ giữa chừng để lại job ma không ai đóng (đợt 1 còn 3 job ma
    # từ 11:03). Khớp luật Pages: running quá 900s là hết hạn nhận.
    # (2) dọn định kỳ hàng done/error quá 7 ngày để bảng không phình vô
    # hạn (payload/file ở KV vốn đã có TTL riêng).
    # Cả hai nuốt lỗi: dọn dẹp hỏng không được chặn việc nhặt job.
    now = int(time.time())
    try:
        r = d1("UPDATE gh_jobs SET status='error', error='Job treo quá 900 giây không có máy xử lý — hệ thống tự chốt lỗi.', updated_at=? WHERE status='running' AND updated_at<?",
               [now, now - CLAIM_TTL_S])
        n = (r.get("meta") or {}).get("changes", 0)
        if n:
            print(f"sweep: closed {n} ghost running jobs", flush=True)
    except Exception as e:  # noqa: BLE001
        print("sweep ghost-close failed:", e, flush=True)
    try:
        r = d1("DELETE FROM gh_jobs WHERE status IN ('done','error') AND updated_at<?",
               [now - 7 * 24 * 3600])
        n = (r.get("meta") or {}).get("changes", 0)
        if n:
            print(f"sweep: pruned {n} old done/error jobs (>7d)", flush=True)
    except Exception as e:  # noqa: BLE001
        print("sweep prune failed:", e, flush=True)
    rows = d1("SELECT id FROM gh_jobs WHERE status='queued' ORDER BY created_at LIMIT 3")["results"]
    return [r["id"] for r in rows]


def mark_error(batch, e):
    try:
        kv_put_pinned(batch, f"ghres:{batch}", json.dumps(
            {"status": "error", "jobs": [], "error": str(e)[:200]}))
        d1("UPDATE gh_jobs SET status='error', error=?, updated_at=? WHERE id=?",
           [str(e)[:200], int(time.time()), batch])
    except Exception:
        pass


def main():
    mode = (os.environ.get("MODE") or "").lower()
    if mode == "plan":
        plan_mode()
        return
    if mode == "planrun":
        planrun_mode()
        return
    if mode == "single":
        b = os.environ.get("JOB_ID") or ""
        try:
            process(b, claimed=True)
        except Exception as e:  # noqa: BLE001
            print("JOB FAILED", b, e, flush=True)
            mark_error(b, e)
        return
    if mode == "synth":
        # Lỗi ở 1 luồng: cứ để job này fail — khâu assemble (always())
        # sẽ thấy đoạn thiếu, chốt lỗi theo đúng kỷ luật cũ.
        synth_mode()
        return
    if mode == "assemble":
        b = os.environ.get("JOB_ID") or ""
        try:
            assemble_mode()
        except Exception as e:  # noqa: BLE001
            print("ASSEMBLE FAILED", b, e, flush=True)
            mark_error(b, e)
        return
    if mode == "warm":
        warm_mode()
        return
    if mode == "warmclaim":
        warmclaim_mode()
        return
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
            mark_error(b, e)


if __name__ == "__main__":
    main()
