#!/usr/bin/env python3
# Đo đối đầu VieNeu: pip vieneu 3.8.5 (Python) vs vieneu-rs (Rust, dist của
# nhà lấy từ Node 2 ngày 15/11/2026) — chạy trên GitHub Actions.
# Lệnh Sếp 25/11: bộ câu ngắn (99 + ~1.000 ký tự) + 1 job truyện thật
# (~8.000 ký tự, chia tối đa 20 đoạn theo mẫu story-pipeline, matrix song
# song), giọng Ngọc Huyền, loudnorm -14 khi ghép. Engine thắng ở job truyện
# = engine mặc định. Kèm: kiểm kê model mỗi engine nạp + mẫu nghe thử cùng
# câu cùng giọng cho mỗi engine (sample-<engine>.wav).
import json
import os
import re
import subprocess
import sys
import time

VOICE = "Ngọc Huyền"
ROOT = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(ROOT, "bench")


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


def split_story(text, max_parts=20, target=500):
    """Chia theo câu, gộp tới ~target ký tự/phần, tối đa max_parts phần
    (đúng mẫu prepare của story-pipeline)."""
    sents = [s.strip() for s in re.findall(r"[^.!?…]+[.!?…]*\s*|\S[^.!?…]*$", text.replace("\n", " ")) if s.strip()]
    parts, cur = [], ""
    for s in sents:
        if cur and len(cur) + len(s) > target:
            parts.append(cur)
            cur = s
        else:
            cur = (cur + " " + s).strip()
    if cur:
        parts.append(cur)
    while len(parts) > max_parts:
        # gộp 2 phần ngắn nhất kề nhau
        i = min(range(len(parts) - 1), key=lambda k: len(parts[k]) + len(parts[k + 1]))
        parts[i:i + 2] = [parts[i] + " " + parts[i + 1]]
    return parts


def cmd_split():
    src = sys.argv[2] if len(sys.argv) > 2 else os.path.join(BENCH, "story.txt")
    outdir = sys.argv[3] if len(sys.argv) > 3 else os.path.join(ROOT, "bench-out", "segments")
    os.makedirs(outdir, exist_ok=True)
    text = open(src, encoding="utf-8").read()
    parts = split_story(text)
    for i, p in enumerate(parts):
        with open(os.path.join(outdir, f"seg_{i:02d}.txt"), "w", encoding="utf-8") as f:
            f.write(p)
    man = {"source": os.path.basename(src), "chars": len(text), "parts": len(parts),
           "part_chars": [len(p) for p in parts]}
    with open(os.path.join(outdir, "manifest.json"), "w") as f:
        json.dump(man, f, ensure_ascii=False, indent=1)
    print(json.dumps(man, ensure_ascii=False))


_ENG = None


def py_engine():
    global _ENG
    if _ENG is None:
        from vieneu import Vieneu
        t0 = time.time()
        _ENG = Vieneu()
        _ENG._load_seconds = time.time() - t0
    return _ENG


def synth_py(text, out):
    eng = py_engine()
    t0 = time.time()
    audio = eng.infer(text, voice=VOICE)
    dt = time.time() - t0
    eng.save(audio, out)
    return {"synth_s": round(dt, 3), "load_s": round(getattr(eng, "_load_seconds", 0), 3),
            "audio_s": round(ff_dur(out), 3), "chars": len(text)}


def synth_rs(text, out, dist, idx="1"):
    work = os.path.join(ROOT, "bench-out", "rs-work")
    os.makedirs(work, exist_ok=True)
    units = os.path.join(work, f"units_{idx}.json")
    with open(units, "w", encoding="utf-8") as f:
        json.dump({"voice": VOICE, "units": [{"id": str(idx), "text": text}]}, f, ensure_ascii=False)
    loader = os.path.join(dist, "syslibs", "ld-linux-x86-64.so.2")
    # Tham số hạn chót của worker là EPOCH TUYỆT ĐỐI (kernel Kaggle truyền
    # start+10200), KHÔNG phải số giây — truyền "1800" là hạn chót năm 1970
    # -> worker nạp model xong dừng ngay, DEADLINE_STOP new=0 (bắt tại
    # bench run 45461002333, sửa + kiểm chứng tại máy 25/11).
    deadline = str(int(time.time()) + 1800)
    cmd = [loader, "--library-path", os.path.join(dist, "syslibs"),
           os.path.join(dist, "vieneu-rs"),
           "--model-dir", os.path.join(dist, "model", "int8"),
           "--codec-dir", os.path.join(dist, "model", "codec"),
           "--model-root", os.path.join(dist, "model"),
           "--dict", os.path.join(dist, "assets", "sea_g2p.bin"),
           "--voices-file", os.path.join(dist, "assets", "voices_v3_turbo.json"),
           "worker", units, "", "", work, deadline, "0"]
    env = dict(os.environ, OMP_NUM_THREADS="2", LC_ALL="C.UTF-8", LANG="C.UTF-8")
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=dist, env=env)
    dt = time.time() - t0
    log = (r.stdout or "") + (r.stderr or "")
    m = re.search(r"MODEL_LOADED\s+([0-9.]+)s", log)
    seg = os.path.join(work, f"seg_{idx}.wav")
    if not os.path.isfile(seg):
        raise RuntimeError("rust produced no seg: " + log[-500:])
    subprocess.run(["cp", seg, out], check=True)
    return {"synth_s": round(dt, 3), "load_s": float(m.group(1)) if m else None,
            "audio_s": round(ff_dur(out), 3), "chars": len(text)}


def cmd_one():
    engine, segfile, out = sys.argv[2], sys.argv[3], sys.argv[4]
    text = open(segfile, encoding="utf-8").read()
    if engine == "python":
        info = synth_py(text, out)
    else:
        info = synth_rs(text, out, os.environ["RUST_DIST"], idx=os.path.basename(segfile)[4:6])
    info["engine"] = engine
    info["rtf"] = round(info["synth_s"] / info["audio_s"], 3) if info["audio_s"] else None
    with open(out + ".json", "w") as f:
        json.dump(info, f, indent=1)
    print(json.dumps(info, ensure_ascii=False))


def cmd_short():
    engine = sys.argv[2]
    raw = open(os.path.join(BENCH, "short.txt"), encoding="utf-8").read()
    parts = [p.strip() for p in raw.split("\n\n") if p.strip()]
    outdir = os.path.join(ROOT, "bench-out")
    os.makedirs(outdir, exist_ok=True)
    infos = []
    for i, p in enumerate(parts):
        out = os.path.join(outdir, f"short-{engine}-{i}.wav")
        if engine == "python":
            info = synth_py(p, out)
        else:
            info = synth_rs(p, out, os.environ["RUST_DIST"], idx=str(90 + i))
        info["engine"] = engine
        info["rtf"] = round(info["synth_s"] / info["audio_s"], 3) if info["audio_s"] else None
        infos.append(info)
        if i == 0:
            subprocess.run(["cp", out, os.path.join(outdir, f"sample-{engine}.wav")], check=True)
    with open(os.path.join(outdir, f"short-{engine}.json"), "w") as f:
        json.dump(infos, f, indent=1)
    print(json.dumps(infos, ensure_ascii=False))


def cmd_compose():
    engine = sys.argv[2]
    segdir = os.path.join(ROOT, "bench-out", "segments")
    outdir = os.path.join(ROOT, "bench-out")
    wavs, total_synth, total_audio = [], 0.0, 0.0
    for name in sorted(os.listdir(segdir)):
        if not name.endswith(".wav"):
            continue
        wavs.append(os.path.join(segdir, name))
        tj = name + ".json"
        if os.path.exists(os.path.join(segdir, tj)):
            d = json.load(open(os.path.join(segdir, tj)))
            total_synth += d.get("synth_s", 0)
            total_audio += d.get("audio_s", 0)
    lst = os.path.join(outdir, f"list-{engine}.txt")
    with open(lst, "w") as f:
        for w in wavs:
            f.write(f"file '{w}'\n")
    joined = os.path.join(outdir, f"story-{engine}-raw.wav")
    run(["ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", lst,
         "-ac", "1", "-ar", "48000", joined])
    final = os.path.join(outdir, f"story-{engine}.mp3")
    r = run(["ffmpeg", "-y", "-v", "error", "-i", joined, "-af",
             "loudnorm=I=-14:TP=-1:LRA=11", "-codec:a", "libmp3lame", "-b:a", "128k", final])
    if r.returncode != 0:
        raise RuntimeError("compose failed: " + r.stderr[-300:])
    dur = ff_dur(final)
    vd = run(["ffmpeg", "-i", final, "-af", "volumedetect", "-f", "null", "-"])
    m = re.search(r"mean_volume:\s*(-?[0-9.]+) dB", vd.stderr or "")
    res = {"engine": engine, "segments": len(wavs),
           "synth_total_s": round(total_synth, 2),
           "audio_total_s": round(total_audio, 2),
           "rtf_avg": round(total_synth / total_audio, 3) if total_audio else None,
           "final_duration_s": round(dur, 2),
           "mean_volume_db": float(m.group(1)) if m else None,
           "mp3_bytes": os.path.getsize(final)}
    with open(os.path.join(outdir, f"result-{engine}.json"), "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res, ensure_ascii=False))


def cmd_inventory():
    engine = sys.argv[2]
    outdir = os.path.join(ROOT, "bench-out")
    os.makedirs(outdir, exist_ok=True)
    files = []
    if engine == "python":
        # Nạp engine để nó tải model thật, rồi kiểm kê cache HuggingFace.
        py_engine()
        base = os.path.expanduser("~/.cache/huggingface")
        for dp, _dn, fn in os.walk(base):
            for x in fn:
                p = os.path.join(dp, x)
                if os.path.islink(p) or x.endswith((".json", ".txt", ".lock")) and "vieneu" not in p.lower():
                    continue
                files.append({"path": p.replace(base, "hf"), "bytes": os.path.getsize(p)})
    else:
        dist = os.environ["RUST_DIST"]
        for sub in ("model", "assets"):
            for dp, _dn, fn in os.walk(os.path.join(dist, sub)):
                for x in fn:
                    p = os.path.join(dp, x)
                    files.append({"path": p.replace(dist, "dist"), "bytes": os.path.getsize(p)})
    files.sort(key=lambda d: -d["bytes"])
    inv = {"engine": engine, "files": files[:40],
           "total_bytes": sum(d["bytes"] for d in files)}
    with open(os.path.join(outdir, f"inventory-{engine}.json"), "w") as f:
        json.dump(inv, f, indent=1)
    print(json.dumps({"engine": engine, "total_bytes": inv["total_bytes"],
                      "top": files[:8]}, ensure_ascii=False))


def cmd_fullmux():
    """Ghép bản full: tiếng thắng đo + nhạc nền dưới tiếng + video gốc ->
    master nguyên chất lượng + bản Telegram 720x1280 dưới 50MB."""
    voice_wav, video_src, music, out_prefix = sys.argv[2:6]
    master = out_prefix + "-master.mp4"
    tele = out_prefix + "-telegram.mp4"
    # Master: giữ nguyên hình gốc (stream copy khi độ dài khớp tiếng; nếu
    # tiếng dài hơn video thì lặp video), thay âm thanh bằng tiếng+nhạc.
    r = run(["ffmpeg", "-y", "-v", "error", "-stream_loop", "-1", "-i", video_src,
             "-i", voice_wav, "-i", music,
             "-filter_complex",
             "[2:a]volume=0.18[m];[1:a][m]amix=inputs=2:duration=first:dropout_transition=0[a]",
             "-map", "0:v", "-map", "[a]", "-c:v", "copy", "-shortest", master])
    if r.returncode != 0:
        raise RuntimeError("master mux failed: " + r.stderr[-300:])
    # Bản Telegram: trần cứng 50MB của Telegram — CRF tự do từng cho ra
    # 195MB với video 17,5 phút (run 45463976361) nên phải nén theo NGÂN
    # SÁCH BITRATE tính từ độ dài, mã hoá xong còn vòng kiểm tra lại.
    dur = ff_dur(master)
    for attempt in range(3):
        budget = int(49_000_000 * 8 / max(dur, 1) * (0.94 ** attempt))
        vbps = max(150_000, budget - 96_000)
        r = run(["ffmpeg", "-y", "-v", "error", "-i", master,
                 "-vf", "scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2",
                 "-c:v", "libx264", "-preset", "veryfast", "-b:v", str(vbps),
                 "-maxrate", str(vbps), "-bufsize", str(vbps * 2),
                 "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", tele])
        if r.returncode != 0:
            raise RuntimeError("telegram mux failed: " + r.stderr[-300:])
        if os.path.getsize(tele) <= 50_000_000:
            break
    else:
        raise RuntimeError("telegram encode van vuot 50MB sau 3 lan nen")
    info = {"master_bytes": os.path.getsize(master), "telegram_bytes": os.path.getsize(tele),
            "duration_s": round(ff_dur(tele), 2)}
    print(json.dumps(info, ensure_ascii=False))
    with open(out_prefix + "-info.json", "w") as f:
        json.dump(info, f, indent=1)


if __name__ == "__main__":
    cmd = sys.argv[1]
    {"split": cmd_split, "one": cmd_one, "short": cmd_short,
     "compose": cmd_compose, "inventory": cmd_inventory,
     "fullmux": cmd_fullmux}[cmd]()
