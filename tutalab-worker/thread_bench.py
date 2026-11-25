#!/usr/bin/env python3
# Bench threads cho binary vieneu-rs (ONNX Runtime) trên máy Actions
# 4 vCPU: cùng 1 đoạn chuẩn (~500 ký tự, giọng Ngọc Huyền), chạy ở các
# mức OMP_NUM_THREADS khác nhau (unset/1/2/4), mỗi mức 3 lượt, đo thời
# gian tổng + MODEL_LOADED + RTF. Chạy qua workflow thread-bench.yml.
import json
import os
import re
import subprocess
import sys
import time

TEXT = ("Ngày xưa ở một làng quê yên bình bên dòng sông xanh, có một cô gái "
        "thợ dệt tên là Mai, nổi tiếng khắp vùng vì đôi tay khéo léo và tấm "
        "lòng nhân hậu. Mỗi sớm mai, cô đều ra bến sông gánh nước, nghe chim "
        "hót trên cành tre và mơ về một ngày được đi xa khỏi lũy tre làng để "
        "thấy biển rộng trời cao. Người làng ai cũng quý mến cô, chỉ có bà "
        "dì ghẻ là hay cau có, giao cho cô biết bao việc nặng nhọc từ tinh "
        "mơ đến tối mịt mới thôi. Thế nhưng Mai không hề oán trách, cô vẫn "
        "cặm cụi làm việc và giữ nụ cười hiền trên môi.")
VOICE = "Ngọc Huyền"
REPS = 3


def find_dist():
    dest = "/tmp/rustdist"
    for dp, _dn, fn in os.walk(dest):
        if "vieneu-rs" in fn:
            return dp
    url = os.environ.get("RUST_DIST_URL") or ""
    if not url:
        return None
    import urllib.request
    tar = "/tmp/vieneu-rust-dist.tar"
    if not os.path.exists(tar):
        urllib.request.urlretrieve(url, tar)
    os.makedirs(dest, exist_ok=True)
    subprocess.run(["tar", "-xf", tar, "-C", dest], check=True)
    for dp, _dn, fn in os.walk(dest):
        if "vieneu-rs" in fn:
            return dp
    return None


def ff_dur(path):
    r = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                        "format=duration", "-of", "csv=p=0", path],
                       capture_output=True, text=True)
    try:
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def one_run(dist, omp, rep):
    work = f"/tmp/tbwork-{omp}-{rep}"
    os.makedirs(work, exist_ok=True)
    upath = os.path.join(work, "units.json")
    with open(upath, "w", encoding="utf-8") as f:
        json.dump({"voice": VOICE,
                   "units": [{"id": "0", "text": TEXT}]},
                  f, ensure_ascii=False)
    loader = os.path.join(dist, "syslibs", "ld-linux-x86-64.so.2")
    deadline = str(int(time.time()) + 1200)
    cmd = [loader, "--library-path", os.path.join(dist, "syslibs"),
           os.path.join(dist, "vieneu-rs"),
           "--model-dir", os.path.join(dist, "model", "int8"),
           "--codec-dir", os.path.join(dist, "model", "codec"),
           "--model-root", os.path.join(dist, "model"),
           "--dict", os.path.join(dist, "assets", "sea_g2p.bin"),
           "--voices-file", os.path.join(dist, "assets", "voices_v3_turbo.json"),
           "worker", upath, "", "", work, deadline, "0"]
    env = dict(os.environ, LC_ALL="C.UTF-8", LANG="C.UTF-8")
    if omp is None:
        env.pop("OMP_NUM_THREADS", None)
    else:
        env["OMP_NUM_THREADS"] = str(omp)
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=dist, env=env)
    wall = time.time() - t0
    log = (r.stdout or "") + (r.stderr or "")
    m = re.search(r"MODEL_LOADED\s+([0-9.]+)s", log)
    load = float(m.group(1)) if m else None
    seg = os.path.join(work, "seg_0.wav")
    dur = ff_dur(seg) if os.path.exists(seg) else 0.0
    pure = wall - (load or 0)
    return {"omp": omp, "rep": rep, "wall_s": round(wall, 2),
            "load_s": load, "audio_s": round(dur, 2),
            "rtf_total": round(wall / dur, 3) if dur else None,
            "rtf_pure": round(pure / dur, 3) if dur else None,
            "rc": r.returncode}


def main():
    dist = find_dist()
    if not dist:
        print("TB_RESULT " + json.dumps({"error": "no dist"}))
        sys.exit(1)
    print("dist:", dist, "chars:", len(TEXT), flush=True)
    results = []
    for omp in (None, 1, 2, 4):
        for rep in range(REPS):
            try:
                r = one_run(dist, omp, rep)
            except Exception as e:  # noqa: BLE001
                r = {"omp": omp, "rep": rep, "error": str(e)[:200]}
            results.append(r)
            print("TB_RUN " + json.dumps(r, ensure_ascii=False), flush=True)
    med = {}
    for omp in (None, 1, 2, 4):
        vals = [r["rtf_pure"] for r in results
                if r.get("omp") == omp and r.get("rtf_pure")]
        walls = [r["wall_s"] for r in results
                 if r.get("omp") == omp and r.get("wall_s")]
        if vals:
            med[str(omp)] = {"median_rtf_pure": sorted(vals)[len(vals) // 2],
                             "median_wall_s": sorted(walls)[len(walls) // 2]}
    print("TB_RESULT " + json.dumps({"chars": len(TEXT), "medians": med,
                                     "runs": results}, ensure_ascii=False))


if __name__ == "__main__":
    main()
