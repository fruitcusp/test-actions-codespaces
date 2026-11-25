# TuTaLab worker image — image hoá 25/11/2026 (mẫu ngủ đông: job bật
# lên là chạy ngay, không cài đặt gì thêm).
# Chứa sẵn: ffmpeg + git và gói Rust dist ĐÃ GIẢI NÉN ở /opt/rustdist
# (worker tìm ở /tmp/rustdist -> symlink). KHÔNG còn VieNeu Python
# (gỡ sạch 26/11/2026 theo lệnh Sếp): mọi engine đều là vieneu-rs.
# Dựng bởi workflow build-worker-image.yml — CHỈ dispatch tay.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/opt/hf \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends \
      ffmpeg git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Gói Rust dist: tải tar từ Release của chính repo này, giải nén sẵn và
# XOÁ tar ngay trong cùng 1 lớp (tránh image mang dist 2 lần).
# Đối chiếu sha256 của binary với bản đã kiểm chứng (BIN_SHA pha cache).
RUN curl -fsSL -o /tmp/vieneu-rust-dist.tar \
      https://github.com/fruitcusp/test-actions-codespaces/releases/download/vieneu-rust-dist/vieneu-rust-dist.tar \
 && mkdir -p /opt/rustdist \
 && tar -xf /tmp/vieneu-rust-dist.tar -C /opt/rustdist \
 && rm -f /tmp/vieneu-rust-dist.tar \
 && BIN=$(find /opt/rustdist -name vieneu-rs -type f | head -1) \
 && echo "vieneu-rs at $BIN" \
 && echo "551cc7703c78ec27498c9157fd06eb1d087cb91e05fc5af81e1366ef2f144a43  $BIN" | sha256sum -c - \
 && ln -s /opt/rustdist /tmp/rustdist

# 3 bản nhạc nền (đợt 2 sửa TuTaLab 25/11/2026): worker trộn sẵn biến
# thể MP3 kèm nhạc khi chốt batch — xem mix_music_variants trong
# worker.py. File nằm trong thư mục music/ cạnh Dockerfile này.
COPY music/ /opt/music/

WORKDIR /work
