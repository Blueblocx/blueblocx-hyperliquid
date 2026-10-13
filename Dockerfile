# HyperCore pipeline image. Batch job, no server process.
#
#   docker build -t blueblocx-hyperliquid .
#   docker run --rm --env-file .env -v hypercore-data:/data blueblocx-hyperliquid blueblocx_hyperliquid.preflight --wallets-file /data/wallets.txt
#   docker run --rm --env-file .env -v hypercore-data:/data blueblocx-hyperliquid            # = blueblocx_hyperliquid.daily (see CMD)
#
# /data holds everything that must survive between runs: wallets.txt, the staged
# CSVs, per-wallet watermarks, run summaries and (dev only) the local Iceberg
# catalog. Mount a volume there.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HL_OUT_DIR=/data/out \
    HL_WALLETS_FILE=/data/wallets.txt \
    HL_LOG_JSON=1

# an empty pins file so preflight runs (WARN, not FAIL) before a volume provides one
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin app \
 && mkdir -p /data/out && touch /data/wallets.txt && chown -R app:app /data

WORKDIR /app
COPY requirements.txt constraints.txt ./
RUN pip install -r requirements.txt -c constraints.txt

# the loader derives table shapes from the DDL at runtime, so it ships with the code
COPY blueblocx_hyperliquid ./blueblocx_hyperliquid
COPY docs/PROPOSED_SCHEMA.sql ./docs/PROPOSED_SCHEMA.sql
COPY tests ./tests

USER app
VOLUME ["/data"]

ENTRYPOINT ["python", "-m"]
CMD ["blueblocx_hyperliquid.daily", "--iceberg", "--s3-backfill"]
