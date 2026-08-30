"""Talk to the public archive at https://data.binance.vision.

The website is a static front-end over an S3 bucket.  The same bucket answers a
standard *ListBucket* XML request, so we never have to brute-force file names -
we ask it directly which monthly archives exist for a symbol.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

from .config import Config

LOG = logging.getLogger(__name__)

VISION_HOST = "https://data.binance.vision"
# data.binance.vision serves the browser UI (HTML); the underlying S3 bucket
# answers the ListBucket XML query used for discovery.
LIST_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
# trailing "-YYYY-MM.zip" / "-YYYY-MM-DD.zip" / "-YYYY-MM.csv" ...
_DATE_RE = re.compile(r"-(\d{4}-\d{2})(?:-\d{2})?\.(?:zip|csv)$")


def month_of(filename: str) -> str | None:
    """Extract the ``YYYY-MM`` stamp from an archive file name, or ``None``."""
    m = _DATE_RE.search(filename)
    return m.group(1) if m else None


# ── path / url construction ──────────────────────────────────────────────────
def archive_prefix(cfg: Config, symbol: str) -> str:
    """S3 key prefix for a symbol, e.g. ``data/spot/monthly/klines/BTCUSDT/4h/``."""
    parts = ["data", cfg.market_path, cfg.data_frequency, cfg.data_type, symbol]
    if cfg.is_kline:
        parts.append(cfg.interval)
    return "/".join(parts) + "/"


def local_dir(cfg: Config, symbol: str) -> Path:
    """Local archive dir mirroring the URL layout (symbol folder lower-cased)."""
    parts = [
        cfg.output_dir,
        cfg.market_path,
        cfg.data_frequency,
        cfg.data_type,
        symbol.lower(),
    ]
    if cfg.is_kline:
        parts.append(cfg.interval)
    return Path(*parts)


def download_url(key: str) -> str:
    return f"{VISION_HOST}/{key}"


# ── HTTP with light retry ────────────────────────────────────────────────────
def _get(session: requests.Session, url: str, *, retries: int = 3, **kw) -> requests.Response:
    kw.setdefault("timeout", 60)
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return session.get(url, **kw)
        except requests.RequestException as exc:  # transient network error
            last = exc
            LOG.debug("GET %s failed (%s/%s): %s", url, attempt, retries, exc)
            time.sleep(min(2 ** attempt, 10))
    raise last  # type: ignore[misc]


# ── listing ─────────────────────────────────────────────────────────────────
def list_keys(session: requests.Session, prefix: str) -> list[str]:
    """Every object key under ``prefix`` (handles pagination)."""
    keys: list[str] = []
    marker = ""
    while True:
        params = {"delimiter": "/", "prefix": prefix}
        if marker:
            params["marker"] = marker
        resp = _get(session, LIST_ENDPOINT, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        page = [
            c.findtext(f"{_S3_NS}Key")
            for c in root.findall(f"{_S3_NS}Contents")
        ]
        keys.extend(k for k in page if k)
        truncated = (root.findtext(f"{_S3_NS}IsTruncated") or "false").lower() == "true"
        if not truncated:
            break
        marker = root.findtext(f"{_S3_NS}NextMarker") or (page[-1] if page else "")
        if not marker:
            break
    return keys


@dataclass(frozen=True)
class ArchiveFile:
    key: str          # full S3 key
    month: str        # "YYYY-MM"

    @property
    def name(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def csv_name(self) -> str:
        return self.name[:-4] + ".csv"  # strip ".zip"


def available_files(session: requests.Session, cfg: Config, symbol: str) -> list[ArchiveFile]:
    """Monthly (or daily) zip archives that actually exist for ``symbol``, filtered
    by the configured ``start_month`` / ``end_month`` range."""
    out: list[ArchiveFile] = []
    for key in list_keys(session, archive_prefix(cfg, symbol)):
        if not key.endswith(".zip"):
            continue  # skip the .CHECKSUM sidecars
        m = _DATE_RE.search(key)
        if not m:
            continue
        month = m.group(1)
        if cfg.start_month and month < cfg.start_month:
            continue
        if cfg.end_month and month > cfg.end_month:
            continue
        out.append(ArchiveFile(key=key, month=month))
    out.sort(key=lambda f: f.key)
    return out


# ── download / verify / extract ─────────────────────────────────────────────
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fetch_expected_sha(session: requests.Session, url: str) -> str | None:
    resp = _get(session, url + ".CHECKSUM")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.text.split()[0].lower()


def _safe_extract(zip_path: Path, dest_dir: Path) -> list[str]:
    dest_root = dest_dir.resolve()
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            target = (dest_dir / member).resolve()
            if dest_root != target and dest_root not in target.parents:
                raise RuntimeError(f"unsafe path in zip {zip_path.name}: {member!r}")
        zf.extractall(dest_dir)
        return zf.namelist()


def download_file(
    session: requests.Session,
    cfg: Config,
    af: ArchiveFile,
    dest_dir: Path,
) -> str:
    """Fetch one archive file. Returns a short status string.

    One of: ``downloaded`` | ``skipped`` | ``missing`` | ``checksum_failed``.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    csv_path = dest_dir / af.csv_name
    if cfg.skip_existing and csv_path.exists():
        return "skipped"

    url = download_url(af.key)
    zip_path = dest_dir / af.name
    tmp_path = zip_path.with_suffix(zip_path.suffix + ".part")

    with _get(session, url, stream=True) as resp:
        if resp.status_code == 404:
            LOG.warning("no archive for %s (%s) - delisted or not yet published", af.name, af.month)
            return "missing"
        resp.raise_for_status()
        with tmp_path.open("wb") as fh:
            for chunk in resp.iter_content(1 << 16):
                fh.write(chunk)
    tmp_path.replace(zip_path)

    if cfg.verify_checksum:
        expected = _fetch_expected_sha(session, url)
        if expected is None:
            msg = f"{af.name}: no .CHECKSUM published"
            if cfg.strict_checksum:
                zip_path.unlink(missing_ok=True)
                raise RuntimeError(msg)
            LOG.warning("%s - skipping verification", msg)
        else:
            actual = _sha256(zip_path).lower()
            if actual != expected:
                zip_path.unlink(missing_ok=True)
                LOG.error("%s: sha256 mismatch (want %s got %s)", af.name, expected, actual)
                return "checksum_failed"

    _safe_extract(zip_path, dest_dir)
    if not cfg.keep_zip:
        zip_path.unlink(missing_ok=True)
    return "downloaded"
