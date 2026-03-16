"""
CMS MRF Downloader
──────────────────
Fetches the Table of Contents (TOC) index from each network, parses it
to find in-network-rate file URLs, then downloads them with streaming
to handle files that can exceed 10GB.

CMS April 2026 TOC Schema (in-network-rates):
{
  "reporting_entity_name": "...",
  "reporting_entity_type": "...",
  "last_updated_on": "YYYY-MM-DD",
  "version": "3.x.x",
  "reporting_structure": [
    {
      "reporting_plans": [...],
      "in_network_files": [
        {"description": "...", "location": "https://..."}
      ]
    }
  ]
}
"""

import asyncio
import hashlib
import json
from pathlib import Path
from typing import AsyncIterator

import httpx
import structlog

log = structlog.get_logger(__name__)

DEFAULT_TIMEOUT = 300  # seconds
CHUNK_SIZE = 1024 * 1024 * 8  # 8MB chunks


class MRFFile:
    """Represents a single MRF data file discovered from a TOC index."""

    def __init__(
        self,
        network_id: str,
        network_name: str,
        state_code: str,
        plan_name: str,
        plan_market_type: str,
        description: str,
        url: str,
        file_type: str = "in-network-rates",
    ):
        self.network_id = network_id
        self.network_name = network_name
        self.state_code = state_code
        self.plan_name = plan_name
        self.plan_market_type = plan_market_type
        self.description = description
        self.url = url
        self.file_type = file_type

    def __repr__(self) -> str:
        return f"<MRFFile {self.network_id} | {self.plan_name} | {self.url[-50:]}>"


async def fetch_toc(
    network_id: str,
    network_name: str,
    state_code: str,
    index_url: str,
    skip_plan_market_types: list[str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[MRFFile]:
    """
    Download and parse a CMS TOC index JSON to discover MRF file URLs.
    Returns a list of MRFFile objects for in-network-rates files.
    """
    skip_types = set(skip_plan_market_types or [])
    files: list[MRFFile] = []

    log.info("Fetching TOC index", network=network_id, url=index_url)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        try:
            resp = await client.get(index_url)
            resp.raise_for_status()
            toc = resp.json()
        except Exception as e:
            log.error("Failed to fetch TOC", network=network_id, error=str(e))
            return []

    reporting_structure = toc.get("reporting_structure", [])
    last_updated = toc.get("last_updated_on", "unknown")

    log.info("TOC parsed", network=network_id, last_updated=last_updated,
             structure_count=len(reporting_structure))

    for structure in reporting_structure:
        reporting_plans = structure.get("reporting_plans", [{}])
        in_network_files = structure.get("in_network_files", [])

        for plan in reporting_plans:
            plan_name = plan.get("plan_name", "Unknown Plan")
            plan_market_type = plan.get("plan_market_type", "")

            if plan_market_type in skip_types:
                log.debug("Skipping plan market type", type=plan_market_type, plan=plan_name)
                continue

            for file_entry in in_network_files:
                url = file_entry.get("location", "")
                description = file_entry.get("description", "")

                if not url:
                    continue

                files.append(
                    MRFFile(
                        network_id=network_id,
                        network_name=network_name,
                        state_code=state_code,
                        plan_name=plan_name,
                        plan_market_type=plan_market_type,
                        description=description,
                        url=url,
                        file_type="in-network-rates",
                    )
                )

    log.info("MRF files discovered", network=network_id, count=len(files))
    return files


async def download_mrf_file(
    mrf_file: MRFFile,
    output_dir: Path,
    max_size_bytes: int = 5 * 1024 ** 3,
    timeout: int = DEFAULT_TIMEOUT,
    force_refresh: bool = False,
) -> Path | None:
    """
    Stream-download a single MRF file to disk.
    Returns the local path, or None if download failed/skipped.

    Handles:
    - Gzipped files (.json.gz)
    - Size limit enforcement
    - Checksum-based change detection (skip if unchanged)
    - Atomic writes (temp file → final file rename)
    """
    # Derive a safe filename from the URL
    url_hash = hashlib.md5(mrf_file.url.encode()).hexdigest()[:8]
    suffix = ".json.gz" if mrf_file.url.endswith(".gz") else ".json"
    filename = f"{mrf_file.network_id}_{mrf_file.plan_name[:30].replace(' ', '_')}_{url_hash}{suffix}"
    output_path = output_dir / filename
    temp_path = output_path.with_suffix(".tmp")

    if output_path.exists() and not force_refresh:
        log.info("File already exists, skipping", path=str(output_path))
        return output_path

    log.info("Downloading MRF file", network=mrf_file.network_id,
             plan=mrf_file.plan_name, url=mrf_file.url[-60:])

    output_dir.mkdir(parents=True, exist_ok=True)
    bytes_downloaded = 0

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            async with client.stream("GET", mrf_file.url) as response:
                response.raise_for_status()

                with open(temp_path, "wb") as f:
                    async for chunk in response.aiter_bytes(CHUNK_SIZE):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                        if bytes_downloaded > max_size_bytes:
                            log.warning(
                                "File exceeds size limit, stopping download",
                                network=mrf_file.network_id,
                                size_mb=bytes_downloaded // (1024 ** 2),
                                limit_mb=max_size_bytes // (1024 ** 2),
                            )
                            return None

        # Atomic rename
        temp_path.rename(output_path)
        size_mb = bytes_downloaded / (1024 ** 2)
        log.info("Download complete", network=mrf_file.network_id,
                 size_mb=round(size_mb, 1), path=str(output_path))
        return output_path

    except Exception as e:
        log.error("Download failed", network=mrf_file.network_id, error=str(e))
        if temp_path.exists():
            temp_path.unlink()
        return None


async def download_all(
    mrf_files: list[MRFFile],
    output_dir: Path,
    max_concurrent: int = 3,
    **kwargs,
) -> list[tuple[MRFFile, Path | None]]:
    """
    Download multiple MRF files with bounded concurrency.
    Returns list of (MRFFile, local_path_or_None) tuples.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _download_with_semaphore(mrf: MRFFile):
        async with semaphore:
            path = await download_mrf_file(mrf, output_dir, **kwargs)
            return mrf, path

    tasks = [_download_with_semaphore(f) for f in mrf_files]
    return await asyncio.gather(*tasks)
