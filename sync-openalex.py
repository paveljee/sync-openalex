#!/usr/bin/env python3

# originally generated using ChatGPT 5.4 Extended thinking
# via chatgpt.com on 2026-03-30; prompt lost
# 0.1.0 - 2026-03-31: 165f5372ff3c451aabc43033d57a73178dc6393e
# 0.2.0 - 2026-04-05: we own partitions, freeze them on full download
# 0.2.1 - 2026-04-05: trust aws sync on checksum, we only do a quick check

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Sequence


DEFAULT_REMOTE_ROOT: Final[str] = "s3://openalex/data"
DEFAULT_LOCAL_ROOT: Final[Path] = Path("./openalex/data")
DEFAULT_PROGRESS_PATH: Final[Path] = Path("./sync_progress.json")
DEFAULT_AWS_BIN: Final[str] = shutil.which("aws") or "aws"
PARTITION_TYPE: Final[str] = "updated_date"
PARTITION_PREFIX: Final[str] = f"{PARTITION_TYPE}="


class ScriptError(Exception):
    """Raised for expected script failures."""


@dataclass(frozen=True, slots=True)
class Entity:
    name: str
    key: str


ENTITIES: Final[tuple[Entity, ...]] = (
    Entity(name="Works", key="works"),
    Entity(name="Authors", key="authors"),
    Entity(name="Sources", key="sources"),
    Entity(name="Institutions", key="institutions"),
    Entity(name="Topics", key="topics"),
    Entity(name="Domains", key="domains"),
    Entity(name="Fields", key="fields"),
    Entity(name="Subfields", key="subfields"),
    Entity(name="Publishers", key="publishers"),
    Entity(name="Funders", key="funders"),
    Entity(name="Concepts", key="concepts"),
)
ENTITY_KEYS: Final[frozenset[str]] = frozenset(entity.key for entity in ENTITIES)


@dataclass(frozen=True, slots=True)
class Config:
    remote_root: str
    local_root: Path
    progress_path: Path
    aws_bin: str


@dataclass(frozen=True, slots=True)
class ManifestCheckResult:
    entity: Entity
    local_manifest: Path
    local_sha256: str | None
    remote_sha256: str
    downloaded_manifest: bool


def parse_args(argv: Sequence[str]) -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Validate local OpenAlex manifests and then sync OpenAlex "
            "partitions one at a time."
        )
    )
    parser.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
        help="Remote OpenAlex S3 root.",
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=DEFAULT_LOCAL_ROOT,
        help="Local root directory mirroring the remote root.",
    )
    parser.add_argument(
        "--progress-path",
        type=Path,
        default=DEFAULT_PROGRESS_PATH,
        help="Path to the sync progress JSON file.",
    )
    parser.add_argument(
        "--aws-bin",
        default=DEFAULT_AWS_BIN,
        help="Path to the aws CLI binary.",
    )
    namespace = parser.parse_args(list(argv))
    return Config(
        remote_root=str(namespace.remote_root).rstrip("/"),
        local_root=Path(namespace.local_root),
        progress_path=Path(namespace.progress_path),
        aws_bin=str(namespace.aws_bin),
    )


def main(argv: Sequence[str]) -> int:
    config = parse_args(argv)

    print_status("Verifying manifests.")
    results = verify_or_initialize_manifests(config)
    print_manifest_summary(results)

    if config.progress_path.exists():
        print_status(f"Loading progress file: {config.progress_path}")
    else:
        print_status(
            f"Initializing new progress file: {config.progress_path}"
        )
    progress = load_progress(config.progress_path)
    download_status = build_download_status(
        config=config,
        existing_download_status=progress.get("download_status", {}),
    )
    progress["download_status"] = download_status
    save_progress(config.progress_path, progress)
    print_status(f"Saved progress snapshot: {config.progress_path}")

    print_status("Checking for pending partitions to sync.")
    sync_pending_partitions(
        config=config,
        progress=progress,
    )

    print("All partition syncs completed.")
    return 0


def verify_or_initialize_manifests(config: Config) -> list[ManifestCheckResult]:
    results: list[ManifestCheckResult] = []

    for entity in ENTITIES:
        local_dir = config.local_root / entity.key
        local_manifest = local_dir / "manifest"
        remote_manifest_uri = remote_manifest_uri_for(config, entity)

        if local_manifest.is_file():
            remote_sha256 = download_and_hash_remote_manifest(
                config=config,
                remote_manifest_uri=remote_manifest_uri,
            )
            local_sha256 = sha256_file(local_manifest)
            if local_sha256 != remote_sha256:
                raise ScriptError(
                    "Manifest checksum mismatch for "
                    f"{entity.name} ({entity.key}).\n"
                    f"Local manifest:  {local_manifest}\n"
                    f"Local sha256:    {local_sha256}\n"
                    f"Remote sha256:   {remote_sha256}"
                )

            results.append(
                ManifestCheckResult(
                    entity=entity,
                    local_manifest=local_manifest,
                    local_sha256=local_sha256,
                    remote_sha256=remote_sha256,
                    downloaded_manifest=False,
                )
            )
            continue

        if not is_directory_empty(local_dir):
            raise ScriptError(
                "Manifest does not exist for non-empty directory "
                f"{local_dir}. Expected file: {local_manifest}"
            )

        local_dir.mkdir(parents=True, exist_ok=True)
        download_s3_object(
            config=config,
            s3_uri=remote_manifest_uri,
            destination=local_manifest,
        )
        remote_sha256 = sha256_file(local_manifest)
        print(
            f"Downloaded missing manifest for {entity.name} to {local_manifest}",
            file=sys.stderr,
        )
        results.append(
            ManifestCheckResult(
                entity=entity,
                local_manifest=local_manifest,
                local_sha256=None,
                remote_sha256=remote_sha256,
                downloaded_manifest=True,
            )
        )

    return results


def print_manifest_summary(results: Sequence[ManifestCheckResult]) -> None:
    for result in results:
        if result.downloaded_manifest:
            print(
                f"{result.entity.name}: initialized manifest "
                f"({result.remote_sha256})"
            )
        else:
            print(
                f"{result.entity.name}: manifest verified "
                f"({result.remote_sha256})"
            )


def print_status(message: str) -> None:
    print(message, file=sys.stderr)


def load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"download_status": {}}
    if not path.is_file():
        raise ScriptError(f"Expected progress file path, found non-file: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ScriptError(f"Failed to read progress file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ScriptError(
            f"Failed to parse progress file as JSON: {path}\n{exc}"
        ) from exc

    if not isinstance(data, dict):
        raise ScriptError(f"Progress file must contain a JSON object: {path}")

    download_status = data.get("download_status")
    if download_status is None:
        data["download_status"] = {}
        return data
    if not isinstance(download_status, dict):
        raise ScriptError(
            "Progress file key 'download_status' must contain a JSON object."
        )

    return data


def save_progress(path: Path, progress: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(progress, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        raise ScriptError(f"Failed to write progress file: {path}") from exc


def build_download_status(
    *,
    config: Config,
    existing_download_status: Any,
) -> dict[str, Any]:
    if existing_download_status is None:
        existing_download_status = {}
    if not isinstance(existing_download_status, dict):
        raise ScriptError(
            "Progress file key 'download_status' must contain a JSON object."
        )

    print_status(f"Scanning local inventory under {config.local_root}")
    local_inventory, local_last_fetched = scan_local_inventory(config)
    print_status(
        "Finished local inventory scan: "
        f"{count_partitions(local_inventory)} partitions"
    )
    print_status(f"Scanning remote inventory under {config.remote_root}")
    remote_inventory, remote_last_fetched = scan_remote_inventory(config)
    print_status(
        "Finished remote inventory scan: "
        f"{count_partitions(remote_inventory)} partitions"
    )
    local_last_calculated = iso_timestamp_now()
    remote_last_calculated = iso_timestamp_now()

    download_status: dict[str, Any] = {}
    for entity in ENTITIES:
        existing_entity = existing_download_status.get(entity.key, {})
        download_status[entity.key] = {
            "local": build_local_entity_status(
                existing_entity=existing_entity,
                local_partitions=local_inventory[entity.key],
                remote_partitions=remote_inventory[entity.key],
                last_fetched=local_last_fetched,
                last_calculated=local_last_calculated,
            ),
            "remote": build_remote_entity_status(
                remote_partitions=remote_inventory[entity.key],
                last_fetched=remote_last_fetched,
                last_calculated=remote_last_calculated,
            ),
        }

    return download_status


def scan_local_inventory(
    config: Config,
) -> tuple[dict[str, dict[str, dict[str, dict[str, Any]]]], str]:
    inventory: dict[str, dict[str, dict[str, dict[str, Any]]]] = {
        entity.key: {} for entity in ENTITIES
    }

    for entity in ENTITIES:
        entity_dir = config.local_root / entity.key
        if not entity_dir.exists():
            continue
        if not entity_dir.is_dir():
            raise ScriptError(f"Expected directory path, found non-directory: {entity_dir}")

        for partition_dir in sorted(entity_dir.iterdir(), key=lambda path: path.name):
            if not partition_dir.is_dir():
                continue
            if not partition_dir.name.startswith(PARTITION_PREFIX):
                continue

            partition_key = partition_dir.name[len(PARTITION_PREFIX):]
            listing: dict[str, dict[str, Any]] = {}
            for file_path in sorted(partition_dir.iterdir(), key=lambda path: path.name):
                if not file_path.is_file():
                    continue
                listing[file_path.name] = build_local_file_metadata(file_path)
            inventory[entity.key][partition_key] = listing

    return inventory, iso_timestamp_now()


def scan_remote_inventory(
    config: Config,
) -> tuple[dict[str, dict[str, dict[str, dict[str, Any]]]], str]:
    bucket, prefix = parse_s3_uri(config.remote_root)
    inventory: dict[str, dict[str, dict[str, dict[str, Any]]]] = {
        entity.key: {} for entity in ENTITIES
    }

    for s3_object in iter_remote_objects(config=config, bucket=bucket, prefix=prefix):
        key = s3_object.get("Key")
        if not isinstance(key, str):
            continue

        parsed = parse_partition_object_key(prefix=prefix, key=key)
        if parsed is None:
            continue

        entity_key, partition_key, filename = parsed
        metadata = dict(s3_object)
        metadata["filename"] = filename

        inventory[entity_key].setdefault(partition_key, {})[filename] = metadata

    return inventory, iso_timestamp_now()


def build_local_entity_status(
    *,
    existing_entity: Any,
    local_partitions: dict[str, dict[str, dict[str, Any]]],
    remote_partitions: dict[str, dict[str, dict[str, Any]]],
    last_fetched: str,
    last_calculated: str,
) -> dict[str, Any]:
    partitions: dict[str, Any] = {}

    for partition_key in sorted(set(local_partitions) | set(remote_partitions)):
        listing = ordered_listing(local_partitions.get(partition_key, {}))
        existing_summary = existing_local_partition_summary(
            existing_entity=existing_entity,
            partition_key=partition_key,
        )
        fully_downloaded = bool(existing_summary.get("fully_downloaded"))
        timestamp_fully_downloaded = (
            existing_summary.get("timestamp_fully_downloaded")
            if fully_downloaded
            else None
        )
        partitions[partition_key] = {
            "summary": {
                "total_part_files": len(listing),
                "fully_downloaded": fully_downloaded,
                "timestamp_fully_downloaded": timestamp_fully_downloaded,
                "last_calculated": last_calculated,
            },
            "listing": listing,
        }

    local_status = {
        "summary": {
            "partition_type": PARTITION_TYPE,
            "total_partitions": 0,
            "total_files": 0,
            "fully_downloaded": False,
            "timestamp_fully_downloaded": None,
            "last_fetched": last_fetched,
            "last_calculated": last_calculated,
        },
        "partitions": partitions,
    }
    recalculate_local_entity_summary(local_status, last_calculated)
    return local_status


def build_remote_entity_status(
    *,
    remote_partitions: dict[str, dict[str, dict[str, Any]]],
    last_fetched: str,
    last_calculated: str,
) -> dict[str, Any]:
    partitions: dict[str, Any] = {}
    total_files = 0

    for partition_key in sorted(remote_partitions):
        listing = ordered_listing(remote_partitions[partition_key])
        total_part_files = len(listing)
        total_files += total_part_files
        partitions[partition_key] = {
            "summary": {
                "total_part_files": total_part_files,
                "last_calculated": last_calculated,
            },
            "listing": listing,
        }

    return {
        "summary": {
            "partition_type": PARTITION_TYPE,
            "total_partitions": len(partitions),
            "total_files": total_files,
            "last_fetched": last_fetched,
            "last_calculated": last_calculated,
        },
        "partitions": partitions,
    }


def recalculate_local_entity_summary(
    local_status: dict[str, Any],
    last_calculated: str,
) -> None:
    partitions = local_status["partitions"]
    total_files = 0
    fully_downloaded = bool(partitions)

    for partition_data in partitions.values():
        listing = ordered_listing(partition_data["listing"])
        partition_data["listing"] = listing
        partition_summary = partition_data["summary"]
        partition_summary["total_part_files"] = len(listing)
        total_files += len(listing)
        if not partition_summary.get("fully_downloaded"):
            fully_downloaded = False

    summary = local_status["summary"]
    summary["partition_type"] = PARTITION_TYPE
    summary["total_partitions"] = len(partitions)
    summary["total_files"] = total_files
    summary["fully_downloaded"] = fully_downloaded
    summary["timestamp_fully_downloaded"] = (
        last_calculated if fully_downloaded else None
    )
    summary["last_calculated"] = last_calculated


def existing_local_partition_summary(
    *,
    existing_entity: Any,
    partition_key: str,
) -> dict[str, Any]:
    if not isinstance(existing_entity, dict):
        return {}

    local_state = existing_entity.get("local")
    if not isinstance(local_state, dict):
        return {}

    partitions = local_state.get("partitions")
    if not isinstance(partitions, dict):
        return {}

    partition_state = partitions.get(partition_key)
    if not isinstance(partition_state, dict):
        return {}

    summary = partition_state.get("summary")
    if not isinstance(summary, dict):
        return {}

    return summary


def ordered_listing(
    listing: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {filename: listing[filename] for filename in sorted(listing)}


def count_partitions(
    inventory: dict[str, dict[str, dict[str, dict[str, Any]]]],
) -> int:
    return sum(len(partitions) for partitions in inventory.values())


def sync_pending_partitions(
    *,
    config: Config,
    progress: dict[str, Any],
) -> None:
    download_status = progress.get("download_status")
    if not isinstance(download_status, dict):
        raise ScriptError(
            "Progress file key 'download_status' must contain a JSON object."
        )

    total_pending = 0

    for entity in ENTITIES:
        entity_state = download_status.get(entity.key)
        if not isinstance(entity_state, dict):
            raise ScriptError(f"Missing progress entry for entity: {entity.key}")

        local_state = entity_state.get("local")
        remote_state = entity_state.get("remote")
        if not isinstance(local_state, dict) or not isinstance(remote_state, dict):
            raise ScriptError(f"Malformed progress entry for entity: {entity.key}")

        local_partitions = local_state.get("partitions")
        remote_partitions = remote_state.get("partitions")
        if not isinstance(local_partitions, dict) or not isinstance(remote_partitions, dict):
            raise ScriptError(f"Malformed partition listing for entity: {entity.key}")

        pending_partition_keys: list[str] = []
        frozen_partition_count = 0

        for partition_key in sorted(local_partitions):
            partition_state = local_partitions[partition_key]
            if not isinstance(partition_state, dict):
                raise ScriptError(
                    f"Malformed local partition entry for {entity.key} {partition_key}"
                )
            partition_summary = partition_state.get("summary")
            if not isinstance(partition_summary, dict):
                raise ScriptError(
                    f"Malformed local partition summary for {entity.key} {partition_key}"
                )

            if partition_summary.get("fully_downloaded"):
                frozen_partition_count += 1
                continue
            pending_partition_keys.append(partition_key)

        if pending_partition_keys:
            print_status(
                f"{entity.name}: {len(pending_partition_keys)} pending partitions, "
                f"{frozen_partition_count} frozen partitions."
            )
        else:
            print_status(
                f"{entity.name}: no pending partitions "
                f"({frozen_partition_count} frozen partitions)."
            )

        total_pending += len(pending_partition_keys)

        for partition_key in pending_partition_keys:
            partition_state = local_partitions[partition_key]
            if not isinstance(partition_state, dict):
                raise ScriptError(
                    f"Malformed local partition entry for {entity.key} {partition_key}"
                )
            partition_summary = partition_state.get("summary")
            if not isinstance(partition_summary, dict):
                raise ScriptError(
                    f"Malformed local partition summary for {entity.key} {partition_key}"
                )
            remote_partition_state = remote_partitions.get(partition_key)
            if not isinstance(remote_partition_state, dict):
                raise ScriptError(
                    "Remote inventory missing partition for "
                    f"{entity.name} ({entity.key}) updated_date={partition_key}"
                )
            remote_listing = remote_partition_state.get("listing")
            if not isinstance(remote_listing, dict):
                raise ScriptError(
                    "Malformed remote partition listing for "
                    f"{entity.name} ({entity.key}) updated_date={partition_key}"
                )

            sync_partition(
                config=config,
                entity=entity,
                partition_key=partition_key,
            )
            local_partition_path = (
                config.local_root / entity.key / partition_directory_name(partition_key)
            )
            verified_listing = scan_local_partition(local_partition_path)
            verify_partition_integrity(
                entity=entity,
                partition_key=partition_key,
                remote_listing=remote_listing,
                local_listing=verified_listing,
            )

            last_calculated = iso_timestamp_now()
            partition_state["listing"] = verified_listing
            partition_summary["total_part_files"] = len(verified_listing)
            partition_summary["fully_downloaded"] = True
            partition_summary["timestamp_fully_downloaded"] = last_calculated
            partition_summary["last_calculated"] = last_calculated
            recalculate_local_entity_summary(local_state, last_calculated)
            save_progress(config.progress_path, progress)
            print_status(
                f"Verified {entity.name} updated_date={partition_key} "
                "and marked it fully downloaded."
            )

    if total_pending == 0:
        print_status("No pending partitions to sync.")


def build_local_file_metadata(path: Path) -> dict[str, Any]:
    stat_result = path.stat()
    return {
        "filename": path.name,
        "LastModified": iso_from_unix_timestamp(stat_result.st_mtime),
        "Size": stat_result.st_size,
    }


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ScriptError(f"Expected S3 URI, found: {s3_uri}")

    bucket_and_prefix = s3_uri[len("s3://"):]
    bucket, separator, prefix = bucket_and_prefix.partition("/")
    if not bucket:
        raise ScriptError(f"Missing bucket name in S3 URI: {s3_uri}")
    if not separator:
        return bucket, ""
    return bucket, prefix.strip("/")


def iter_remote_objects(
    *,
    config: Config,
    bucket: str,
    prefix: str,
) -> Sequence[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    continuation_token: str | None = None
    prefix_value = f"{prefix}/" if prefix else ""

    while True:
        args = [
            config.aws_bin,
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix_value,
            "--output",
            "json",
            "--no-sign-request",
        ]
        if continuation_token:
            args.extend(["--continuation-token", continuation_token])

        response = run_json_command(args)
        contents = response.get("Contents", [])
        if not isinstance(contents, list):
            raise ScriptError("Unexpected list-objects-v2 response: missing Contents list.")
        for item in contents:
            if isinstance(item, dict):
                objects.append(item)

        if not response.get("IsTruncated"):
            return objects

        continuation_token = response.get("NextContinuationToken")
        if not isinstance(continuation_token, str) or not continuation_token:
            raise ScriptError(
                "Unexpected list-objects-v2 response: missing continuation token."
            )


def parse_partition_object_key(
    *,
    prefix: str,
    key: str,
) -> tuple[str, str, str] | None:
    expected_prefix = f"{prefix}/" if prefix else ""
    if expected_prefix and not key.startswith(expected_prefix):
        return None

    relative_key = key[len(expected_prefix):] if expected_prefix else key
    parts = relative_key.split("/")
    if len(parts) != 3:
        return None

    entity_key, partition_name, filename = parts
    if entity_key not in ENTITY_KEYS:
        return None
    if not partition_name.startswith(PARTITION_PREFIX):
        return None
    if not filename:
        return None

    return entity_key, partition_name[len(PARTITION_PREFIX):], filename


def run_json_command(args: Sequence[str]) -> dict[str, Any]:
    output = run_command(args, capture_output=True)
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ScriptError(
            f"Command did not return valid JSON: {shlex.join(list(args))}\n{exc}"
        ) from exc

    if not isinstance(parsed, dict):
        raise ScriptError(
            f"Command did not return a JSON object: {shlex.join(list(args))}"
        )

    return parsed


def sync_partition(
    *,
    config: Config,
    entity: Entity,
    partition_key: str,
) -> None:
    remote_prefix = remote_partition_uri_for(config, entity, partition_key)
    local_dir = config.local_root / entity.key / partition_directory_name(partition_key)
    local_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Syncing {entity.name} updated_date={partition_key}: "
        f"{remote_prefix} -> {local_dir}",
        file=sys.stderr,
    )
    run_command(
        [
            config.aws_bin,
            "s3",
            "sync",
            remote_prefix,
            str(local_dir),
            "--no-sign-request",
            "--no-progress",  # File transfer progress is not displayed. This flag is only applied when the quiet and only-show-errors flags are not provided.
            "--checksum-mode", "ENABLED",  # To retrieve the checksum, this mode must be enabled. If the object has a checksum, it will be verified.
            "--delete",  # Files that exist in the destination but not in the source are deleted during sync. Note that files excluded by filters are excluded from deletion. "Otherwise you’ll get duplicate entities that have moved between partitions." https://developers.openalex.org/download/download-to-machine#download-the-full-snapshot
        ],
        capture_output=False,
    )


def scan_local_partition(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    if not path.is_dir():
        raise ScriptError(f"Expected directory path, found non-directory: {path}")

    listing: dict[str, dict[str, Any]] = {}
    for file_path in sorted(path.iterdir(), key=lambda file_path: file_path.name):
        if not file_path.is_file():
            continue
        listing[file_path.name] = build_local_file_metadata(file_path)
    return ordered_listing(listing)


def verify_partition_integrity(
    *,
    entity: Entity,
    partition_key: str,
    remote_listing: dict[str, Any],
    local_listing: dict[str, Any],
) -> None:
    remote_filenames = set(remote_listing)
    local_filenames = set(local_listing)
    missing_files = sorted(remote_filenames - local_filenames)
    unexpected_files = sorted(local_filenames - remote_filenames)
    mismatches: list[str] = []

    for filename in sorted(remote_filenames & local_filenames):
        remote_metadata = remote_listing[filename]
        local_metadata = local_listing[filename]
        if not isinstance(remote_metadata, dict) or not isinstance(local_metadata, dict):
            mismatches.append(f"{filename}: malformed metadata entry")
            continue

        metadata_comparison = select_comparable_metadata(
            remote_metadata=remote_metadata,
            local_metadata=local_metadata,
        )
        if metadata_comparison is None:
            mismatches.append(
                f"{filename}: missing comparable LastModified or Size metadata"
            )
            continue

        metadata_key, remote_value, local_value = metadata_comparison
        if remote_value != local_value:
            mismatches.append(
                f"{filename}: {metadata_key} mismatch "
                f"(remote={remote_value!r}, local={local_value!r})"
            )

    if (
        len(remote_listing) != len(local_listing)
        or missing_files
        or unexpected_files
        or mismatches
    ):
        details = [
            "Partition integrity check failed for "
            f"{entity.name} ({entity.key}) updated_date={partition_key}",
            f"Remote file count: {len(remote_listing)}",
            f"Local file count:  {len(local_listing)}",
        ]
        if missing_files:
            details.append(f"Missing local files: {', '.join(missing_files)}")
        if unexpected_files:
            details.append(f"Unexpected local files: {', '.join(unexpected_files)}")
        details.extend(mismatches)
        raise ScriptError("\n".join(details))


def select_comparable_metadata(
    *,
    remote_metadata: dict[str, Any],
    local_metadata: dict[str, Any],
) -> tuple[str, str, str] | None:
    comparable_pairs = (
        (
            "Size",
            normalize_size_metadata(remote_metadata.get("Size")),
            normalize_size_metadata(local_metadata.get("Size")),
        ),
        (
            "LastModified",
            normalize_timestamp_metadata(remote_metadata.get("LastModified")),
            normalize_timestamp_metadata(local_metadata.get("LastModified")),
        ),
    )

    for metadata_key, remote_value, local_value in comparable_pairs:
        if remote_value is None or local_value is None:
            return None
        if remote_value != local_value:
            return metadata_key, remote_value, local_value

    return "verified", "ok", "ok"


def normalize_size_metadata(value: Any) -> str | None:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def normalize_timestamp_metadata(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return normalized

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return str(int(parsed.astimezone(timezone.utc).timestamp()))


def partition_directory_name(partition_key: str) -> str:
    return f"{PARTITION_PREFIX}{partition_key}"


def remote_partition_uri_for(
    config: Config,
    entity: Entity,
    partition_key: str,
) -> str:
    return (
        f"{remote_entity_uri_for(config, entity)}/"
        f"{partition_directory_name(partition_key)}"
    )


def iso_timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_from_unix_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace(
        "+00:00",
        "Z",
    )


def sync_entity(config: Config, entity: Entity) -> None:
    remote_prefix = remote_entity_uri_for(config, entity)
    local_dir = config.local_root / entity.key
    local_dir.mkdir(parents=True, exist_ok=True)

    print(f"Syncing {entity.name}: {remote_prefix} -> {local_dir}", file=sys.stderr)
    run_command(
        [
            config.aws_bin,
            "s3",
            "sync",
            remote_prefix,
            str(local_dir),
            "--no-sign-request",
            "--no-progress",  # File transfer progress is not displayed. This flag is only applied when the quiet and only-show-errors flags are not provided.
            "--checksum-mode", "ENABLED",  # To retrieve the checksum, this mode must be enabled. If the object has a checksum, it will be verified.
            #"--no-overwrite",  # This flag prevents overwriting of files at the destination. With this flag, only files not present at the destination will be transferred. "You never need to re-download a partition you already have. Anything that changed has moved to a newer partition." https://developers.openalex.org/download/snapshot-format#how-partitions-work
            "--delete",  # Files that exist in the destination but not in the source are deleted during sync. Note that files excluded by filters are excluded from deletion. "Otherwise you’ll get duplicate entities that have moved between partitions." https://developers.openalex.org/download/download-to-machine#download-the-full-snapshot
        ],
        capture_output=False,
    )


def remote_entity_uri_for(config: Config, entity: Entity) -> str:
    return f"{config.remote_root}/{entity.key}"


def remote_manifest_uri_for(config: Config, entity: Entity) -> str:
    return f"{remote_entity_uri_for(config, entity)}/manifest"


def download_and_hash_remote_manifest(config: Config, remote_manifest_uri: str) -> str:
    with tempfile.TemporaryDirectory(prefix="openalex-manifest-") as tmp_dir_str:
        temp_path = Path(tmp_dir_str) / "manifest"
        download_s3_object(
            config=config,
            s3_uri=remote_manifest_uri,
            destination=temp_path,
        )
        return sha256_file(temp_path)


def download_s3_object(config: Config, s3_uri: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            config.aws_bin,
            "s3",
            "cp",
            s3_uri,
            str(destination),
            "--no-sign-request",
            "--only-show-errors",
        ],
        capture_output=False,
    )


def is_directory_empty(path: Path) -> bool:
    if not path.exists():
        return True
    if not path.is_dir():
        raise ScriptError(f"Expected directory path, found non-directory: {path}")
    return next(path.iterdir(), None) is None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def run_command(args: Sequence[str], *, capture_output: bool) -> str:
    try:
        completed = subprocess.run(
            list(args),
            check=False,
            capture_output=capture_output,
            text=True,
        )
    except FileNotFoundError as exc:
        program = args[0] if args else "command"
        raise ScriptError(f"Executable not found: {program!r}") from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise ScriptError(
            f"Command failed: {shlex.join(list(args))}\n{detail}"
        )

    return completed.stdout if capture_output and completed.stdout else ""


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
