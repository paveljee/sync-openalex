#!/usr/bin/env python3
# generated using ChatGPT 5.4 Extended thinking
# via chatgpt.com on 2026-03-30; prompt lost
from __future__ import annotations

import argparse
import hashlib
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence


DEFAULT_REMOTE_ROOT: Final[str] = "s3://openalex/data"
DEFAULT_LOCAL_ROOT: Final[Path] = Path("./openalex/data")
DEFAULT_AWS_BIN: Final[str] = shutil.which("aws") or "aws"


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


@dataclass(frozen=True, slots=True)
class Config:
    remote_root: str
    local_root: Path
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
            "Validate local OpenAlex manifests and then sync each entity "
            "directory one at a time."
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
        "--aws-bin",
        default=DEFAULT_AWS_BIN,
        help="Path to the aws CLI binary.",
    )
    namespace = parser.parse_args(list(argv))
    return Config(
        remote_root=str(namespace.remote_root).rstrip("/"),
        local_root=Path(namespace.local_root),
        aws_bin=str(namespace.aws_bin),
    )


def main(argv: Sequence[str]) -> int:
    config = parse_args(argv)

    results = verify_or_initialize_manifests(config)
    print_manifest_summary(results)

    for entity in ENTITIES:
        sync_entity(config, entity)

    print("All entity syncs completed.")
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
