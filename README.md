# sync-openalex

**[paveljee][github-paveljee]**
**commented on**
**Apr 5, 2026**

> `sync-openalex.py`
> incrementally mirrors the OpenAlex snapshot
> into `./openalex/data`.
>
> For example,
> I have been using this with a 8 TB HDD with no issues.
> 
> Informed heavily by official OpenAlex docs,
> in particular
> [Snapshot data format][openalex-snapshot-format] and
> [Download to your machine][openalex-download-to-machine].
>
> Has no external dependencies,
> Python stdlib only
> (tested on 3.12.9).
>
> Run it with `python sync-openalex.py`
> (assumes that [AWS CLI][aws-cli] is installed).
>
> Type-check and lint with `make`
> (assumes `mypy` and `ruff` are installed).
>
> Originally generated using ChatGPT,
> later revised manually
> but mostly using OpenAI Codex
> (e.g., see `SPEC.md`).
> 
> Features:
> 
> - uses a hardcoded entity list in the script
> - validates each entity manifest first
> - syncs one `updated_date=...` partition at a time
> with `aws s3 sync --checksum-mode ENABLED`
> - tracks descriptive local and remote inventory
> in `./sync_progress.json`
> - freezes partitions once they are fully downloaded
> - creates a `sync_progress.json.tar.gz` backup before reuse
> - can prompt to rename stale local-only partitions
> to `.delete`
> when the remote side no longer has them
>
> The changelog is within the script as a comment.

[github-paveljee]: https://github.com/paveljee
[openalex-snapshot-format]: https://developers.openalex.org/download/snapshot-format
[openalex-download-to-machine]: https://developers.openalex.org/download/download-to-machine
[aws-cli]: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
