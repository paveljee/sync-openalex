# Specification for script update

## human wrote - ai never touches this section

Date of spec: 2026-04-05

modify the behaviour of sync-openalex.py to the below.
only modify what you really need to change to achieve this spec,
don't touch anything else about the script in passing -
including keep all code comments.

the reason why we are changing is that
we already own the inventory of entities by hardcoding their list but
we also want to own the inventory of partitions,
while also noting down when a particular partition has been full downloaded and
keeping any older partitions intact and
only downloading new ones.
this comes from [openalex policy](https://developers.openalex.org/download/snapshot-format)
[copied and pasted on 2026-04-05 @ 06:13:57 UTC]:

> Keeping your snapshot up to date
> --------------------------------
> 
> The `updated_date` partitions make incremental updates straightforward. Unlike dated snapshots that each contain the full dataset, each partition contains only the records that last changed on that date.
> 
> ###
> 
> [​](https://developers.openalex.org/download/snapshot-format#how-partitions-work)
> 
> How partitions work
> 
> Imagine launching OpenAlex with 1,000 Authors, all created on 2024-01-01:
> 
> ```
> /data/authors/
> ├── manifest
> └── updated_date=2024-01-01 [1000 Authors]
>     ├── 0000_part_00.gz
>     └── ...
> 
> ```
> 
> If we update 50 of those Authors on 2024-01-15, they move out of the old partition and into the new one:
> 
> ```
> /data/authors/
> ├── manifest
> ├── updated_date=2024-01-01 [950 Authors]
> │   └── ...
> └── updated_date=2024-01-15 [50 Authors]
>     └── ...
> 
> ```
> 
> If we also discover 50 new Authors, they go into the same new partition:
> 
> ```
> /data/authors/
> ├── manifest
> ├── updated_date=2024-01-01 [950 Authors]
> │   └── ...
> └── updated_date=2024-01-15 [100 Authors]
>     └── ...
> 
> ```
> 
> So if you made your snapshot copy on 2024-01-01, you only need to download `updated_date=2024-01-15` to get everything that changed or was added since then.
> 
> To update a snapshot copy that you created or updated on date `X`, insert or update the records in partitions where `updated_date` > `X`.
> 
> You never need to re-download a partition you already have. Anything that changed has moved to a newer partition.

so here is the behaviour we want:

first we must open file sync_progress.json from a prespecified location,
by default look in Path("./sync_progress.json").
the file's structure:

- download_status
  - [for each entity as key, honouring sorting as in hardcoded variable]:
    - local
      - summary
        - partition_type [always hardcoded to "updated_date"]
        - total_partitions
        - total_files [meaning total files across all partitions]
        - fully_downloaded
        - timestamp_fully_downloaded
        - last_fetched
        - last_calculated
      - partitions
        - [for each updated_date as key, sorted from older to latest]
          - summary
            - total_part_files
            - fully_downloaded
            - timestamp_fully_downloaded
            - last_calculated
          - listing
            - [for each part file, sorted from min to max]
              - [here goes flat dict of any metadata aws offers for the from the file listing, but must include filename, last modified date, and hash sum, but also include anything else it offers; use key names offered by aws]
    - remote
      - [here goes same tree as for local, except fully_downloaded and timestamp_fully_downloaded keys are not used]

how this all works:
the script does global search in DEFAULT_LOCAL_ROOT and updates local dict.
keys are self explanatory above except that
last_calculated means iso timestamp when summary last recalculated and must be accurate;
last_fetched means when local global search or remote aws ls was done,
as such last_fetched always preceded last_calculated;
fully_downloaded is boolean and set to false by default and will only be set to true if conditions are met - see below;
timestamp_fully_downloaded means when conditions were met for fully_downloaded;
so, script does global search in local dir and updates local dict.
then it does a broad aws ls on DEFAULT_REMOTE_ROOT and updates remote dict;
broad aws ls is expected to give us fully breakdown on all files and so
we parse it locally to recalculate summary.
as such, no separate fetch date is needed per partition because 
last_calculated reflects when fetch was done globally and then
this is parsed to calculate partition level info;
both for local and remote this behaviour is same.

now to the key behaviour -
fully_downloaded and
timestamp_fully_downloaded keys.
let's go over this for partition-level first.
the script must verify that for this particular partition,
at the time of fetch
number of part files is equal and
each part file on remote has same hash as local.
this verifies integrity for partition at fetch time.
if this is ok we mark it fully_downloaded=true and
set timestamp_fully_downloaded to _current last_calculated_.
since then we consider this partition frozen and never touch it.

then the role of the script is simply,
on each launch,
doing a new fetch local, then remote;
any pre-existing fully_downloaded=true from further processing;
then go over all fully_downloaded=false,
looping over entity and then nested over partition,
one partition at a time,
doing the aws sync command for this particular partition only and
let aws handle this per partition;
just one aws sync per partition is finished,
do our fully_downloaded integrity check and
if ok mark fully_downloaded=true,
else print granular error and exit 1,
keeping therefore this fully transactional at partition level,
meaning that either
partition is fully downloaded _within a single script run_ and marked fully_downloaded,
or fails for whatever reason and thus is not marked fully downloaded.

args we use for partition level aws sync:
"--no-sign-request",
"--no-progress",
"--checksum-mode", "ENABLED"
"--delete",

there is _no global level aws sync_ happening therefore in this spec.

in fact, we will have to opt for trusting aws sync on checksums, and
therefore keep sync_progress.json purely descriptive.
so we don't calculate any hash sums on your own
because it's taking too long.
so, we trust aws sync to do its checksum mode and
we just verify that last modified date and file size are identical.

of note, existing manifest checking behaviour still preceds all ops described in this spec and
as such, must be fully preserved unchanged.

asking ai below to interpret.
using:
```
model = "gpt-5.4"
model_reasoning_effort = "xhigh"
web_search = "disabled"
network_access = false
```
via codex vs code extension
openai.chatgpt
Version
26.325.31654
under sandboxed lima.

## ai section - how it understood the spec

This is how I currently understand the requested change.

1. High-level behavior change
- Preserve the current manifest-first behavior unchanged, and replace the later whole-entity sync flow with a progress-file-driven, partition-by-partition sync flow.
- The hardcoded entity list remains the owned inventory of entities and also defines output ordering.
- The script should now own the inventory of partitions in `sync_progress.json`, while keeping already completed older partitions intact and only syncing partitions that are not yet marked complete.
- The correctness gate for the sync stage is no longer "manifest matches, then sync the whole entity"; it is now "one partition sync completes with `aws s3 sync --checksum-mode ENABLED`, then that partition passes our own descriptive check against the remote inventory captured for this run".

2. Progress file shape
- Add a progress file path, defaulting to `Path("./sync_progress.json")`.
- Top level shape is:

```json
{
  "download_status": {
    "<entity-key>": {
      "local": {},
      "remote": {}
    }
  }
}
```

- Entity keys in `download_status` must follow the hardcoded entity order.
- Entity summaries include both `last_fetched` and `last_calculated`.
- Partition summaries include `last_calculated`.
- `partition_type` is always the literal string `"updated_date"`.
- My interpretation is that partition keys inside `partitions` should be the bare date value, for example `2024-01-15`, because `partition_type` already stores the `"updated_date"` label.
- My interpretation is that `listing` should be an ordered mapping keyed by filename, with each value being the flat metadata dict for that file and also including `filename` inside the dict.

3. What gets scanned each run
- First, load any existing `sync_progress.json`. If it does not exist yet, initialize an empty structure and continue.
- Then do a full local scan under `DEFAULT_LOCAL_ROOT`, limited to the hardcoded entities, and rebuild the `local` tree from disk.
- Then do one broad remote inventory fetch under `DEFAULT_REMOTE_ROOT` and rebuild the `remote` tree from that fetch.
- Both local and remote summaries are descriptive snapshots of what was seen during the current run.
- `last_fetched` must be the ISO timestamp for when the raw local scan or broad remote listing happened, and it must precede `last_calculated`.
- `last_calculated` must be an accurate ISO timestamp for when the corresponding inventory snapshot was computed.
- The remote tree mirrors the local tree structurally, except remote does not use `fully_downloaded` or `timestamp_fully_downloaded`.

4. Per-file metadata
- Each file entry must contain at least:
- `filename`
- the AWS-provided last modified field
- the AWS-provided hash / checksum field
- Any other metadata returned by AWS for that file should also be preserved in the same flat dict.
- Use the key names offered by AWS for those metadata fields.
- For local files, do not calculate any hashes in the script; the local listing only needs the metadata required for the descriptive snapshot and the later last-modified and size comparison.

5. Meaning of `fully_downloaded`
- Partition level:
- Default is `false`.
- It becomes `true` only after a partition-scoped sync finishes and that exact partition passes integrity verification during the same run.
- The integrity rule is strict: the remote and local partition must have the same number of part files, and every expected part file must have the same last modified date and file size on both sides.
- When that passes, set `timestamp_fully_downloaded` to the partition's current `last_calculated`.
- After that, the partition is frozen: future runs keep its stored `fully_downloaded=true` state and do not sync it again.
- This frozen behavior is intentional even if the current remote listing for that older partition later changes, because the spec explicitly wants older partitions kept intact locally.
- Entity level:
- My interpretation is that entity summary `fully_downloaded` is a rollup over partition states, not its own independent sticky state.
- In other words, it should be `true` when every partition currently tracked for that entity is marked `fully_downloaded=true`, and `false` otherwise.
- My interpretation is that entity summary `timestamp_fully_downloaded` should be the entity summary's current `last_calculated` when that rollup is true, and empty or null otherwise.

6. How remote-only new partitions are represented
- If the remote inventory contains a partition that is not present locally yet, the script still needs a local progress entry for that partition so it can be scheduled for download.
- My interpretation is that after the remote fetch, the local tree should be augmented with any missing remote partition keys, initialized as not fully downloaded, with an empty local listing until the partition is actually synced.

7. Sync loop
- There is no entity-wide or global `aws s3 sync` anymore.
- After the initial local and remote inventory rebuild, the script iterates:
- entities in hardcoded order
- partitions from oldest to newest
- It skips any partition already marked `fully_downloaded=true` in the persisted local progress state.
- For every remaining partition, it runs exactly one partition-scoped `aws s3 sync`.
- The partition-scoped sync uses these flags:
- `--no-sign-request`
- `--no-progress`
- `--checksum-mode ENABLED`
- `--delete`

8. Transaction boundary
- A partition is the unit of transaction.
- After a partition sync completes, the script immediately re-checks that one partition against the remote inventory already captured for this run.
- If the check passes, mark it fully downloaded and persist that status.
- If the check fails, print a granular error and exit with status 1.
- Therefore a partition is either fully verified and marked within one run, or it is left unmarked.

9. Assumptions I am making from the spec before implementation
- The existing manifest download and manifest checksum workflow remains in the main control flow and must be preserved unchanged before the later sync work starts.
- Scanning should ignore entity directories that are not in the hardcoded entity list, because the entity inventory is explicitly owned by that list.
- The single broad remote fetch must provide enough metadata to populate the remote listing and later compare last modified date and file size. Any AWS checksum metadata it exposes is descriptive only, because transfer integrity is being trusted to `aws s3 sync --checksum-mode ENABLED`.
