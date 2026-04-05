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
        - last_calculated
      - partitions
        - [for each updated_date as key, sorted from older to latest]
          - summary
            - total_part_files
            - fully_downloaded
            - timestamp_fully_downloaded
          - listing
            - [for each part file, sorted from min to max]
              - [here goes flat dict of any metadata aws offers for the from the file listing, but must include filename, last modified date, and hash sum, but also include anything else it offers]
    - remote
      - [here goes same tree as for local, except fully_downloaded and timestamp_fully_downloaded keys are not used]

how this all works:
the script does global search in DEFAULT_LOCAL_ROOT and updates local dict.
keys are self explanatory above except that
last_calculated means iso timestamp when summary last recalculated and must be accurate;
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

in fact, we might have opted for trusting aws sync on checksums, and
therefore keep sync_progress.json purely descriptive.
yet we will not do that and will do our own checks to be sure.
so we only tick fully_downloaded when we see that the actually downloaded files
match what we expected to get from this partition based on the aws ls fetch we pre-run.

## ai section - how it understood the spec
