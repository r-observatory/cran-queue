#!/usr/bin/env bash
# Rebuild the previous release's full queue.db in the current directory, with
# its manifest.json beside it. queue.db is carried forward in the releases, so
# this download IS the pipeline's state.
#
# Treating any failure here as "no previous release" has cost this pipeline its
# history twice: on 2026-07-16 a swallowed download (#4), on 2026-10-03 a
# swallowed release lookup (#15). So the cases diverge:
#
#   * a release list that comes back empty is a genuine first run;
#   * a repository that HAS a release must hand over its database, every
#     history part its manifest lists, and the manifest, or the run fails.
#
# gh runs only in plain assignments or bare commands, so under set -e an API
# error fails the run instead of reading as "no releases".
#
# A release since the yearly split carries queue-current.db.zst, and its
# manifest names the history-YYYY releases holding the closed years. Earlier
# releases carry the whole history as queue.db.zst, or as a plain queue.db.
set -euo pipefail

releases="$(gh release list --limit 1 --exclude-drafts)"
if [ -z "$releases" ]; then
  echo "No releases exist yet; starting fresh."
  exit 0
fi
tag="$(gh release view --json tagName -q .tagName)"
echo "Previous release: $tag"

# A missing asset and a failed download look the same to `gh release download`,
# so ask the release what it carries before pulling anything.
assets="$(gh release view "$tag" --json assets -q '.assets[].name')"
has() { printf '%s\n' "$assets" | grep -qx "$1"; }

if has 'queue-current.db.zst'; then
  want="queue-current.db.zst"
elif has 'queue.db.zst'; then
  want="queue.db.zst"
elif has 'queue.db'; then
  want="queue.db"
else
  echo "::error::${tag} carries none of queue-current.db.zst, queue.db.zst or queue.db (assets: ${assets:-none}). Refusing to start from an empty database."
  exit 1
fi
echo "Pulling $want"

# fetch TAG ASSET DIR: download with retries, or report and return 1.
fetch() {
  local attempt
  for attempt in 1 2 3 4 5; do
    if gh release download "$1" --pattern "$2" --dir "$3" --clobber; then
      [ "$attempt" -gt 1 ] && echo "downloaded $2 from $1 (attempt $attempt)"
      return 0
    fi
    echo "attempt ${attempt}/5 failed to download $2 from $1"
    [ "$attempt" -lt 5 ] && sleep $((attempt * 10))
  done
  echo "::error::$1 exists but its $2 could not be downloaded. Refusing to start from an incomplete database, which would discard the accumulated snapshots."
  return 1
}

fetch "$tag" "$want" . || exit 1
if [ "$want" != "queue.db" ]; then
  command -v zstd >/dev/null || { echo "::error::zstd not found on the runner"; exit 1; }
  zstd -dq -f "$want" -o queue.db \
    || { echo "::error::${tag}'s ${want} could not be decompressed"; exit 1; }
  rm -f "$want"
fi
# An empty or truncated file would reach import-history.R as a fresh database
# and re-bootstrap just as a failed download does.
if [ ! -s queue.db ]; then
  echo "::error::downloaded queue.db from ${tag} is empty"
  exit 1
fi

# The manifest states what that release contained, and update.R holds this run
# to it before publishing. Releases predating it do not have one, so its absence
# is not an error there. A release that lists one must hand it over, and a
# yearly release cannot be used without it: it names the history parts.
if has 'manifest.json'; then
  fetch "$tag" manifest.json . || exit 1
elif [ "$want" = "queue-current.db.zst" ]; then
  echo "::error::${tag} carries queue-current.db.zst but no manifest.json naming its history parts. Refusing to start from one year."
  exit 1
else
  echo "::warning::${tag} has no manifest.json; this run cannot be checked against it"
fi

if [ "$want" = "queue-current.db.zst" ]; then
  command -v jq >/dev/null || { echo "::error::jq not found on the runner"; exit 1; }
  parts="$(jq -r '.parts[] | "\(.tag) \(.asset)"' manifest.json)"
  mkdir -p parts
  while read -r ptag passet; do
    [ -n "$ptag" ] || continue
    fetch "$ptag" "$passet" parts || exit 1
  done <<<"$parts"
  Rscript scripts/assemble.R queue.db manifest.json parts
  rm -rf parts
fi
