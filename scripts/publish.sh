#!/usr/bin/env bash
# Publish this run: first a history-YYYY release for any closed year that has
# none (new-parts.tsv, written by update.R), then the run's own release.
#
# A history release is written once and never changed. Each is a prerelease
# with --latest=false, because every reader of this repository (this pipeline's
# own prior download, the data merger) resolves Latest with no tag, and Latest
# must stay the newest hourly release. The parts go first so that the run's
# manifest never names a part that is not published yet.
set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"

# HTTP status of the release for tag $1: 200, 404, or empty when the API could
# not be reached. Anything but 200 or 404 stops the run.
release_status() {
  local out
  out="$(gh api -i "repos/${GITHUB_REPOSITORY}/releases/tags/$1" 2>/dev/null || true)"
  printf '%s\n' "$out" | head -1 | awk '{print $2}'
}

# The digest GitHub records for asset $2 on release $1, or empty.
asset_digest() {
  gh release view "$1" --json assets -q ".assets[] | select(.name == \"$2\") | .digest"
}

if [ -s new-parts.tsv ]; then
  while IFS=$'\t' read -r year ptag path sha; do
    [ -n "$year" ] || continue
    asset="$(basename "$path")"
    status="$(release_status "$ptag")"
    case "$status" in
      404)
        gh release create "$ptag" "$path" \
          --title "Queue snapshots $year" \
          --notes "queue_snapshots rows for $year, ids as in the full database. Written once; the hourly releases list it in manifest.json. See the README for putting the years back together." \
          --prerelease --latest=false
        ;;
      200)
        have="$(asset_digest "$ptag" "$asset")"
        if [ -z "$have" ]; then
          gh release upload "$ptag" "$path"
        elif [ "$have" != "sha256:$sha" ]; then
          echo "::error::$ptag already holds a different $asset ($have; this run built sha256:$sha). A closed year must not change."
          exit 1
        fi
        ;;
      *)
        echo "::error::could not tell whether $ptag exists (HTTP ${status:-none}); not publishing"
        exit 1
        ;;
    esac
    have="$(asset_digest "$ptag" "$asset")"
    if [ "$have" != "sha256:$sha" ]; then
      echo "::error::$ptag holds $asset as ${have:-nothing}, expected sha256:$sha"
      exit 1
    fi
    echo "$ptag holds $asset (sha256:$sha)"
  done < new-parts.tsv
fi

TAG="v$(date -u +'%Y%m%d-%H%M%S')"
gh release create "$TAG" \
  queue-current.db.zst \
  manifest.json \
  --title "Queue snapshot $TAG" \
  --notes-file release_notes.md \
  --latest
