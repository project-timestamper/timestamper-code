#!/usr/bin/env bash
# Commit and push docs/common_crawl_blocks one calendar year at a time
# (each pack stays well under GitHub's 2 GiB push limit).
#
# Run from the timestamper git repo (not timestamper-code):
#   ../timestamper-code/push-cc-blocks-by-year.sh
#   ../timestamper-code/push-cc-blocks-by-year.sh --dry-run
#
# If you already made one giant unpushed commit of all crawls:
#   git reset HEAD~1    # keeps files, unstages; do not use --hard
# then run this script.
#
# Passkey: Git cannot prompt a passkey on every push. Authenticate once,
# then this script uses the stored credential:
#   gh auth login -h github.com -p https -w
#   gh auth setup-git
# Browser/passkey happens during `gh auth login`. After that, `git push`
# uses `gh` as the credential helper (no passkey per year).
# Check with: gh auth status
set -euo pipefail

dry_run=0
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=1
fi

root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "run this from the timestamper git repo" >&2
  exit 1
}
blocks="$root/docs/common_crawl_blocks"
if [[ ! -d "$blocks" ]]; then
  echo "missing $blocks" >&2
  exit 1
fi

year_of() {
  local name="$1"
  if [[ "$name" =~ CC-MAIN-([0-9]{4}) ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
  fi
}

years="$(
  for dir in "$blocks"/CC-MAIN-*; do
    [[ -d "$dir" ]] || continue
    year_of "$(basename "$dir")"
  done | sort -u
)"

if [[ -z "$years" ]]; then
  echo "no CC-MAIN-* directories in $blocks" >&2
  exit 1
fi

echo "repo: $root"
echo "years: $(echo "$years" | tr '\n' ' ')"
echo

commit_if_needed() {
  local message="$1"
  if git diff --cached --quiet; then
    echo "  nothing staged, skip commit"
    return 0
  fi
  if [[ "$dry_run" -eq 1 ]]; then
    echo "  dry-run: would commit: $message"
    git reset HEAD >/dev/null
    return 0
  fi
  git commit -m "$message"
  git push
}

if [[ -f "$blocks/completed_crawls.txt" ]]; then
  echo "==> completed_crawls.txt"
  git add -- "docs/common_crawl_blocks/completed_crawls.txt"
  commit_if_needed "Add Common Crawl completed_crawls.txt"
fi

for year in $years; do
  echo "==> $year"
  paths=()
  for dir in "$blocks"/CC-MAIN-*; do
    [[ -d "$dir" ]] || continue
    [[ "$(year_of "$(basename "$dir")")" == "$year" ]] || continue
    paths+=("docs/common_crawl_blocks/$(basename "$dir")")
  done
  if [[ ${#paths[@]} -eq 0 ]]; then
    continue
  fi
  printf '  %s\n' "${paths[@]}"
  git add -- "${paths[@]}"
  commit_if_needed "Add Common Crawl ${year} block hashes"
done

echo "done"
