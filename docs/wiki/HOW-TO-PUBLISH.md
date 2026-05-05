# How to publish this wiki to the GitHub Wiki tab

## TL;DR

The `docs/wiki/` directory in this repo is the source of truth for
the eBull operator wiki. To publish it under the **Wiki** tab on
GitHub:

1. Visit https://github.com/Luke-Bradford/eBull/wiki and click
   "Create the first page". Title it `Home`, paste any placeholder
   content, save. (GitHub does not provision the wiki Git
   repository until the first page is created via the web UI.)
2. Run the publish script (below) to mirror `docs/wiki/` into the
   wiki repo.

## Why this is necessary

GitHub Wiki content lives in a sibling Git repository at
`https://github.com/<owner>/<repo>.wiki.git`. That repo is
auto-created the first time a page is saved through the web UI;
before that, push attempts return `Repository not found`.

eBull keeps the canonical wiki content in-repo (under `docs/wiki/`)
so it ships with the rest of the source — wiki changes go through
the same PR review process as code. The GitHub Wiki tab is a
mirror, not the source.

## Publish script

After the one-time web bootstrap, run:

```bash
git clone https://github.com/Luke-Bradford/eBull.wiki.git /tmp/ebull-wiki
cd /tmp/ebull-wiki

# Mirror docs/wiki/ contents. GitHub Wiki uses Home.md not README.md.
cp /path/to/eBull/docs/wiki/README.md       Home.md
cp /path/to/eBull/docs/wiki/getting-started.md ./
cp /path/to/eBull/docs/wiki/architecture.md  ./
cp /path/to/eBull/docs/wiki/data-sources.md  ./
cp /path/to/eBull/docs/wiki/ownership-card.md ./
cp /path/to/eBull/docs/wiki/glossary.md      ./
mkdir -p runbooks
cp /path/to/eBull/docs/wiki/runbooks/*.md    runbooks/

git add .
git commit -m "sync from docs/wiki/"
git push
```

GitHub Wiki flattens the directory tree by default. Subdirectories
under the wiki repo render as path segments in URLs. Internal
markdown links in `docs/wiki/` use relative `[label](path/file.md)`
syntax that works both in-repo and on the wiki — no rewrite needed.

## Keeping the mirror in sync

Re-run the publish script after any merged PR that touches
`docs/wiki/`. Or wire it into a GitHub Action triggered on push to
`main`.

## Why not use the wiki repo directly

- Wiki edits would not go through PR review.
- Wiki content would not version alongside the code it documents.
- Diffs against schema / API changes would be impossible to track.

The in-repo + mirror pattern keeps the wiki as a derived artefact of
reviewed source.
