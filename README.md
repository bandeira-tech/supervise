# supervise

A continuous status dashboard for a project directory. One terminal window,
always-on, shows you the things you usually `git status` / `gh pr list` /
re-run tests for, without typing anything.

## What it shows

**ops view (default)** — git branch, ahead/behind, last commit, dirty status
with each modified/untracked path rendered as a clickable `file://` link;
test pass/fail with duration; open PRs and active worktrees as a unified
list (clickable PR numbers, clickable worktree paths that open in Finder);
package version vs. registry.

**content view** — top-level file/dir layout of the target.

**action view** — detected issues (e.g. merged worktrees still on disk) with
single-keystroke fix actions.

## Run

```sh
./supervise [path]              # defaults to cwd
./supervise -v content ~/proj   # alternate view
```

Keys: `1` ops, `2` content, `3` action, `?` help, `!`/`@`/`#` force refresh.
Click a row in ops view to wake its fetcher (tests, PRs).

## Hot reload

The script watches its own mtime. When you save a change to `supervise.py`,
running clients spawn the new version under `--self-check`; if it imports
cleanly and renders once, they `os.execv` themselves onto it. If self-check
fails, the old process keeps running and a `⚠ reload failed: <error>`
banner appears on the title bar until you fix the file.

## Layout

- `supervise.py` — everything lives here, single file by design.
- `supervise` — bash wrapper that `exec`s the file under `.venv/bin/python`.
- `.venv/` — only dep is `rich`.
