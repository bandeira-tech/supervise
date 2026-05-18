#!/usr/bin/env python3
"""supervise — continuous project status dashboard"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import select
import termios
import tty
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.console import Console, Group
from rich.live import Live
from rich.rule import Rule
from rich.style import Style
from rich.table import Table
from rich.text import Text


def run(cmd: list[str], cwd=None, timeout=60) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as e:
        return -1, "", str(e)


def detect_test_cmd(target: Path) -> Optional[list[str]]:
    if (target / "deno.json").exists() or (target / "deno.jsonc").exists():
        fname = "deno.json" if (target / "deno.json").exists() else "deno.jsonc"
        try:
            cfg = json.loads((target / fname).read_text())
            if "test" in cfg.get("tasks", {}):
                return ["deno", "task", "test"]
        except Exception:
            pass
        return ["deno", "test"]
    if (target / "mix.exs").exists():
        return ["mix", "test"]
    if (target / "Cargo.toml").exists():
        return ["cargo", "test"]
    if (target / "go.mod").exists():
        return ["go", "test", "./..."]
    if (target / "package.json").exists():
        try:
            pkg = json.loads((target / "package.json").read_text())
            if "test" in pkg.get("scripts", {}):
                if (target / "pnpm-lock.yaml").exists():
                    return ["pnpm", "test", "--", "--run"]
                elif (target / "yarn.lock").exists():
                    return ["yarn", "test", "--watchAll=false"]
                return ["npm", "test", "--", "--watchAll=false"]
        except Exception:
            pass
    for f in ("pytest.ini", "pyproject.toml", "setup.cfg"):
        if (target / f).exists():
            return ["python", "-m", "pytest", "--tb=short", "-q"]
    if (target / "Makefile").exists():
        rc, _, _ = run(["make", "-n", "test"], cwd=target, timeout=5)
        if rc == 0:
            return ["make", "test"]
    return None


@dataclass
class GitState:
    branch: str = "?"
    dirty: bool = False
    status_output: str = ""
    commit_msg: str = ""
    commit_time: Optional[datetime] = None
    remote_url: Optional[str] = None
    ahead: int = 0
    behind: int = 0
    worktrees: list[tuple[str, str, bool]] = field(default_factory=list)  # (branch, abs_path, merged)
    error: Optional[str] = None


@dataclass
class TestState:
    cmd: Optional[list[str]] = None
    running: bool = False
    run_started: Optional[datetime] = None
    passed: Optional[bool] = None
    output: str = ""
    last_run: Optional[datetime] = None
    duration: float = 0.0
    count: Optional[str] = None  # e.g. "42 passed" or "38 passed, 4 failed"


@dataclass
class PRState:
    prs: list = field(default_factory=list)
    last_fetch: Optional[datetime] = None
    fetching: bool = False
    fetch_started: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class Issue:
    id: str
    kind: str
    title: str
    detected_at: datetime
    state: str = "open"  # open | running | fixed | failed
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    _action: object = None  # callable returning (rc, out, err) or None


class IssueStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._issues: dict[str, Issue] = {}
        self._listeners: list = []

    def subscribe(self, fn):
        with self._lock:
            self._listeners.append(fn)

    def _emit(self, event: str, issue: Issue):
        for fn in list(self._listeners):
            try:
                fn(event, issue)
            except Exception:
                pass

    def reconcile(self, detected: list[Issue]):
        events: list[tuple[str, Issue]] = []
        with self._lock:
            ids_now = {i.id for i in detected}
            for id_ in list(self._issues):
                cur = self._issues[id_]
                if cur.state == "open" and id_ not in ids_now:
                    events.append(("vanished", cur))
                    del self._issues[id_]
            for new in detected:
                cur = self._issues.get(new.id)
                if cur is None:
                    self._issues[new.id] = new
                    events.append(("detected", new))
                elif cur.state == "fixed":
                    self._issues[new.id] = new
                    events.append(("reopened", new))
                else:
                    cur.title = new.title
                    cur._action = new._action
        for ev, iss in events:
            self._emit(ev, iss)

    def start(self, id_: str) -> bool:
        with self._lock:
            iss = self._issues.get(id_)
            if iss is None or iss.state == "running":
                return False
            iss.state = "running"
            iss.error = None
            iss.started_at = datetime.now(tz=timezone.utc)
            snap = iss
        self._emit("started", snap)
        return True

    def finish(self, id_: str, error: Optional[str] = None):
        with self._lock:
            iss = self._issues.get(id_)
            if iss is None:
                return
            iss.finished_at = datetime.now(tz=timezone.utc)
            iss.state = "failed" if error else "fixed"
            iss.error = error
            snap = iss
        self._emit("failed" if error else "fixed", snap)

    def action_for(self, id_: str):
        with self._lock:
            iss = self._issues.get(id_)
            return iss._action if iss else None

    def snapshot(self) -> tuple[list[Issue], list[Issue], list[Issue]]:
        with self._lock:
            issues = list(self._issues.values())
        opens = [i for i in issues if i.state in ("open", "running")]
        failed = [i for i in issues if i.state == "failed"]
        fixed = sorted(
            [i for i in issues if i.state == "fixed"],
            key=lambda i: i.finished_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return opens, failed, fixed


@dataclass
class PackageState:
    name: Optional[str] = None
    registry: str = "npm"               # "npm" or "jsr"
    local_version: Optional[str] = None # from config file
    published_version: Optional[str] = None
    fetching: bool = False
    fetch_started: Optional[datetime] = None
    last_fetch: Optional[datetime] = None
    error: Optional[str] = None


_WATCH_SKIP_DIRS = frozenset({
    ".git", "__pycache__", "node_modules", ".venv", "venv",
    "target", "_build", "dist", "build",
    ".mypy_cache", ".ruff_cache", ".pytest_cache",
    ".deno", ".gradle", ".npm", ".cache",
    "coverage", ".nyc_output", ".turbo", ".next", ".nuxt",
})

_WATCH_SKIP_FILES = frozenset({
    "deno.lock", "package-lock.json", "pnpm-lock.yaml",
    "yarn.lock", "Cargo.lock", "mix.lock", "uv.lock",
    "poetry.lock", "Gemfile.lock", "go.sum", "composer.lock",
})

_CODE_EXTS = frozenset({
    ".py", ".pyi", ".ts", ".tsx", ".mts", ".cts",
    ".js", ".jsx", ".mjs", ".cjs",
    ".ex", ".exs", ".go", ".rs", ".java", ".kt", ".scala",
    ".c", ".cpp", ".cc", ".h", ".hpp",
    ".swift", ".rb", ".php", ".cs",
    ".lua", ".zig", ".elm", ".clj", ".cljs",
    ".ml", ".mli", ".hs", ".jl",
    ".sh", ".bash", ".zsh", ".fish",
})


class Supervisor:
    def __init__(self, target: Path, pr_interval: int):
        self.target = target.resolve()
        self.pr_interval = pr_interval
        self._lock = threading.Lock()
        self.git = GitState()
        self.tests = TestState(cmd=detect_test_cmd(target))
        self.prs = PRState()
        self.pkg = PackageState()
        self.issues = IssueStore()
        self._action_row_map: dict[int, str] = {}
        self._action_key_map: dict[str, str] = {}
        self._stop = threading.Event()
        self._pr_wake = threading.Event()
        self._pkg_wake = threading.Event()
        self._test_wake = threading.Event()
        self._remote_url: Optional[str] = None  # None = not yet fetched, "" = no github remote
        self._default_ref: Optional[str] = None  # cached "origin/main" or similar
        self._pkg_info = _detect_package(target)  # (registry, name, local_version) or None
        if self._pkg_info:
            registry, name, local_version = self._pkg_info
            self.pkg = PackageState(name=name, registry=registry, local_version=local_version)

    def start(self):
        for fn in (self._git_loop, self._test_loop, self._pr_loop, self._pkg_loop):
            t = threading.Thread(target=fn, daemon=True)
            t.start()

    def stop(self):
        self._stop.set()
        self._pr_wake.set()
        self._pkg_wake.set()
        self._test_wake.set()

    def force_refresh(self):
        self._pr_wake.set()
        self._pkg_wake.set()
        with self._lock:
            can_run = self.tests.cmd is not None and not self.tests.running
        if can_run:
            self._test_wake.set()

    def _git_loop(self):
        while not self._stop.is_set():
            g = GitState()
            rc, out, _ = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=self.target)
            if rc != 0:
                g.error = "not a git repo"
            else:
                g.branch = out.strip()
                _, status, _ = run(["git", "status", "--porcelain"], cwd=self.target)
                g.dirty = bool(status.strip())
                g.status_output = status
                _, log, _ = run(
                    ["git", "log", "-1", "--format=%s\x1f%ct"],
                    cwd=self.target,
                )
                if log.strip():
                    parts = log.strip().split("\x1f")
                    g.commit_msg = parts[0].strip()
                    if len(parts) > 1:
                        try:
                            g.commit_time = datetime.fromtimestamp(int(parts[1]), tz=timezone.utc)
                        except ValueError:
                            pass
                _, ab_out, _ = run(
                    ["git", "rev-list", "--count", "--left-right", "HEAD...@{u}"],
                    cwd=self.target,
                )
                if ab_out.strip():
                    ab_parts = ab_out.strip().split()
                    if len(ab_parts) == 2:
                        try:
                            g.ahead, g.behind = int(ab_parts[0]), int(ab_parts[1])
                        except ValueError:
                            pass
                if self._remote_url is None:
                    _, remote, _ = run(["git", "remote", "get-url", "origin"], cwd=self.target)
                    self._remote_url = _parse_github_url(remote.strip()) or ""
                g.remote_url = self._remote_url or None
                _, wt_out, _ = run(["git", "worktree", "list", "--porcelain"], cwd=self.target)
                if self._default_ref is None:
                    _, sym, _ = run(
                        ["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
                        cwd=self.target,
                    )
                    self._default_ref = sym.strip() or "origin/main"
                g.worktrees = _parse_worktrees(wt_out, self.target, self._default_ref)
            with self._lock:
                self.git = g
            if not g.error:
                self.issues.reconcile(_detect_issues(self.target, g))
            self._stop.wait(2)

    def _scan_mtimes(self) -> dict:
        mtimes: dict = {}
        try:
            for root, dirs, files in os.walk(self.target):
                dirs[:] = [d for d in dirs if d not in _WATCH_SKIP_DIRS]
                for f in files:
                    if f in _WATCH_SKIP_FILES:
                        continue
                    p = Path(root) / f
                    try:
                        mtimes[str(p)] = p.stat().st_mtime
                    except OSError:
                        pass
        except Exception:
            pass
        return mtimes

    def _test_loop(self):
        if not self.tests.cmd:
            return
        prev = self._scan_mtimes()
        self._run_tests()
        while not self._stop.is_set():
            forced = self._test_wake.wait(timeout=2)
            self._test_wake.clear()
            curr = self._scan_mtimes()
            if curr != prev or forced:
                prev = self._scan_mtimes()  # rescan after any settle
                self._run_tests()
                self._test_wake.wait(timeout=10)  # cooldown: ignore churn from the run itself
                self._test_wake.clear()

    def _run_tests(self):
        with self._lock:
            if self.tests.running:
                return
            self.tests.running = True
            self.tests.run_started = datetime.now(tz=timezone.utc)
            cmd = self.tests.cmd

        start = time.monotonic()
        rc, stdout, stderr = run(cmd, cwd=self.target, timeout=300)
        elapsed = time.monotonic() - start

        combined = (stdout + stderr).strip()
        lines = combined.splitlines()
        if len(lines) > 80:
            combined = "\n".join(lines[-80:])

        # parse test count from output (x passed, y failed patterns)
        test_count = _parse_test_counts(combined)

        with self._lock:
            self.tests.running = False
            self.tests.run_started = None
            self.tests.passed = rc == 0
            self.tests.output = combined
            self.tests.last_run = datetime.now(tz=timezone.utc)
            self.tests.duration = elapsed
            self.tests.count = test_count

    def _pr_loop(self):
        while not self._stop.is_set():
            self._fetch_prs()
            self._pr_wake.wait(timeout=self.pr_interval)
            self._pr_wake.clear()

    def _fetch_prs(self):
        with self._lock:
            self.prs.fetching = True
            self.prs.fetch_started = datetime.now(tz=timezone.utc)

        rc, out, err = run(
            ["gh", "pr", "list", "--json", "number,title,author,createdAt,headRefName", "--limit", "10"],
            cwd=self.target,
            timeout=20,
        )
        p = PRState(last_fetch=datetime.now(tz=timezone.utc))
        if rc == 0:
            try:
                p.prs = json.loads(out)
            except Exception as e:
                p.error = f"parse: {e}"
        else:
            p.error = (err or "gh failed").strip().splitlines()[0]

        with self._lock:
            self.prs = p

    def _pkg_loop(self):
        if not self._pkg_info:
            return
        self._fetch_pkg()
        while not self._stop.is_set():
            self._pkg_wake.wait(timeout=600)
            self._pkg_wake.clear()
            if not self._stop.is_set():
                self._fetch_pkg()

    def _fetch_pkg(self):
        with self._lock:
            if not self.pkg.name:
                return
            self.pkg.fetching = True
            self.pkg.fetch_started = datetime.now(tz=timezone.utc)
            registry = self.pkg.registry
            name = self.pkg.name

        published = _fetch_published_version(registry, name)

        with self._lock:
            self.pkg.fetching = False
            self.pkg.fetch_started = None
            self.pkg.published_version = published
            self.pkg.last_fetch = datetime.now(tz=timezone.utc)

    def snapshot(self):
        with self._lock:
            return self.git, self.tests, self.prs, self.pkg

    def execute_issue(self, id_: str):
        action = self.issues.action_for(id_)
        if action is None:
            return
        if not self.issues.start(id_):
            return

        def _run():
            try:
                result = action()
                if isinstance(result, tuple) and len(result) == 3:
                    rc, out, err = result
                    if rc == 0:
                        self.issues.finish(id_)
                    else:
                        msg = (err or out or f"exit {rc}").strip().splitlines()
                        self.issues.finish(id_, error=msg[0] if msg else f"exit {rc}")
                else:
                    self.issues.finish(id_)
            except Exception as e:
                self.issues.finish(id_, error=str(e))
            self._pr_wake.set()

        threading.Thread(target=_run, daemon=True).start()


def _parse_worktrees(porcelain: str, target: Path, default_ref: str) -> list[tuple[str, str, bool]]:
    """Parse `git worktree list --porcelain`; exclude the target itself."""
    result: list[tuple[str, str, bool]] = []
    path: Optional[str] = None
    branch: Optional[str] = None
    target_str = str(target)
    for line in porcelain.splitlines() + [""]:
        if not line.strip():
            if path and path != target_str:
                b = branch or "(detached)"
                merged = False
                if branch:
                    rc, _, _ = run(
                        ["git", "merge-base", "--is-ancestor", branch, default_ref],
                        cwd=target,
                    )
                    merged = rc == 0
                result.append((b, path, merged))
            path, branch = None, None
            continue
        if line.startswith("worktree "):
            path = line[len("worktree "):].strip()
        elif line.startswith("branch "):
            ref = line[len("branch "):].strip()
            branch = ref.removeprefix("refs/heads/")
    return result


def _make_wt_remove(repo: Path, wt_path: str, branch: str):
    def action():
        rc, out, err = run(["git", "worktree", "remove", wt_path], cwd=repo, timeout=30)
        if rc != 0:
            return rc, out, err
        return run(["git", "branch", "-D", branch], cwd=repo, timeout=10)
    return action


def _detect_issues(target: Path, g: GitState) -> list[Issue]:
    now = datetime.now(tz=timezone.utc)
    issues: list[Issue] = []
    if g.ahead > 0:
        issues.append(Issue(
            id="remote-push",
            kind="remote-push",
            title=f"local ↑{g.ahead} — push",
            detected_at=now,
            _action=lambda: run(["git", "push"], cwd=target, timeout=60),
        ))
    if g.behind > 0:
        issues.append(Issue(
            id="remote-pull",
            kind="remote-pull",
            title=f"remote ↓{g.behind} — pull -r",
            detected_at=now,
            _action=lambda: run(["git", "pull", "--rebase"], cwd=target, timeout=120),
        ))
    for branch, path, merged in g.worktrees:
        if merged and branch != "(detached)":
            issues.append(Issue(
                id=f"wt-merged:{branch}",
                kind="wt-merged",
                title=f"{branch} merged — remove worktree + branch",
                detected_at=now,
                _action=_make_wt_remove(target, path, branch),
            ))
    return issues


def _display_path(p: str) -> str:
    home = str(Path.home())
    if p.startswith(home + "/"):
        return "~/" + p[len(home) + 1:]
    if p == home:
        return "~"
    return p


def _parse_github_url(remote: str) -> Optional[str]:
    m = re.match(r"git@github\.com:(.+?)(?:\.git)?$", remote)
    if m:
        return f"https://github.com/{m.group(1)}"
    m = re.match(r"https?://github\.com/(.+?)(?:\.git)?$", remote)
    if m:
        return f"https://github.com/{m.group(1)}"
    return None


def _detect_package(target: Path) -> Optional[tuple[str, str, Optional[str]]]:
    """Return (registry, name, local_version) from project config, or None."""
    for fname in ("deno.json", "deno.jsonc"):
        fp = target / fname
        if fp.exists():
            try:
                cfg = json.loads(fp.read_text())
                name = cfg.get("name", "")
                version = cfg.get("version") or None
                if name.startswith("@"):
                    return ("jsr", name, version)
            except Exception:
                pass
    fp = target / "package.json"
    if fp.exists():
        try:
            pkg = json.loads(fp.read_text())
            name = pkg.get("name", "")
            version = pkg.get("version") or None
            if name:
                return ("npm", name, version)
        except Exception:
            pass
    return None


def _fetch_published_version(registry: str, name: str) -> Optional[str]:
    try:
        if registry == "npm":
            url = f"https://registry.npmjs.org/{name}/latest"
        else:
            url = f"https://jsr.io/{name}/meta.json"
        req = urllib.request.Request(url, headers={"User-Agent": "supervise/0.1"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("version") if registry == "npm" else data.get("latest")
    except Exception:
        return None


def _version_gt(a: str, b: str) -> bool:
    def parts(v: str) -> list[int]:
        v = re.sub(r"^v", "", v)
        v = re.split(r"[-+]", v)[0]
        result = []
        for p in v.split("."):
            try:
                result.append(int(p))
            except ValueError:
                result.append(0)
        return result
    return parts(a) > parts(b)


def _parse_test_counts(output: str) -> Optional[str]:
    # Deno: "ok | 12 passed | 0 failed (123ms)"
    m = re.search(r"(\d+) passed.*?(\d+) failed", output)
    if m:
        p, f = int(m.group(1)), int(m.group(2))
        return f"{p}/{p+f}" if f == 0 else f"{p}/{p+f} ({f} failed)"
    m = re.search(r"(\d+) passed", output)
    if m:
        return f"{m.group(1)} passed"
    # pytest: "5 passed", "3 failed"
    m = re.search(r"(\d+) failed", output)
    if m:
        p_m = re.search(r"(\d+) passed", output)
        p = int(p_m.group(1)) if p_m else 0
        f = int(m.group(1))
        return f"{p}/{p+f} ({f} failed)"
    # mix test: "1 test, 0 failures" or "5 tests, 2 failures"
    m = re.search(r"(\d+) tests?, (\d+) failures?", output)
    if m:
        total, failures = int(m.group(1)), int(m.group(2))
        passed = total - failures
        return f"{passed}/{total}" if failures == 0 else f"{passed}/{total} ({failures} failed)"
    return None


def _clip_output(output: str, console: Optional["Console"] = None) -> str:
    try:
        height = console.size.height if console else os.get_terminal_size().lines
    except Exception:
        height = 40
    max_lines = max(5, height - 14)
    lines = output.splitlines()
    if len(lines) <= max_lines:
        return output
    clipped = len(lines) - max_lines
    return f"[↑ {clipped} more lines]\n" + "\n".join(lines[-max_lines:])


def _elapsed(since: Optional[datetime]) -> str:
    if since is None:
        return ""
    secs = max(0, int((datetime.now(tz=timezone.utc) - since).total_seconds()))
    return f"{secs}s" if secs < 60 else f"{secs // 60}m{secs % 60:02d}s"


def time_ago(dt: Optional[datetime]) -> str:
    if dt is None:
        return "never"
    now = datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    secs = max(0, int((now - dt).total_seconds()))
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def _sym(state: str) -> Text:
    return {
        "good": Text("✓", style="bold green"),
        "bad":  Text("✗", style="bold red"),
        "run":  Text("~", style="yellow"),
    }.get(state, Text(" "))


def _script_version(script: Path) -> str:
    try:
        h = hashlib.sha1(script.read_bytes()).hexdigest()[:7]
        ts = datetime.fromtimestamp(script.stat().st_mtime).strftime("%Y%m%dT%H%M")
        return f">>@v0.1.{h}-{ts}"
    except Exception:
        return ""


def _ops_body(
    git: GitState, tests: TestState, prs: PRState, console: Optional[Console]
) -> tuple[list, str]:
    tbl = Table(
        show_header=False, box=None,
        pad_edge=False, show_edge=False,
        padding=(0, 2, 0, 0),
        expand=True,
    )
    tbl.add_column("sym",   width=1,  no_wrap=True, min_width=1, max_width=1)
    tbl.add_column("value", ratio=1,  no_wrap=True, overflow="ellipsis")

    # ─ git ───────────────────────────────────────────────────────────────────
    if git.error:
        tbl.add_row(_sym("bad"), Text(git.error, style="red"))
    else:
        v = Text()
        if git.commit_time:
            v.append(time_ago(git.commit_time), style="dim")
            v.append("  ")
        v.append(git.branch, style="yellow")
        if git.ahead:
            v.append(f" ↑{git.ahead}", style="green")
        if git.behind:
            v.append(f" ↓{git.behind}", style="red")
        if git.commit_msg:
            v.append("  ")
            v.append(git.commit_msg, style="dim italic")
        tbl.add_row(_sym("bad" if git.dirty else "good"), v)

    # ─ tests ─────────────────────────────────────────────────────────────────
    if tests.cmd is not None:
        v = Text()
        if tests.running:
            v.append(_elapsed(tests.run_started), style="dim")
            if tests.count:
                v.append(f"  {tests.count}", style="dim")
            if tests.last_run:
                v.append(f"  {tests.duration:.1f}s", style="dim")
            tbl.add_row(_sym("run"), v)
        elif tests.passed is None:
            v.append("—", style="dim")
            tbl.add_row(_sym(""), v)
        elif tests.passed:
            if tests.last_run:
                v.append(time_ago(tests.last_run), style="dim")
                v.append("  ")
            if tests.count:
                v.append(tests.count, style="")
            if tests.last_run:
                v.append(f"  {tests.duration:.1f}s", style="dim")
            tbl.add_row(_sym("good"), v)
        else:
            if tests.last_run:
                v.append(time_ago(tests.last_run), style="dim")
                v.append("  ")
            if tests.count:
                v.append(tests.count, style="red")
            tbl.add_row(_sym("bad"), v)

    # ─ gh/pr (own table so test output can be inserted above it) ─────────────
    pr_tbl = Table(
        show_header=False, box=None,
        pad_edge=False, show_edge=False,
        padding=(0, 2, 0, 0),
        expand=True,
    )
    pr_tbl.add_column("sym",   width=1,  no_wrap=True, min_width=1, max_width=1)
    pr_tbl.add_column("value", ratio=1,  no_wrap=True, overflow="ellipsis")
    v = Text()
    wt_by_branch = {b: (p, m) for b, p, m in git.worktrees}
    pr_branches = {pr.get("headRefName") for pr in prs.prs}
    standalone_wts = [(b, p, m) for b, p, m in git.worktrees if b not in pr_branches]
    has_any = bool(prs.prs) or bool(standalone_wts)
    if prs.fetching and prs.last_fetch is None:
        v.append(_elapsed(prs.fetch_started), style="dim")
        pr_sym = _sym("run")
    elif not has_any:
        if prs.fetching:
            v.append(f"{_elapsed(prs.fetch_started)}  ", style="dim")
        elif prs.last_fetch:
            v.append(f"{time_ago(prs.last_fetch)}  ", style="dim")
        v.append("none", style="dim")
        pr_sym = _sym("run") if prs.fetching else _sym("good")
    else:
        if prs.fetching:
            v.append(f"{_elapsed(prs.fetch_started)}", style="dim")
        row_idx = 0
        for pr in prs.prs[:5]:
            author = (pr.get("author") or {}).get("login", "")
            pr_url = f"{git.remote_url}/pull/{pr['number']}" if git.remote_url else None
            branch = pr.get("headRefName")
            is_current = branch == git.branch
            wt_entry = wt_by_branch.get(branch)
            if row_idx > 0 or prs.fetching:
                v.append("\n")
            v.append("▸  " if is_current else "   ")
            link_style = Style(link=pr_url) if pr_url else Style()
            if wt_entry:
                wt_path, wt_merged = wt_entry
                v.append(f"#{pr['number']} {branch}", style=link_style)
                if wt_merged:
                    v.append(" ✗", style="bold red")
                v.append(f"  {_display_path(wt_path)}", style="dim cyan")
            else:
                v.append(f"#{pr['number']} {pr['title'][:60]}", style=link_style)
                if author:
                    v.append(f"  {author}", style="dim")
            row_idx += 1
        for branch, path, merged in standalone_wts:
            is_current = branch == git.branch
            if row_idx > 0 or prs.fetching:
                v.append("\n")
            v.append("▸  " if is_current else "   ")
            v.append(branch, style="yellow")
            if merged:
                v.append(" ✗", style="bold red")
            v.append(f"  {_display_path(path)}", style="dim cyan")
            row_idx += 1
        pr_sym = _sym("run") if prs.fetching else _sym("")
    pr_tbl.add_row(pr_sym, v)

    test_failed = tests.cmd and not tests.running and tests.passed is False
    parts: list = [tbl]
    if test_failed and tests.output:
        parts.append(Rule(style="dim red"))
        parts.append(Text(_clip_output(tests.output, console), style="dim"))
        parts.append(Rule(style="dim red"))
    parts.append(pr_tbl)
    if not test_failed and git.dirty and git.status_output:
        parts.append(Rule(style="dim yellow"))
        parts.append(Text(git.status_output.rstrip(), style="dim"))

    color = "red" if tests.passed is False else ("yellow" if git.dirty else "green")
    return parts, color


def _scan_dir(path: Path) -> tuple[list[str], list[str], list[str]]:
    """Return (code_dirs, code_files, rest_files) for one directory level."""
    try:
        entries = sorted(os.listdir(path))
    except Exception:
        return [], [], []
    dirs, code_files, rest_files = [], [], []
    for name in entries:
        p = path / name
        if p.is_dir():
            if not name.startswith(".") and name not in _WATCH_SKIP_DIRS:
                dirs.append(name)
        elif p.is_file():
            if Path(name).suffix.lower() in _CODE_EXTS:
                code_files.append(name)
            else:
                rest_files.append(name)
    return dirs, code_files, rest_files


def _content_body(target: Path) -> tuple[list, str]:
    src = target / "src"

    if src.is_dir():
        # top: contents of src/
        code_dirs, code_files, _ = _scan_dir(src)
        # bottom: full root listing, src included
        root_dirs, root_code, root_rest = _scan_dir(target)
        bottom_dirs  = root_dirs   # src stays in here
        bottom_files = root_code + root_rest
        open_dir     = "src"
    else:
        code_dirs, code_files, rest = _scan_dir(target)
        bottom_dirs  = []
        bottom_files = rest
        open_dir     = None

    parts: list = []

    if code_dirs or code_files:
        code_tbl = Table(
            show_header=False, box=None,
            pad_edge=False, show_edge=False,
            padding=(0, 0, 0, 1),
        )
        code_tbl.add_column("name", no_wrap=True, overflow="ellipsis")
        for d in code_dirs:
            code_tbl.add_row(Text(f"{d}/", style="cyan"))
        for f in code_files:
            code_tbl.add_row(Text(f))
        parts.append(code_tbl)

    if bottom_dirs or bottom_files:
        if parts:
            parts.append(Rule(style="dim"))
        rest_tbl = Table(
            show_header=False, box=None,
            pad_edge=False, show_edge=False,
            padding=(0, 0, 0, 1),
        )
        rest_tbl.add_column("name", no_wrap=True, overflow="ellipsis")
        for d in bottom_dirs:
            if d == open_dir:
                rest_tbl.add_row(Text(f"▸ {d}/", style="cyan"))
            else:
                rest_tbl.add_row(Text(f"  {d}/", style="dim cyan"))
        for f in bottom_files:
            rest_tbl.add_row(Text(f, style="dim"))
        parts.append(rest_tbl)

    return parts, "blue"


def _pkg_line(pkg: PackageState) -> Text:
    if not pkg.name:
        return Text("")
    label = f"{pkg.registry}:{pkg.name}"
    t = Text()
    if pkg.fetching and pkg.last_fetch is None:
        t.append("~", style="yellow")
        t.append(f"  {_elapsed(pkg.fetch_started)}", style="dim")
        t.append(f"  {label}")
    elif pkg.published_version is None:
        t.append(" ")
        t.append(f"  {label}", style="dim")
    else:
        local = pkg.local_version
        published = pkg.published_version
        if local:
            ahead = _version_gt(local, published)
            sym = "✓" if local == published else ("↑" if ahead else "✗")
            sym_style = "bold red" if (local != published and not ahead) else "bold green"
        else:
            ahead = False
            sym, sym_style = "✓", "bold green"
        if pkg.fetching:
            t.append("~", style="yellow")
            t.append(f"  {_elapsed(pkg.fetch_started)}", style="dim")
        else:
            t.append(sym, style=sym_style)
            if pkg.last_fetch:
                t.append(f"  {time_ago(pkg.last_fetch)}", style="dim")
        t.append(f"  {label}")
        if local and ahead:
            t.append(f"  {local}", style="green")
        t.append(f"  {published}", style="dim")
    return t


def _issue_table() -> Table:
    tbl = Table(
        show_header=False, box=None,
        pad_edge=False, show_edge=False,
        padding=(0, 1, 0, 0),
        expand=True,
    )
    tbl.add_column("key", width=1, no_wrap=True, min_width=1, max_width=1)
    tbl.add_column("sym", width=1, no_wrap=True, min_width=1, max_width=1)
    tbl.add_column("value", ratio=1, no_wrap=True, overflow="ellipsis")
    return tbl


def _action_body(
    opens: list[Issue], failed: list[Issue], fixed: list[Issue],
) -> tuple[list, str, dict[int, str], dict[str, str]]:
    parts: list = []
    row_map: dict[int, str] = {}
    key_map: dict[str, str] = {}
    body_row = 0
    letters = "abcdefghijklmnopqrstuvwxyz"

    def _next_key() -> str:
        idx = len(key_map)
        return letters[idx] if idx < len(letters) else ""

    if failed:
        tbl = _issue_table()
        for iss in failed:
            v = Text()
            v.append(iss.title)
            if iss.error:
                v.append(f"  {iss.error}", style="dim red")
            key = _next_key()
            if key:
                key_map[key] = iss.id
            tbl.add_row(Text(key, style="bold cyan"), _sym("bad"), v)
            row_map[body_row] = iss.id
            body_row += 1
        parts.append(tbl)
        parts.append(Rule(style="dim red"))
        body_row += 1

    if opens:
        tbl = _issue_table()
        for iss in opens:
            v = Text()
            if iss.state == "running":
                v.append(_elapsed(iss.started_at), style="dim")
                v.append("  ")
                v.append(iss.title)
                sym = _sym("run")
                key_text = Text(" ")
            else:
                v.append(iss.title)
                sym = Text("▶", style="bold cyan")
                key = _next_key()
                if key:
                    key_map[key] = iss.id
                key_text = Text(key, style="bold cyan")
            tbl.add_row(key_text, sym, v)
            row_map[body_row] = iss.id
            body_row += 1
        parts.append(tbl)
    elif not failed:
        parts.append(Text("  no issues", style="dim green"))
        body_row += 1

    if fixed:
        parts.append(Rule(style="dim green"))
        body_row += 1
        tbl = _issue_table()
        for iss in fixed[:10]:
            v = Text()
            if iss.finished_at:
                v.append(time_ago(iss.finished_at), style="dim")
                v.append("  ")
            v.append(iss.title, style="dim")
            tbl.add_row(Text(" "), _sym("good"), v)
            body_row += 1
        parts.append(tbl)

    has_open = any(i.state == "open" for i in opens)
    color = "red" if failed else ("yellow" if has_open else "green")
    return parts, color, row_map, key_map


def render(sup: Supervisor, console: Optional[Console] = None, version: str = "", view: str = "ops", show_help: bool = False) -> Group:
    git, tests, prs, pkg = sup.snapshot()
    target = sup.target

    home = Path.home()
    try:
        display = "~/" + str(target.relative_to(home))
    except ValueError:
        display = str(target)

    header = Text(display, style="cyan")
    if git.remote_url:
        repo_label = git.remote_url.removeprefix("https://")
        header.append("  ")
        header.append(repo_label, style=Style(color="bright_black", link=git.remote_url))

    spacer = _pkg_line(pkg)

    if view == "content":
        body_parts, color = _content_body(target)
        top_parts: list = []
    elif view == "action":
        opens, failed, fixed = sup.issues.snapshot()
        body_parts, color, body_row_map, body_key_map = _action_body(opens, failed, fixed)
        top_parts = []  # no fs/gh/pkg header in action view
        # Screen row layout: 1=bar → body starts at row 2
        sup._action_row_map = {br + 2: iid for br, iid in body_row_map.items()}
        sup._action_key_map = body_key_map
    else:
        body_parts, color = _ops_body(git, tests, prs, console)
        top_parts = [header, spacer]

    if show_help:
        body_parts.append(Rule(style="dim"))
        h = Text()
        h.append("1", style="bold")
        h.append("  ops    ", style="dim")
        h.append("2", style="bold")
        h.append("  content    ", style="dim")
        h.append("3", style="bold")
        h.append("  action    ", style="dim")
        h.append("!", style="bold")
        h.append("  refresh ops    ", style="dim")
        h.append("@", style="bold")
        h.append("  refresh content    ", style="dim")
        h.append("#", style="bold")
        h.append("  refresh action    ", style="dim")
        h.append("a-z", style="bold")
        h.append("  run issue", style="dim")
        body_parts.append(h)

    bar = Table(
        show_header=False, box=None,
        expand=True, padding=(0, 1, 0, 1),
        show_edge=False, pad_edge=False,
    )
    view_num = {"ops": "1", "content": "2", "action": "3"}.get(view, "")
    bar.add_column("v",    style=f"bold on {color}", no_wrap=True)
    bar.add_column("name", ratio=1, style=f"bold on {color}", no_wrap=True, overflow="ellipsis")
    bar.add_column("ver",           style=f"dim on {color}",  no_wrap=True)
    bar.add_row(view_num, target.name, version or "")

    return Group(bar, *top_parts, *body_parts)


def main():
    ap = argparse.ArgumentParser(
        prog="supervise",
        description="Continuous project status dashboard",
    )
    ap.add_argument("target", nargs="?", default=".", help="project directory (default: cwd)")
    ap.add_argument("-v", "--view", choices=["ops", "content", "action"], default="ops", metavar="VIEW", help="view mode: ops (default), content, or action")
    ap.add_argument("--pr-interval", type=int, default=300, metavar="N", help="seconds between PR fetches (default: 300)")
    ap.add_argument("--refresh", type=float, default=1.0, metavar="S", help="display refresh rate in seconds (default: 1)")
    args = ap.parse_args()

    target = Path(args.target).expanduser().resolve()
    if not target.is_dir():
        print(f"error: {target} is not a directory", file=sys.stderr)
        sys.exit(1)

    script = Path(__file__).with_suffix(".py").resolve()
    try:
        script_mtime = script.stat().st_mtime
    except OSError:
        script_mtime = None
    version = _script_version(script)

    sup = Supervisor(target, pr_interval=args.pr_interval)
    sup.start()

    console = Console()
    view = args.view
    show_help = False
    reload_needed = False

    raw_mode = sys.stdin.isatty()
    fd = sys.stdin.fileno() if raw_mode else -1
    old_term = termios.tcgetattr(fd) if raw_mode else None
    if raw_mode:
        tty.setcbreak(fd)
        sys.stdout.write("\x1b[?1000h\x1b[?1006h")  # enable SGR mouse
        sys.stdout.flush()

    def _read_input() -> object:
        """Return a key str, ('mouse', btn, col, row, pressed) tuple, or None."""
        if not raw_mode or not select.select([sys.stdin], [], [], 0)[0]:
            return None
        ch = sys.stdin.read(1)
        if ch != "\x1b":
            return ch
        buf = ""
        while select.select([sys.stdin], [], [], 0.02)[0]:
            c = sys.stdin.read(1)
            buf += c
            if buf and buf[-1] in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz~":
                break
        m = re.match(r"\[<(\d+);(\d+);(\d+)([Mm])$", buf)
        if m:
            return ("mouse", int(m[1]), int(m[2]), int(m[3]), m[4] == "M")
        return None  # unrecognised escape sequence

    def _click_map() -> dict[int, str]:
        """Map screen row (1-indexed) → action token."""
        if view == "action":
            return {row: f"issue:{iid}" for row, iid in sup._action_row_map.items()}
        if view != "ops":
            return {}
        # row 1: title bar, row 2: header, row 3: spacer, row 4: git
        has_tests = sup.tests.cmd is not None
        pr_row = 6 if has_tests else 5
        result = {}
        if has_tests:
            result[5] = "tests"
        result[pr_row] = "prs"
        return result

    try:
        with Live(render(sup, console, version, view=view, show_help=show_help), console=console, refresh_per_second=1, screen=True) as live:
            while True:
                if script_mtime is not None:
                    try:
                        if script.stat().st_mtime != script_mtime:
                            reload_needed = True
                            break
                    except OSError:
                        pass
                event = _read_input()
                if event is None:
                    time.sleep(args.refresh)
                elif isinstance(event, tuple) and event[0] == "mouse":
                    _, btn, col, row, pressed = event
                    if btn == 0 and pressed:
                        action = _click_map().get(row)
                        if action and action.startswith("issue:"):
                            sup.execute_issue(action[len("issue:"):])
                        elif col <= 4 and action == "tests":
                            sup._test_wake.set()
                        elif col <= 4 and action == "prs":
                            sup._pr_wake.set()
                elif isinstance(event, str):
                    ch = event
                    if ch in ("\x03", "\x04"):
                        break
                    elif ch == "1":
                        view, show_help = "ops", False
                    elif ch == "2":
                        view, show_help = "content", False
                    elif ch == "3":
                        view, show_help = "action", False
                    elif ch == "!":   # Shift+1 — switch to ops + full refresh
                        view, show_help = "ops", False
                        sup.force_refresh()
                    elif ch == "@":   # Shift+2 — switch to content + full refresh
                        view, show_help = "content", False
                        sup.force_refresh()
                    elif ch == "#":   # Shift+3 — switch to action + full refresh
                        view, show_help = "action", False
                        sup.force_refresh()
                    elif ch == "?":
                        show_help = not show_help
                    elif view == "action" and ch in sup._action_key_map:
                        sup.execute_issue(sup._action_key_map[ch])
                live.update(render(sup, console, version, view=view, show_help=show_help))
    except KeyboardInterrupt:
        pass
    finally:
        if raw_mode:
            sys.stdout.write("\x1b[?1000l\x1b[?1006l")  # disable mouse
            sys.stdout.flush()
            if old_term is not None:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_term)
        sup.stop()

    if reload_needed:
        console.clear()
        os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    main()
